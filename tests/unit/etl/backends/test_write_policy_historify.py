"""Unit tests for _WritePolicy dispatch — write-mode routing.

Verifies that:
* Every table write mode reaches its own backend hook and no other.
* HistorifySpec is routed to _do_historify (not any other handler).
* _do_historify validates missing-table policy before delegating.
* _do_historify materializes the frame, runs the SCD2 transform with the
  engine's _historify_backend and writes its result through the create/replace
  hooks, releasing the existing frame before the write.
* Temp specs still raise TypeError.
* Specs that are not a table write mode raise TypeError.
"""

from __future__ import annotations

import weakref
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.etl import col
from loom.etl.backends import _write_policy
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.declarative.expr._params import params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyInputMode,
    HistorifyRepairReport,
    HistorifySpec,
)
from loom.etl.declarative.target._schema_mode import SchemaMode
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._temp import TempSpec
from loom.etl.schema._schema import SchemaError, SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.routing import ResolvedTarget

# ---------------------------------------------------------------------------
# Minimal concrete _WritePolicy for testing dispatch
# ---------------------------------------------------------------------------


class _FakeHistorifyBackend:
    """Engine stand-in: the patched transform records its calls here."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.report: HistorifyRepairReport | None = None


def _fake_scd2_transform(
    ops: _FakeHistorifyBackend,
    frame: list[int],
    existing: Any,
    spec: HistorifySpec,
    params_instance: Any,
) -> tuple[list[int], HistorifyRepairReport | None]:
    ops.calls.append((frame, spec, params_instance))
    return [*frame, -1], ops.report


@pytest.fixture(autouse=True)
def _fake_transform(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_write_policy, "scd2_transform", _fake_scd2_transform)


class _StubWritePolicy(_WritePolicy[list[int], list[int], dict[str, Any]]):
    """Minimal concrete subclass for testing dispatch logic only."""

    def __init__(
        self,
        *,
        schema_exists: bool = True,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
    ) -> None:
        resolver = MagicMock()
        resolver.resolve.return_value = MagicMock(spec=ResolvedTarget)
        super().__init__(resolver=resolver, missing_table_policy=missing_table_policy)
        self._schema_exists = schema_exists
        self.historify = _FakeHistorifyBackend()
        self.historify_calls = self.historify.calls
        self.create_calls: list[tuple[Any, ...]] = []
        self.replace_calls: list[tuple[Any, ...]] = []
        self.replace_partitions_calls: list[tuple[Any, ...]] = []

    def _physical_schema(self, target: ResolvedTarget) -> dict[str, Any] | None:
        return {"col": "type"} if self._schema_exists else None

    def _align(
        self, frame: list[int], existing: dict[str, Any] | None, mode: SchemaMode
    ) -> list[int]:
        return frame

    def _materialize_for_write(self, frame: list[int], streaming: bool) -> list[int]:
        return list(frame)  # return a copy so we can track it

    def _pin_rows(self, frame: list[int]) -> tuple[list[int], bool]:
        return frame, not frame

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        return ""

    def _create(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        self.create_calls.append((frame, schema_mode, partition_cols))

    def _append(self, frame: list[int], target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _replace(
        self, frame: list[int], target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        self.replace_calls.append((frame, schema_mode))

    def _replace_partitions(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        self.replace_partitions_calls.append((frame, partition_cols, schema_mode))

    def _replace_where(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _upsert(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        spec: Any,
        existing_schema: dict[str, Any],
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _update(
        self,
        frame: list[int],
        target: ResolvedTarget,
        *,
        spec: Any,
        existing_schema: dict[str, Any],
    ) -> None:
        pass  # Stub — not used in historify dispatch tests.

    def _read_existing_data(
        self,
        target: ResolvedTarget,
        frame: list[int],
        spec: HistorifySpec,
    ) -> list[int] | None:
        return [0] if self._schema_exists else None

    def _historify_backend(self) -> Any:
        return self.historify

    def _write_file(self, frame: list[int], spec: Any, *, streaming: bool) -> None:
        pass  # Stub — not used in historify dispatch tests.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spec(
    effective_date: Any = params.run_date,
    schema_mode: SchemaMode = SchemaMode.STRICT,
    partition_scope: tuple[str, ...] | None = None,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("wh.dim_players"),
        keys=("player_id",),
        effective_date=effective_date,
        mode=HistorifyInputMode.SNAPSHOT,
        track=("team_id",),
        delete_policy=DeletePolicy.CLOSE,
        partition_scope=partition_scope,
        schema_mode=schema_mode,
    )


# ---------------------------------------------------------------------------
# Dispatch routing
# ---------------------------------------------------------------------------


class TestHistorifyDispatch:
    def test_historify_spec_routes_to_do_historify(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1, 2, 3], _spec(), object())
        assert len(writer.historify_calls) == 1

    def test_historify_receives_materialized_frame(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        frame = [10, 20]
        writer.write(frame, _spec(), None)
        received_frame, _, _ = writer.historify_calls[0]
        assert received_frame == [10, 20]

    def test_historify_receives_spec(self) -> None:
        spec = _spec()
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], spec, None)
        _, received_spec, _ = writer.historify_calls[0]
        assert received_spec is spec

    def test_historify_receives_params_instance(self) -> None:
        params_obj = object()
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], _spec(), params_obj)
        _, _, received_params = writer.historify_calls[0]
        assert received_params is params_obj

    def test_non_historify_spec_not_routed_to_historify(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        # AppendSpec hits _do_append, not _do_historify
        append = AppendSpec(table_ref=TableRef("t.out"))
        writer.write([1], append, None)
        assert writer.historify_calls == []


# ---------------------------------------------------------------------------
# Missing-table policy enforcement
# ---------------------------------------------------------------------------


class TestHistorifyMissingTablePolicy:
    def test_missing_table_with_overwrite_schema_allows_creation(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False,
            missing_table_policy=MissingTablePolicy.SCHEMA_MODE,
        )
        spec = _spec(schema_mode=SchemaMode.OVERWRITE)
        writer.write([1], spec, None)
        # No error raised — _historify is called
        assert len(writer.historify_calls) == 1

    def test_missing_table_with_create_policy_allows_creation(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False,
            missing_table_policy=MissingTablePolicy.CREATE,
        )
        writer.write([1], _spec(), None)
        assert len(writer.historify_calls) == 1

    def test_missing_table_with_schema_mode_strict_raises(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False,
            missing_table_policy=MissingTablePolicy.SCHEMA_MODE,
        )
        spec = _spec(schema_mode=SchemaMode.STRICT)
        with pytest.raises(SchemaNotFoundError):
            writer.write([1], spec, None)

    def test_missing_table_with_error_policy_raises_regardless_of_schema_mode(self) -> None:
        for schema_mode in (SchemaMode.OVERWRITE, SchemaMode.STRICT):
            writer = _StubWritePolicy(
                schema_exists=False,
                missing_table_policy=MissingTablePolicy.ERROR,
            )
            spec = _spec(schema_mode=schema_mode)
            with pytest.raises(SchemaNotFoundError):
                writer.write([1], spec, None)

    def test_existing_table_always_delegates_to_historify(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], _spec(), None)
        assert len(writer.historify_calls) == 1


# ---------------------------------------------------------------------------
# Temp targets still raise TypeError
# ---------------------------------------------------------------------------


class TestHistorifyDoesNotAffectTempDispatch:
    def test_temp_spec_still_raises_type_error(self) -> None:
        from loom.etl.checkpoint import CheckpointScope

        writer = _StubWritePolicy()
        temp = TempSpec(temp_name="x", temp_scope=CheckpointScope.RUN)
        with pytest.raises(TypeError, match="temp targets"):
            writer.write([1], temp, None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _do_historify return value (repair report passthrough)
# ---------------------------------------------------------------------------


class TestHistorifyRepairReportPassthrough:
    def test_none_returned_when_historify_returns_none(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        result = writer._do_historify([1], writer._resolver.resolve(None), _spec(), None)
        assert result is None

    def test_repair_report_returned_and_logged_when_transform_reports(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        report = HistorifyRepairReport(
            affected_keys=frozenset({("player_id", 42)}),
            dates_requiring_rerun=("2025-01-01",),
            warnings=("rewove 1 record",),
        )
        log = MagicMock()
        monkeypatch.setattr(_write_policy, "_log", log)
        writer = _StubWritePolicy(schema_exists=True)
        writer.historify.report = report
        target = MagicMock()
        target.logical_ref.ref = "wh.dim_players"

        result = writer._do_historify([1], target, _spec(), None)

        assert result is report
        log.warning.assert_called_once()
        assert log.warning.call_args.args == ("historify_temporal_rerun",)
        assert log.warning.call_args.kwargs["target"] == "wh.dim_players"
        assert log.warning.call_args.kwargs["affected_key_count"] == 1
        assert log.warning.call_args.kwargs["dates_requiring_rerun"] == ["2025-01-01"]


# ---------------------------------------------------------------------------
# Historify result write
# ---------------------------------------------------------------------------


class _Existing:
    """Weak-referenceable stand-in for the existing target frame."""


class TestHistorifyResultWrite:
    def test_first_run_creates_table_with_partition_scope(self) -> None:
        writer = _StubWritePolicy(
            schema_exists=False, missing_table_policy=MissingTablePolicy.CREATE
        )
        writer.write([1], _spec(partition_scope=("season",)), None)
        assert writer.create_calls == [([1, -1], SchemaMode.STRICT, ("season",))]
        assert writer.replace_calls == writer.replace_partitions_calls == []

    def test_existing_table_is_replaced(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], _spec(), None)
        assert writer.replace_calls == [([1, -1], SchemaMode.STRICT)]

    def test_existing_table_with_partition_scope_replaces_partitions(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        writer.write([1], _spec(partition_scope=("season",)), None)
        assert writer.replace_partitions_calls == [([1, -1], ("season",), SchemaMode.STRICT)]

    def test_existing_frame_is_released_before_the_write(self) -> None:
        class _TrackingWriter(_StubWritePolicy):
            existing_ref: weakref.ref[_Existing]
            alive_at_write: bool | None = None

            def _read_existing_data(self, target: Any, frame: Any, spec: Any) -> Any:
                existing = _Existing()
                self.existing_ref = weakref.ref(existing)
                return existing

            def _replace(self, frame: Any, target: Any, *, schema_mode: Any) -> None:
                self.alive_at_write = self.existing_ref() is not None

        writer = _TrackingWriter(schema_exists=True)
        writer.write([1], _spec(), None)
        assert writer.alive_at_write is False


# ---------------------------------------------------------------------------
# Write-mode routing — every mode reaches its own hook
# ---------------------------------------------------------------------------

_HOOKS = (
    "_append",
    "_replace",
    "_replace_partitions",
    "_replace_where",
    "_upsert",
    "_update",
    "_historify_backend",
)

_TABLE_REF = TableRef("wh.players")

_WRITE_MODES: tuple[tuple[Any, str], ...] = (
    (AppendSpec(table_ref=_TABLE_REF), "_append"),
    (ReplaceSpec(table_ref=_TABLE_REF), "_replace"),
    (
        ReplacePartitionsSpec(table_ref=_TABLE_REF, partition_cols=("season",)),
        "_replace_partitions",
    ),
    (
        ReplaceWhereSpec(
            table_ref=_TABLE_REF,
            replace_predicate=col("season") == params.run_date.year,
        ),
        "_replace_where",
    ),
    (UpsertSpec(table_ref=_TABLE_REF, upsert_keys=("player_id",)), "_upsert"),
    (UpdateSpec(table_ref=_TABLE_REF, keys=("player_id",)), "_update"),
    (_spec(), "_historify_backend"),
)


def _recorder(log: list[str], name: str) -> Callable[..., None]:
    """Return a hook stand-in that records its own name when invoked."""

    def record(*_args: Any, **_kwargs: Any) -> Any:
        log.append(name)
        return _FakeHistorifyBackend() if name == "_historify_backend" else None

    return record


class TestWriteModeRouting:
    @pytest.mark.parametrize(("spec", "hook"), _WRITE_MODES)
    def test_a_write_mode_reaches_only_its_own_hook(
        self, spec: Any, hook: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        reached: list[str] = []
        for name in _HOOKS:
            monkeypatch.setattr(writer, name, _recorder(reached, name))

        writer.write([1], spec, None)

        writes_history = hook == "_historify_backend"
        assert reached == ([hook, "_replace"] if writes_history else [hook])

    def test_a_spec_that_is_not_a_table_write_mode_raises(self) -> None:
        writer = _StubWritePolicy(schema_exists=True)
        spec = MagicMock()
        with pytest.raises(TypeError, match="Unsupported target spec"):
            writer.write([1], spec, None)


class TestReplaceWherePredicateOrder:
    """The predicate is compiled before the frame is materialised."""

    class _FailingWriter(_StubWritePolicy):
        def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
            raise ValueError("unresolvable predicate")

        def _materialize_for_write(self, frame: list[int], streaming: bool) -> list[int]:
            raise SchemaError("null-dtype column")

    def test_an_unresolvable_predicate_fails_before_materialization(self) -> None:
        writer = self._FailingWriter(schema_exists=True)
        spec = ReplaceWhereSpec(
            table_ref=_TABLE_REF,
            replace_predicate=col("season") == params.run_date.year,
        )

        with pytest.raises(ValueError, match="unresolvable predicate"):
            writer.write([1], spec, None)
