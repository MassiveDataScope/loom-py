"""Unit tests for compile-time IntoHistory validators.

Covers:
* validate_historify_spec — keys non-empty, track/keys overlap, effective_date
  not in track, boundary cols disjoint, LOG mode not ParamExpr, delete_marker
  LOG-only and disjoint from declared and partition columns, warnings
* validate_param_exprs — effective_date ParamExpr resolves to known field
* Non-historify specs are silently skipped
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pytest

from loom.core.schema import SchemaMode
from loom.etl import ETLParams, ETLStep, IntoHistory, IntoTable
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler._validators import validate_historify_spec, validate_param_exprs
from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyInputMode,
    HistorifySpec,
)
from loom.etl.declarative.target._table import AppendSpec

# ---------------------------------------------------------------------------
# Params types for use in tests
# ---------------------------------------------------------------------------


class _DailyParams(ETLParams):
    run_date: date
    season: str


class _MinimalParams(ETLParams):
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _history_spec(
    ref: str = "wh.dim_players",
    keys: tuple[str, ...] = ("player_id",),
    track: tuple[str, ...] | None = ("team_id",),
    effective_date: Any = p.run_date,
    mode: HistorifyInputMode = HistorifyInputMode.SNAPSHOT,
    partition_scope: tuple[str, ...] | None = None,
    allow_temporal_rerun: bool = False,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef(ref),
        keys=keys,
        effective_date=effective_date,
        mode=mode,
        track=track,
        delete_policy=DeletePolicy.CLOSE,
        schema_mode=SchemaMode.STRICT,
        partition_scope=partition_scope,
        allow_temporal_rerun=allow_temporal_rerun,
    )


def _target(spec: Any) -> TargetBinding:
    return TargetBinding(spec=spec)


def _no_sources() -> tuple[SourceBinding, ...]:
    return ()


# ---------------------------------------------------------------------------
# validate_historify_spec — non-historify specs skipped
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecSkipsOtherSpecs:
    def test_append_spec_skipped(self) -> None:
        spec = AppendSpec(table_ref=TableRef("t.out"))
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_into_table_upsert_skipped(self) -> None:
        spec = IntoTable("t.out").upsert(keys=("id",))._to_spec()
        validate_historify_spec(object, spec)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_historify_spec — valid specs pass
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecValid:
    def test_valid_spec_passes(self) -> None:
        validate_historify_spec(object, _history_spec())  # type: ignore[arg-type]

    def test_multiple_keys_pass(self) -> None:
        spec = _history_spec(keys=("player_id", "league_id"), track=("team_id",))
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_track_none_passes(self) -> None:
        validate_historify_spec(object, _history_spec(track=None))  # type: ignore[arg-type]

    def test_string_effective_date_passes(self) -> None:
        validate_historify_spec(object, _history_spec(effective_date="event_date"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_historify_spec — error: empty keys
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecEmptyKeys:
    def test_empty_keys_raises_with_expected_error(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=(),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_KEYS_EMPTY
        assert exc_info.value.field == "keys"


# ---------------------------------------------------------------------------
# validate_historify_spec — error: keys / track overlap
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecTrackOverlap:
    def test_overlap_raises_with_expected_error(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("player_id", "team_id"),
            track=("team_id", "salary"),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_KEYS_TRACK_OVERLAP
        assert exc_info.value.field == "track"
        assert "team_id" in str(exc_info.value)


# ---------------------------------------------------------------------------
# validate_historify_spec — error: effective_date in track
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecEffectiveDateInTrack:
    def test_raises_when_effective_date_col_in_track(self) -> None:
        spec = _history_spec(effective_date="event_date", track=("event_date", "plan"))
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_EFFECTIVE_DATE_IN_TRACK
        assert exc_info.value.field == "effective_date"

    def test_param_expr_effective_date_not_checked(self) -> None:
        """ParamExpr is not a str — the check is skipped; no error raised."""
        spec = _history_spec(effective_date=p.run_date, track=("run_date",))
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_track_none_not_checked(self) -> None:
        """track=None means no tracking columns — check is skipped."""
        spec = _history_spec(effective_date="event_date", track=None)
        validate_historify_spec(object, spec)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_historify_spec — error: boundary cols overlap keys or track
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecBoundaryColsDisjoint:
    def test_valid_from_in_keys_raises(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("player_id", "valid_from"),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_BOUNDARY_IN_KEYS
        assert exc_info.value.field == "valid_from"

    def test_valid_to_in_track_raises(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("player_id",),
            track=("valid_to", "salary"),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_BOUNDARY_IN_TRACK
        assert exc_info.value.field == "track"

    def test_custom_boundary_name_in_keys_raises(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("player_id", "period_start"),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
            valid_from="period_start",
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_BOUNDARY_IN_KEYS
        assert exc_info.value.field == "valid_from"


# ---------------------------------------------------------------------------
# validate_historify_spec — error: LOG mode with ParamExpr effective_date
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecLogModeParamExpr:
    def test_log_mode_param_expr_raises(self) -> None:
        spec = _history_spec(
            effective_date=p.run_date,
            mode=HistorifyInputMode.LOG,
            track=("plan",),
        )
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_LOG_EFFECTIVE_DATE_NOT_COLUMN
        assert exc_info.value.field == "effective_date"

    def test_log_mode_string_effective_date_passes(self) -> None:
        spec = _history_spec(effective_date="event_date", mode=HistorifyInputMode.LOG)
        validate_historify_spec(object, spec)  # type: ignore[arg-type]

    def test_snapshot_mode_param_expr_passes(self) -> None:
        spec = _history_spec(effective_date=p.run_date, mode=HistorifyInputMode.SNAPSHOT)
        validate_historify_spec(object, spec)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_historify_spec — error: delete_marker misuse
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecDeleteMarker:
    def _spec(self, mode: HistorifyInputMode, delete_marker: str) -> HistorifySpec:
        return HistorifySpec(
            table_ref=TableRef("wh.dim"),
            keys=("id",),
            track=("status",),
            effective_date="event_date" if mode is HistorifyInputMode.LOG else p.run_date,
            mode=mode,
            partition_scope=("season",),
            delete_marker=delete_marker,
        )

    def test_log_mode_marker_passes(self) -> None:
        validate_historify_spec(object, self._spec(HistorifyInputMode.LOG, "is_deleted"))  # type: ignore[arg-type]

    def test_snapshot_mode_marker_raises(self) -> None:
        spec = self._spec(HistorifyInputMode.SNAPSHOT, "is_deleted")
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_DELETE_MARKER_REQUIRES_LOG
        assert exc_info.value.field == "delete_marker"

    @pytest.mark.parametrize("marker", ["id", "status", "event_date", "valid_to", "season"])
    def test_marker_colliding_with_declared_column_raises(self, marker: str) -> None:
        spec = self._spec(HistorifyInputMode.LOG, marker)
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert exc_info.value.code is ETLErrorCode.HISTORIFY_DELETE_MARKER_DECLARED_COLUMN
        assert exc_info.value.field == "delete_marker"


# ---------------------------------------------------------------------------
# validate_historify_spec — warning: no partition_scope
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecWarnNoPartitionScope:
    def test_emits_warning_when_partition_scope_none(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        spec = _history_spec(partition_scope=None)
        with caplog.at_level(logging.WARNING, logger="loom.etl.compiler._validators_step"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert any("partition_scope" in msg for msg in caplog.messages)

    def test_no_warning_when_partition_scope_set(self, caplog: pytest.LogCaptureFixture) -> None:
        spec = _history_spec(partition_scope=("season",))
        with caplog.at_level(logging.WARNING, logger="loom.etl.compiler._validators_step"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        partition_warnings = [m for m in caplog.messages if "partition_scope" in m]
        assert not partition_warnings


# ---------------------------------------------------------------------------
# validate_historify_spec — warning: allow_temporal_rerun without scope
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecWarnRerunWithoutScope:
    def test_emits_warning_when_rerun_without_scope(self, caplog: pytest.LogCaptureFixture) -> None:
        spec = _history_spec(allow_temporal_rerun=True, partition_scope=None)
        with caplog.at_level(logging.WARNING, logger="loom.etl.compiler._validators_step"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert any("re-weave" in msg for msg in caplog.messages)

    def test_no_rerun_warning_when_scope_is_set(self, caplog: pytest.LogCaptureFixture) -> None:
        spec = _history_spec(allow_temporal_rerun=True, partition_scope=("season",))
        with caplog.at_level(logging.WARNING, logger="loom.etl.compiler._validators_step"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        rerun_warnings = [m for m in caplog.messages if "re-weave" in m]
        assert not rerun_warnings


# ---------------------------------------------------------------------------
# validate_historify_spec — warning: track=None
# ---------------------------------------------------------------------------


class TestValidateHistorifySpecWarnTrackNone:
    def test_emits_warning_when_track_none(self, caplog: pytest.LogCaptureFixture) -> None:
        spec = _history_spec(track=None)
        with caplog.at_level(logging.WARNING, logger="loom.etl.compiler._validators_step"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        assert any("track=None" in msg or "inferred" in msg for msg in caplog.messages)

    def test_no_track_warning_when_track_is_set(self, caplog: pytest.LogCaptureFixture) -> None:
        spec = _history_spec(track=("team_id",), partition_scope=("season",))
        with caplog.at_level(logging.WARNING, logger="loom.etl.compiler._validators_step"):
            validate_historify_spec(object, spec)  # type: ignore[arg-type]
        track_warnings = [m for m in caplog.messages if "track=None" in m or "inferred" in m]
        assert not track_warnings


# ---------------------------------------------------------------------------
# validate_param_exprs — effective_date as ParamExpr
# ---------------------------------------------------------------------------


class TestValidateParamExprsHistorify:
    """validate_param_exprs must extract effective_date from HistorifySpec."""

    def test_valid_param_expr_passes(self) -> None:
        spec = _history_spec(effective_date=p.run_date)
        validate_param_exprs(object, _DailyParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_unknown_param_field_raises(self) -> None:
        spec = _history_spec(effective_date=p.snapshot_date)  # not in _DailyParams
        sources = _no_sources()
        target = _target(spec)
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(object, _DailyParams, sources, target)  # type: ignore[arg-type]
        assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
        assert exc_info.value.field == "snapshot_date"

    def test_string_effective_date_not_validated(self) -> None:
        """String effective_date (LOG mode) is a column name, not a ParamExpr."""
        spec = _history_spec(effective_date="event_date")
        validate_param_exprs(object, _MinimalParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_nested_param_expr_path_validates_root(self) -> None:
        """params.run_date.year — root field 'run_date' is validated."""
        spec = _history_spec(effective_date=p.run_date.year)
        validate_param_exprs(object, _DailyParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_non_struct_params_skips_validation(self) -> None:
        """Non-msgspec params type → validation is skipped gracefully."""

        class _PlainParams:
            run_date: date

        spec = _history_spec(effective_date=p.anything_goes)
        validate_param_exprs(object, _PlainParams, _no_sources(), _target(spec))  # type: ignore[arg-type]

    def test_non_historify_spec_not_affected(self) -> None:
        """Existing specs are unaffected by the new historify branch."""
        append_spec = AppendSpec(table_ref=TableRef("t.out"))
        validate_param_exprs(object, _DailyParams, _no_sources(), _target(append_spec))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Full validate_step integration (via ETLStep declaration)
# ---------------------------------------------------------------------------


class _PlayerDimStep(ETLStep[_DailyParams]):
    target = IntoHistory(
        "warehouse.dim_players",
        keys=("player_id",),
        track=("team_id", "salary"),
        effective_date=p.run_date,
        mode="snapshot",
    )

    def execute(self, params: _DailyParams) -> Any:
        return None


class _LoanStep(ETLStep[_DailyParams]):
    """Multiple open vectors — player belongs to two clubs simultaneously."""

    target = IntoHistory(
        "warehouse.dim_contracts",
        keys=("player_id",),
        track=("team_id", "role"),
        effective_date=p.run_date,
        mode="snapshot",
    )

    def execute(self, params: _DailyParams) -> Any:
        return None


class _LogModeStep(ETLStep[_DailyParams]):
    """LOG mode — effective_date is a column name, not a param."""

    target = IntoHistory(
        "warehouse.dim_subscriptions",
        keys=("subscription_id",),
        track=("plan",),
        effective_date="event_date",
        mode="log",
    )

    def execute(self, params: _DailyParams) -> Any:
        return None


class TestValidateStepIntegration:
    """End-to-end: validate_step accepts valid IntoHistory declarations."""

    def test_player_dim_step_compiles(self) -> None:
        from loom.etl.compiler import ETLCompiler
        from loom.etl.testing import StubCatalog

        catalog = StubCatalog(tables={"warehouse.dim_players": ()})
        compiler = ETLCompiler(catalog=catalog)
        compiler.compile_step(_PlayerDimStep)

    def test_loan_step_compiles(self) -> None:
        from loom.etl.compiler import ETLCompiler
        from loom.etl.testing import StubCatalog

        catalog = StubCatalog(tables={"warehouse.dim_contracts": ()})
        compiler = ETLCompiler(catalog=catalog)
        compiler.compile_step(_LoanStep)

    def test_log_mode_step_compiles(self) -> None:
        from loom.etl.compiler import ETLCompiler
        from loom.etl.testing import StubCatalog

        catalog = StubCatalog(tables={"warehouse.dim_subscriptions": ()})
        compiler = ETLCompiler(catalog=catalog)
        compiler.compile_step(_LogModeStep)
