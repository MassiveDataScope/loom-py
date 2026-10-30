"""PolarsStepRunner — Polars step test harness, in memory or on a local Delta root."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from loom.core.config import ConfigContext
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.backends.polars._reader import (
    _apply_source_schema,
    _finalize_source_frame,
    execute_sql,
)
from loom.etl.backends.polars._writer import _check_null_dtype_columns_lazy
from loom.etl.checkpoint import CheckpointScope, CheckpointStore
from loom.etl.checkpoint._backends._memory import _InMemoryCheckpointBackend
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.declarative.target._table import ReplaceWhereSpec
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec
from loom.etl.executor import ETLExecutor
from loom.etl.lineage._records import RunContext
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.schema._schema import ColumnSchema, LoomType, SchemaError
from loom.etl.testing._delta import DeltaTables
from loom.etl.testing._result import StepResult

if TYPE_CHECKING:
    from loom.etl.testing._scenario import ETLScenario

SeedSchema = Mapping[str, LoomType]
"""Loom types of some or all seeded columns."""

TempData = Sequence[tuple[Any, ...]] | pl.DataFrame | pl.LazyFrame
"""Data ``seed_temp`` accepts with a temp name: row tuples or a Polars frame."""

_TEMP_ROOT = "memory://loom-step-runner"


class _CapturingWriter:
    """Capture the step output once as a ``DataFrame``, then hand it to *target* if any.

    Collecting before the real write means a step that reads its own target is
    not evaluated again against the table it just changed.
    """

    def __init__(self, target: TargetWriter | None = None) -> None:
        self._target = target
        self.frame: pl.DataFrame | None = None
        self.spec: TargetSpec | None = None
        self._last_params: Any = None

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        params_instance: Any,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        captured = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        self.frame = captured
        self.spec = spec
        self._last_params = params_instance
        if self._target is not None:
            # Forward the type the step returned, so the real writer accepts or rejects it.
            forwarded = captured.lazy() if isinstance(frame, pl.LazyFrame) else frame
            self._target.write(
                forwarded, spec, params_instance, streaming=streaming, write_ctx=write_ctx
            )


class _PolarsStubReader:
    def __init__(self, frames: dict[str, pl.LazyFrame]) -> None:
        self._frames = frames

    def read(
        self,
        spec: SourceSpec,
        params_instance: Any,
        /,
    ) -> pl.LazyFrame:
        table_ref = getattr(spec, "table_ref", None)
        key = table_ref.ref if table_ref is not None else spec.alias
        frame = self._frames[key]
        if isinstance(spec, (TableSourceSpec, FileSourceSpec)):
            return _finalize_source_frame(frame, spec, params_instance)
        return frame

    def read_streaming(
        self,
        spec: SourceSpec,
        params_instance: Any,
        /,
    ) -> pl.LazyFrame:
        return self.read(spec, params_instance)

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> pl.LazyFrame:
        return execute_sql(frames, query)


def _build_lazy_frame(
    data: Sequence[tuple[Any, ...]],
    columns: Sequence[str],
    *,
    schema: SeedSchema | None = None,
) -> pl.LazyFrame:
    _require_known_columns(columns, schema)
    declared = schema or {}
    frame_schema = [
        (name, loom_type_to_polars(declared[name]) if name in declared else None)
        for name in columns
    ]
    return pl.DataFrame(
        list(data), schema=frame_schema, orient="row", infer_schema_length=None
    ).lazy()


def _require_known_columns(columns: Sequence[str], schema: SeedSchema | None) -> None:
    unknown = sorted(set(schema or ()) - set(columns))
    if unknown:
        raise ValueError(f"schema names columns that are not seeded: {unknown}")


def _temp_frame(
    data: TempData,
    columns: Sequence[str] | None,
    schema: SeedSchema | None,
) -> pl.LazyFrame:
    if isinstance(data, pl.DataFrame):
        data = data.lazy()
    if isinstance(data, pl.LazyFrame):
        return _cast(data, schema)
    if columns is None:
        raise ValueError("seed_temp with row tuples needs columns=.")
    return _build_lazy_frame(data, columns, schema=schema)


def _cast(frame: pl.LazyFrame, schema: SeedSchema | None) -> pl.LazyFrame:
    if not schema:
        return frame
    _require_known_columns(frame.collect_schema().names(), schema)
    columns = tuple(ColumnSchema(name, dtype) for name, dtype in schema.items())
    return _apply_source_schema(frame, columns)


def _require_typed_columns(ref: str, frame: pl.LazyFrame) -> None:
    try:
        _check_null_dtype_columns_lazy(frame)
    except SchemaError as exc:
        raise SchemaError(
            f"seed({ref!r}) has columns without dtype because they are empty or all null; "
            f"declare their types with schema=.\n{exc}"
        ) from exc


class PolarsStepRunner:
    """Test harness for Polars :class:`~loom.etl.ETLStep` subclasses.

    Seeds are plain Python tuples — no Polars dependency at definition time.
    Internally, each seed is converted to a ``polars.LazyFrame`` so
    ``execute()`` receives the same type as in production.  Sources are read
    like the real reader does: ``.where()`` filters, ``.columns()`` projects,
    and declared schemas and ``parse_json`` columns are applied.

    Temps (``FromTemp``/``IntoTemp``) live in memory in a real
    :class:`~loom.etl.checkpoint.CheckpointStore`, keyed by ``temp_name``, and
    persist across :meth:`run` calls of the same runner, which keeps one run
    context for its whole life.

    Without ``delta_root`` table writes are captured, not applied.  With
    ``delta_root`` the real Polars reader and writer work on that directory:
    seeds are written as Delta tables, missing targets are created, and
    :meth:`table` returns the final state of a table.

    Cache scenarios and frames, not runners: a runner holds the state its
    steps write, so tests that mutate it (upserts, replaces, fan-in temps)
    need a function-scoped runner, and ``delta_root`` should be the test's
    ``tmp_path``.  Loom does not check fixture scopes.

    Args:
        *scenarios: Scenarios applied in order at construction.
        delta_root: Local directory for real Delta reads and writes.

    Example::

        @pytest.fixture
        def runner(tmp_path):
            return PolarsStepRunner(SILVER_ORDERS + RAW_CDC, delta_root=tmp_path)

        def test_tombstone_is_idempotent(runner):
            runner.run(UpsertOrders, P)
            runner.run(UpsertOrders, P)
            runner.table("silver.orders").assert_count(0)
    """

    def __init__(self, *scenarios: ETLScenario, delta_root: str | Path | None = None) -> None:
        self._tables: dict[str, pl.LazyFrame] = {}
        self._temp_backend = _InMemoryCheckpointBackend()
        self._temps = CheckpointStore(_TEMP_ROOT, backend=self._temp_backend)
        self._context = RunContext(run_id="step-runner-run", correlation_id="step-runner")
        self._writer = _CapturingWriter()
        self._config_context: ConfigContext | None = None
        self._delta = DeltaTables(delta_root) if delta_root is not None else None
        for scenario in scenarios:
            scenario.apply(self)

    def seed(
        self,
        ref: str,
        data: Sequence[tuple[Any, ...]],
        columns: Sequence[str],
        *,
        schema: SeedSchema | None = None,
    ) -> PolarsStepRunner:
        """Register raw data under the logical table reference *ref*.

        With ``delta_root`` the data is written as the Delta table *ref*.

        Args:
            ref:     Logical table reference, e.g. ``"raw.orders"``.
            data:    Row data as a list of tuples.
            columns: Column names aligned with the tuple positions.
            schema:  Loom types of some or all columns.  Declare it for empty
                     or all-null columns, which otherwise have no dtype.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            SchemaError: With ``delta_root``, when a column has no dtype because
                it is empty or all null and *schema* does not declare it.
        """
        frame = _build_lazy_frame(data, columns, schema=schema)
        if self._delta is not None:
            _require_typed_columns(ref, frame)
            self._delta.seed(ref, frame)
        else:
            self._tables[ref] = frame
        return self

    def seed_temp(
        self,
        name: str | StepResult,
        data: TempData | None = None,
        columns: Sequence[str] | None = None,
        *,
        schema: SeedSchema | None = None,
    ) -> PolarsStepRunner:
        """Register data as the temp *name*, as an ``IntoTemp`` step would.

        Two forms:

        * ``seed_temp(name, data, columns=None, *, schema=None)`` — *data* is
          row tuples (with *columns*), a ``pl.DataFrame`` or a ``pl.LazyFrame``.
        * ``seed_temp(result)`` — the :class:`StepResult` of an ``IntoTemp``
          step, seeded under the temp name that step wrote.

        ``FromTemp`` reads temps by ``temp_name``, not by its alias.  The temp
        is seeded in RUN scope.  ``FromTemp`` looks in RUN scope before
        CORRELATION scope, as in production, so a seeded temp hides one that a
        step writes with ``IntoTemp(scope=CORRELATION)`` under the same name.
        The result of that step's :meth:`run` is still the temp it wrote.

        Args:
            name:    Temp name, or the result of an ``IntoTemp`` step.
            data:    Row tuples, a Polars frame; omitted with a result.
            columns: Column names aligned with the tuple positions.
            schema:  Loom types of some or all columns.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            ValueError: When the result is not from an ``IntoTemp`` step, a
                name comes without data, or row tuples come without *columns*.
        """
        if isinstance(name, StepResult):
            if name.temp_name is None:
                raise ValueError(
                    "seed_temp needs a temp name: this StepResult is not from an IntoTemp step."
                )
            data = name.to_polars()
            name = name.temp_name
        if data is None:
            raise ValueError(f"seed_temp({name!r}) needs data.")
        self._temps.put(
            name,
            run_id=self._context.run_id,
            correlation_id=self._context.correlation_id,
            scope=CheckpointScope.RUN,
            data=_temp_frame(data, columns, schema),
        )
        return self

    def with_config(self, values: Mapping[str, Any]) -> PolarsStepRunner:
        """Set the config the step's ``FromConfig`` values resolve from.

        Keys are validated when :meth:`run` compiles the step, exactly as the
        runner does, so a fake token exercises the production path.

        Args:
            values: Config mapping, e.g. ``{"respondio": {"api_token": "fake"}}``.

        Returns:
            ``self`` for fluent chaining.
        """
        self._config_context = ConfigContext.from_dict(values)
        return self

    def run(self, step_cls: type[Any], params: Any) -> StepResult:
        """Compile and execute *step_cls* against the seeded tables and temps.

        Args:
            step_cls: :class:`~loom.etl.ETLStep` subclass to execute.
            params:   Concrete params instance for this run.

        Returns:
            :class:`~loom.etl.testing._result.StepResult` for assertions: the
            temp an ``IntoTemp`` target wrote (accumulated for fan-in),
            otherwise the frame the step wrote.

        Raises:
            KeyError:     When a source table was not seeded (in memory).
            FileNotFoundError: When a ``FromTemp`` temp was neither seeded nor written.
            ETLCompilationError: When a ``FromConfig`` key is not in
                :meth:`with_config`.
            RuntimeError: When the step produced no output.
        """
        plan = ETLCompiler(config_context=self._config_context).compile_step(step_cls)
        reader, self._writer = self._io()
        ETLExecutor(
            reader,
            self._writer,
            checkpoint_store=self._temps,
            config_context=self._config_context,
        ).run_step(plan, params, self._context)
        if self._writer.frame is not None:
            merged = self._delta.last_merge if self._delta is not None else None
            return StepResult(self._writer.frame, target=self._writer.spec, merge_metrics=merged)
        spec = plan.target_binding.spec
        written = self._temp_backend.probe_last()
        if isinstance(spec, (TempSpec, TempFanInSpec)) and written is not None:
            return StepResult(written.collect(), target=spec)
        raise RuntimeError("Step produced no output — check that target is declared.")

    def table(self, ref: str) -> StepResult:
        """Return the current content of the Delta table *ref*.

        Reads the final state as the real reader does: rows deleted by an
        upsert's ``delete_flag`` are gone and the flag column is not stored.

        Raises:
            RuntimeError: When the runner was built without ``delta_root``.
        """
        if self._delta is None:
            raise RuntimeError("table() needs PolarsStepRunner(delta_root=...).")
        return StepResult(self._delta.read(ref))

    @property
    def target_spec(self) -> TargetSpec:
        """Target spec from the last :meth:`run` call.

        Raises:
            RuntimeError: When :meth:`run` has not been called yet.
        """
        if self._writer.spec is None:
            raise RuntimeError("No spec — call run() first.")
        return self._writer.spec

    @property
    def resolved_predicate(self) -> str | None:
        """SQL predicate resolved from the last run's target spec and params.

        Returns ``None`` when the write mode has no predicate.
        """
        spec = self._writer.spec
        if not isinstance(spec, ReplaceWhereSpec):
            return None
        return predicate_to_sql(spec.replace_predicate, self._writer._last_params)

    def _io(self) -> tuple[SourceReader, _CapturingWriter]:
        if self._delta is None:
            return _PolarsStubReader(dict(self._tables)), _CapturingWriter()
        return self._delta.reader, _CapturingWriter(self._delta)
