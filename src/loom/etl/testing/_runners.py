"""PolarsStepRunner — Polars step test harness, in memory or on a local Delta root."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import polars as pl

from loom.core.config import ConfigContext
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends.polars._dtype import loom_type_to_polars
from loom.etl.backends.polars._reader import _finalize_source_frame
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.declarative.target._table import ReplaceWhereSpec, UpsertSpec
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec
from loom.etl.executor import ETLExecutor
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.schema._schema import LoomType
from loom.etl.testing._delta import DeltaTables
from loom.etl.testing._result import StepResult
from loom.etl.testing._scenario import ETLScenario
from loom.etl.testing._temps import RunnerTemps

SeedSchema = Mapping[str, LoomType]
TempData = Sequence[tuple[Any, ...]] | pl.DataFrame | pl.LazyFrame | StepResult


class _PolarsCapturingWriter:
    def __init__(self) -> None:
        self.frame: pl.LazyFrame | pl.DataFrame | None = None
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
        _ = (streaming, write_ctx)
        self.frame = frame
        self.spec = spec
        self._last_params = params_instance


class _DeltaCapturingWriter(_PolarsCapturingWriter):
    """Capture the step output, then hand the same rows to the real writer.

    The frame is collected once before the write, so a step that reads its own
    target is not evaluated again against the table it just changed.
    """

    def __init__(self, target: TargetWriter) -> None:
        super().__init__()
        self._target = target

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        params_instance: Any,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        lazy = isinstance(frame, pl.LazyFrame)
        captured = frame.collect() if lazy else frame
        super().write(captured, spec, params_instance)
        self._target.write(
            captured.lazy() if lazy else captured,
            spec,
            params_instance,
            streaming=streaming,
            write_ctx=write_ctx,
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
        ctx = pl.SQLContext()
        for name, frame in frames.items():
            ctx.register(name, frame)
        return ctx.execute(query, eager=False)


def _build_lazy_frame(
    data: Sequence[tuple[Any, ...]],
    columns: Sequence[str],
    *,
    schema: SeedSchema | None = None,
) -> pl.LazyFrame:
    dtypes = _polars_dtypes(columns, schema)
    series = [
        pl.Series(name, [row[i] for row in data], dtype=dtypes.get(name))
        for i, name in enumerate(columns)
    ]
    return pl.DataFrame(series).lazy()


def _polars_dtypes(columns: Sequence[str], schema: SeedSchema | None) -> dict[str, Any]:
    if not schema:
        return {}
    unknown = sorted(set(schema) - set(columns))
    if unknown:
        raise ValueError(f"schema names columns that are not seeded: {unknown}")
    return {name: loom_type_to_polars(dtype) for name, dtype in schema.items()}


def _temp_frame(
    data: TempData,
    columns: Sequence[str] | None,
    schema: SeedSchema | None,
) -> pl.LazyFrame:
    if isinstance(data, StepResult):
        return _cast(data.to_polars().lazy(), schema)
    if isinstance(data, pl.DataFrame):
        return _cast(data.lazy(), schema)
    if isinstance(data, pl.LazyFrame):
        return _cast(data, schema)
    if columns is None:
        raise ValueError("seed_temp with row tuples needs columns=.")
    return _build_lazy_frame(data, columns, schema=schema)


def _require_typed_columns(ref: str, frame: pl.LazyFrame) -> None:
    untyped = [name for name, dtype in frame.collect_schema().items() if dtype == pl.Null]
    if untyped:
        raise ValueError(
            f"seed({ref!r}): columns {untyped} have no dtype because they are empty or "
            "all null, and Delta cannot store them. Declare their types with schema=."
        )


def _cast(frame: pl.LazyFrame, schema: SeedSchema | None) -> pl.LazyFrame:
    if not schema:
        return frame
    dtypes = _polars_dtypes(frame.collect_schema().names(), schema)
    return frame.with_columns(pl.col(name).cast(dtype) for name, dtype in dtypes.items())


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
        self._temps = RunnerTemps()
        self._writer = _PolarsCapturingWriter()
        self._config_context: ConfigContext | None = None
        self._delta: DeltaTables | None = None
        if delta_root is not None:
            self._delta = DeltaTables(delta_root)
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
            ValueError: With ``delta_root``, when a column has no dtype because
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

        ``FromTemp`` reads temps by ``temp_name``, not by its alias.  Pass the
        :class:`StepResult` of an ``IntoTemp`` step alone to seed it under the
        name it was written with.

        The temp is seeded in RUN scope.  ``FromTemp`` looks in RUN scope
        before CORRELATION scope, as in production, so a seeded temp hides one
        that a step writes with ``IntoTemp(scope=CORRELATION)`` under the same
        name.  The result of that step's :meth:`run` is still read from the
        scope it wrote.

        Args:
            name:    Temp name, or the result of an ``IntoTemp`` step.
            data:    Row tuples (with *columns*), a Polars frame, or a
                     :class:`StepResult`.
            columns: Column names aligned with the tuple positions.
            schema:  Loom types of some or all columns.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            ValueError: When the temp name cannot be determined, or row tuples
                come without *columns*.
        """
        if isinstance(name, StepResult):
            if data is not None:
                raise ValueError("seed_temp(result) takes no data; the result is the data.")
            if name.temp_name is None:
                raise ValueError(
                    "seed_temp needs a temp name: this StepResult is not from an IntoTemp step."
                )
            data = name
            name = name.temp_name
        if data is None:
            raise ValueError(f"seed_temp({name!r}) needs data.")
        self._temps.put(name, _temp_frame(data, columns, schema))
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
            temp for an ``IntoTemp`` target, otherwise the frame the step wrote.

        Raises:
            KeyError:     When a source table was not seeded (in memory).
            FileNotFoundError: When a ``FromTemp`` temp was neither seeded nor written.
            ETLCompilationError: When a ``FromConfig`` key is not in
                :meth:`with_config`.
            RuntimeError: When the step produced no output.
        """
        plan = ETLCompiler(config_context=self._config_context).compile_step(step_cls)
        reader, self._writer = self._io()
        version_before = self._target_version(plan)
        ETLExecutor(
            reader,
            self._writer,
            checkpoint_store=self._temps.store,
            config_context=self._config_context,
        ).run_step(plan, params, self._temps.context)
        raw = self._writer.frame
        if raw is not None:
            return self._table_result(raw, version_before)
        spec = plan.target_binding.spec
        if isinstance(spec, (TempSpec, TempFanInSpec)):
            frame = self._temps.read(spec.temp_name, spec.temp_scope)
            return StepResult(frame, temp_name=spec.temp_name)
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

    def _io(self) -> tuple[SourceReader, _PolarsCapturingWriter]:
        if self._delta is None:
            return _PolarsStubReader(dict(self._tables)), _PolarsCapturingWriter()
        return self._delta.reader, _DeltaCapturingWriter(self._delta.writer)

    def _target_version(self, plan: Any) -> int | None:
        if self._delta is None:
            return None
        return self._delta.version(plan.target_binding.spec)

    def _table_result(
        self, raw: pl.LazyFrame | pl.DataFrame, version_before: int | None
    ) -> StepResult:
        frame = raw.collect() if isinstance(raw, pl.LazyFrame) else raw
        spec = self._writer.spec
        delete_flag = spec.delete_flag if isinstance(spec, UpsertSpec) else None
        metrics = None
        if self._delta is not None and spec is not None:
            metrics = self._delta.merge_metrics(spec, version_before)
        return StepResult(frame, delete_flag=delete_flag, merge_metrics=metrics)
