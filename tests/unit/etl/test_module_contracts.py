"""Normal runtime contract tests for ETL definition modules."""

from __future__ import annotations

import importlib
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest


def _reload_module(name: str) -> ModuleType:
    return importlib.reload(importlib.import_module(name))


def _reload_modules(*names: str) -> dict[str, ModuleType]:
    return {name: _reload_module(name) for name in names}


def test_definition_modules_support_reload_contracts() -> None:
    modules = _reload_modules(
        "loom.etl.declarative._format",
        "loom.etl.declarative._read_options",
        "loom.etl.declarative._write_options",
        "loom.etl.declarative.target._file",
        "loom.etl.declarative.target._table",
        "loom.etl.declarative.target._temp",
        "loom.etl.schema._schema",
    )

    assert modules["loom.etl.declarative._format"].Format.CSV.value == "csv"
    assert modules["loom.etl.declarative._read_options"].CsvReadOptions().separator == ","
    assert (
        modules["loom.etl.declarative._write_options"].ParquetWriteOptions().compression == "zstd"
    )
    assert modules["loom.etl.declarative.target._file"].FileSpec is not None
    assert modules["loom.etl.declarative.target._table"].AppendSpec is not None
    assert modules["loom.etl.declarative.target._temp"].TempSpec is not None
    assert modules["loom.etl.schema._schema"].ColumnSchema is not None


@pytest.mark.parametrize(
    ("module_name", "exported_symbol"),
    [
        ("loom.etl", "FromTable"),
        ("loom.etl.compiler", "ETLCompiler"),
        ("loom.etl.executor", "ETLExecutor"),
        ("loom.etl.declarative", "IntoTable"),
        ("loom.etl.pipeline", "ETLStep"),
        ("loom.etl.runner", "ETLRunner"),
        ("loom.etl.schema", "ColumnSchema"),
        ("loom.etl.storage", "StorageConfig"),
        ("loom.etl.checkpoint", "CheckpointStore"),
        ("loom.etl.testing", "PolarsStepRunner"),
        ("loom.etl.compiler", "validate_plan_catalog"),
        ("loom.etl.compiler", "filter_plan"),
        ("loom.etl.compiler", "InvalidStageError"),
        ("loom.etl.compiler", "iter_processes"),
        ("loom.etl.compiler", "iter_steps_in_process"),
        ("loom.etl.compiler", "iter_all_steps"),
        ("loom.etl.lineage", "LineageConfig"),
        ("loom.etl.lineage.sinks", "TableLineageStore"),
    ],
)
def test_package_entrypoints_support_reload_contracts(
    module_name: str,
    exported_symbol: str,
) -> None:
    module = _reload_module(module_name)
    assert exported_symbol in module.__all__


@pytest.mark.parametrize(
    ("module_name", "attribute_name"),
    [
        ("loom.etl.lineage._records", "RunContext"),
        ("loom.core.observability.protocol", "LifecycleObserver"),
        ("loom.etl.lineage.sinks._protocol", "LineageStore"),
        ("loom.etl.pipeline._params", "ETLParams"),
        ("loom.etl.runtime.contracts", "TableDiscovery"),
        ("loom.etl.lineage._config", "ETLObservabilityConfig"),
    ],
)
def test_internal_protocol_and_event_modules_support_reload_contracts(
    module_name: str,
    attribute_name: str,
) -> None:
    module = _reload_module(module_name)
    assert getattr(module, attribute_name) is not None

    temp_scope = _reload_module("loom.etl.checkpoint._scope")
    assert temp_scope.CheckpointScope.RUN.value == "run"


def test_reload_sql_and_compiler_definition_modules_with_basic_usage() -> None:
    modules = _reload_modules(
        "loom.etl.compiler._errors",
        "loom.etl.declarative.target",
        "loom.etl.declarative.expr._predicate",
        "loom.etl.declarative.expr._predicate_dialect",
        "loom.etl.backends._predicate",
    )

    compiler_errors = modules["loom.etl.compiler._errors"]
    target_variants = modules["loom.etl.declarative.target"]
    predicate_nodes = modules["loom.etl.declarative.expr._predicate"]
    predicate_dialect = modules["loom.etl.declarative.expr._predicate_dialect"]
    predicate_utils = modules["loom.etl.backends._predicate"]

    class _Step:
        __qualname__ = "ReloadStep"

    err = compiler_errors.ETLCompilationError.missing_target(_Step)
    assert err.code is compiler_errors.ETLErrorCode.MISSING_TARGET
    assert "ReloadStep" in str(err)
    assert "TargetSpec" in target_variants.__all__

    class _Dialect:
        def column(self, name: str) -> str:
            return f"col:{name}"

        def literal(self, value: object) -> str:
            return f"lit:{value!r}"

        def eq(self, left: str, right: str) -> str:
            return f"{left}={right}"

        def ne(self, left: str, right: str) -> str:
            return f"{left}!={right}"

        def gt(self, left: str, right: str) -> str:
            return f"{left}>{right}"

        def ge(self, left: str, right: str) -> str:
            return f"{left}>={right}"

        def lt(self, left: str, right: str) -> str:
            return f"{left}<{right}"

        def le(self, left: str, right: str) -> str:
            return f"{left}<={right}"

        def in_(self, ref: str, values: tuple[object, ...]) -> str:
            return f"{ref} in {values!r}"

        def and_(self, left: str, right: str) -> str:
            return f"({left} and {right})"

        def or_(self, left: str, right: str) -> str:
            return f"({left} or {right})"

        def not_(self, operand: str) -> str:
            return f"(not {operand})"

    node = predicate_nodes.EqPred(left=1, right=1)
    rendered = predicate_dialect.fold_predicate(node, object(), _Dialect())
    assert rendered == "lit:1=lit:1"
    assert callable(predicate_utils.predicate_to_sql)


def test_invalid_stage_error_is_one_class_across_its_import_paths() -> None:
    import loom.etl
    import loom.etl.compiler
    import loom.etl.runner
    import loom.etl.runner.errors
    import loom.etl.runner.filtering

    assert loom.etl.runner.InvalidStageError is loom.etl.compiler.InvalidStageError
    assert loom.etl.runner.errors.InvalidStageError is loom.etl.compiler.InvalidStageError
    assert loom.etl.InvalidStageError is loom.etl.compiler.InvalidStageError
    assert loom.etl.runner.filtering._filter_plan is loom.etl.compiler.filter_plan


def test_runner_error_message_includes_requested_include_set() -> None:
    from loom.etl.runner.errors import InvalidStageError

    err = InvalidStageError(frozenset({"MissingStep", "MissingProcess"}))
    text = str(err)
    assert "No steps or processes match include=" in text
    assert "MissingStep" in text
    assert "MissingProcess" in text


def test_reload_source_and_target_modules_with_real_builder_calls() -> None:
    modules = _reload_modules(
        "loom.etl.declarative.source._from", "loom.etl.declarative.target._into"
    )
    source_mod = modules["loom.etl.declarative.source._from"]
    target_mod = modules["loom.etl.declarative.target._into"]

    from loom.etl.declarative.expr._params import params
    from loom.etl.declarative.expr._refs import col

    source_spec = (
        source_mod.FromTable("raw.orders")
        .where(col("year") == params.run_date.year)
        .columns("id", "amount")
        .parse_json("payload", list[str])
        ._to_spec("orders")
    )
    assert source_spec.alias == "orders"
    assert source_spec.columns == ("id", "amount")
    assert source_spec.json_columns[0].column == "payload"

    file_spec = (
        source_mod.FromFile("s3://bucket/events.json", format=source_mod.Format.JSON)
        .columns("id")
        .parse_json("body", list[str])
        ._to_spec("events")
    )
    assert file_spec.path == "s3://bucket/events.json"
    assert file_spec.columns == ("id",)
    assert file_spec.json_columns[0].column == "body"

    target = target_mod.IntoTable("staging.orders").replace_partition(year=params.run_date.year)
    compiled_target = target._to_spec()
    assert compiled_target.table_ref.ref == "staging.orders"
    assert compiled_target.replace_predicate is not None

    file_target = target_mod.IntoFile(
        "/var/lib/loom/out.csv", format=target_mod.Format.CSV
    )._to_spec()
    assert file_target.path == "/var/lib/loom/out.csv"

    temp_target = target_mod.IntoTemp("parts", append=True)._to_spec()
    assert temp_target.temp_name == "parts"


def test_reload_checkpoint_store_module_and_helpers() -> None:
    _reload_module("loom.etl.checkpoint._store")
    polars_temp_mod = _reload_module("loom.etl.checkpoint._backends._polars")
    spark_temp_mod = _reload_module("loom.etl.checkpoint._backends._spark")

    import polars as pl

    real_polars = pl.DataFrame().lazy()
    fake_spark = type("DataFrame", (), {"__module__": "pyspark.sql.dataframe"})()

    assert polars_temp_mod._is_polars_lazy_frame(real_polars)
    assert not polars_temp_mod._is_polars_lazy_frame(object())
    assert spark_temp_mod._is_spark_dataframe(fake_spark)

    base = "/var/lib/loom/test"
    single = polars_temp_mod._arrow_path("orders", base)
    part = polars_temp_mod._arrow_part("orders", base)
    assert single.endswith("/orders.arrow")
    assert "/orders/" in part
    assert part.endswith(".arrow")

    class _Reader:
        def parquet(self, _path: str) -> object:
            analysis_exc = type(
                "AnalysisException",
                (Exception,),
                {"__module__": "pyspark.sql.utils"},
            )
            raise analysis_exc("missing")

    class _Spark:
        read = _Reader()

    assert spark_temp_mod._probe_spark(_Spark(), "/var/lib/loom/missing") is None

    # Verify _join_path lives in _paths for shared path construction
    paths_mod = _reload_module("loom.etl.checkpoint._paths")
    join_path = cast(Any, vars(paths_mod)["_join_path"])
    assert join_path("/var/lib/loom", "runs", "abc") == "/var/lib/loom/runs/abc"

    class _ReaderUnexpected:
        def parquet(self, _path: str) -> object:
            raise RuntimeError("boom")

    class _SparkUnexpected:
        read = _ReaderUnexpected()

    unexpected = _SparkUnexpected()
    with pytest.raises(RuntimeError, match="boom"):
        spark_temp_mod._probe_spark(unexpected, "/var/lib/loom/missing")


@pytest.mark.parametrize(
    ("module_name", "attribute_name"),
    [
        ("loom.etl.declarative.target._history", "IntoHistory"),
        ("loom.etl.runner.core", "ETLRunner"),
        ("loom.etl.runner._providers", "load_backend_provider"),
        ("loom.etl.runner._wiring", "make_backends"),
        ("loom.etl.runner.config_loader", "_load_yaml"),
        ("loom.etl.storage.routing", "RoutedCatalog"),
        ("loom.etl.storage._file_locator", "MappingFileLocator"),
    ],
)
def test_runtime_modules_support_reload_contracts(
    module_name: str,
    attribute_name: str,
) -> None:
    module = _reload_module(module_name)
    assert getattr(module, attribute_name) is not None


def test_lazy_pipeline_exports_resolve() -> None:
    from loom.etl.pipeline import ETLParams, ETLProcess, ETLStep

    assert ETLParams is not None
    assert ETLStep is not None
    assert ETLProcess is not None


def test_lazy_sql_exports_resolve() -> None:
    from loom.etl.backends._predicate import sql_literal
    from loom.etl.pipeline import StepSQL
    from loom.etl.pipeline._sql import resolve_sql

    assert StepSQL is not None
    assert callable(resolve_sql)
    assert callable(sql_literal)


def test_schema_structural_types_compose_normally() -> None:
    from loom.etl.schema._schema import (
        ColumnSchema,
        DatetimeType,
        ListType,
        LoomDtype,
        StructField,
        StructType,
    )

    payload = StructType(
        fields=(
            StructField("ts", DatetimeType("us", "UTC")),
            StructField("tags", ListType(inner=LoomDtype.UTF8)),
        )
    )
    schema = (
        ColumnSchema("id", LoomDtype.INT64, nullable=False),
        ColumnSchema("payload", payload),
    )
    assert schema[0].nullable is False
    assert isinstance(schema[1].dtype, StructType)


def test_observer_event_records_hold_runtime_data() -> None:
    from loom.etl.lineage._records import (
        EventName,
        PipelineRunRecord,
        ProcessRunRecord,
        RunContext,
        RunStatus,
        StepRunRecord,
    )

    now = datetime.now(UTC)
    ctx = RunContext(run_id="run-1", correlation_id="corr-1", attempt=2, last_attempt=False)
    pipeline = PipelineRunRecord(
        event=EventName.PIPELINE_END,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
        attempt=ctx.attempt,
        pipeline="MyPipeline",
        started_at=now,
        status=RunStatus.SUCCESS,
        duration_ms=100,
        error=None,
    )
    process = ProcessRunRecord(
        event=EventName.PROCESS_END,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
        attempt=ctx.attempt,
        process_run_id="proc-1",
        process="MyProcess",
        started_at=now,
        status=RunStatus.SUCCESS,
        duration_ms=50,
        error=None,
    )
    step = StepRunRecord(
        event=EventName.STEP_END,
        run_id=ctx.run_id,
        correlation_id=ctx.correlation_id,
        attempt=ctx.attempt,
        step_run_id="step-1",
        step="MyStep",
        started_at=now,
        status=RunStatus.SUCCESS,
        duration_ms=10,
        error=None,
    )
    assert pipeline.pipeline == "MyPipeline"
    assert process.process_run_id == "proc-1"
    assert step.step_run_id == "step-1"


def test_storage_protocols_runtime_checkable_contracts() -> None:
    from loom.etl.declarative.expr._refs import TableRef
    from loom.etl.runtime.contracts import SourceReader, SQLExecutor, TableDiscovery, TargetWriter
    from loom.etl.schema._schema import ColumnSchema, LoomDtype

    class _Catalog:
        def exists(self, ref: TableRef) -> bool:
            return ref.ref == "raw.orders"

        def columns(self, ref: TableRef) -> tuple[str, ...]:
            return ("id",)

        def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
            return (ColumnSchema("id", LoomDtype.INT64),)

        def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
            return None

    class _Reader:
        def read(self, spec: object, params_instance: object) -> object:
            return object()

        def execute_sql(self, frames: dict[str, object], query: str) -> object:
            return object()

    class _Writer:
        def write(self, frame: object, spec: object, params_instance: object) -> None:
            return None

    assert isinstance(_Catalog(), TableDiscovery)
    assert isinstance(_Reader(), SourceReader)
    assert isinstance(_Reader(), SQLExecutor)
    assert isinstance(_Writer(), TargetWriter)


def test_temp_scope_enum_values_are_stable() -> None:
    from loom.etl.checkpoint._scope import CheckpointScope

    assert CheckpointScope.RUN.value == "run"
    assert CheckpointScope.CORRELATION.value == "correlation"


def test_public_root_exports_are_importable() -> None:
    import loom.etl as etl

    for name in ("FromTable", "IntoTable", "ETLRunner", "TableRef", "StepSQL"):
        assert hasattr(etl, name)


class TestTestingHelpersDeclareTheirPytestDependency:
    """The public testing helpers define pytest fixtures, so pytest is declared."""

    def test_the_testing_extra_declares_pytest(self) -> None:
        pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
        extra = pyproject["project"]["optional-dependencies"]["testing"]
        assert any(item.startswith("pytest") for item in extra)

    def test_a_missing_pytest_names_the_extra_to_install(self) -> None:
        for module in ("src/loom/etl/testing/spark.py", "src/loom/etl/testing/__init__.py"):
            source = Path(module).read_text(encoding="utf-8")
            assert "loom-kernel[testing]" in source
