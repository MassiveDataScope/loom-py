"""A write declared with a model aligns and creates to the model's lake schema, on every engine."""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import polars as pl
import pytest
from sqlalchemy import MetaData

from loom.core.errors import InvalidValue
from loom.core.model import BaseModel
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.expr import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import TableSpec
from loom.etl.declarative.target._history import HistorifyInputMode, HistorifySpec
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.engines.polars import PolarsSourceReader, PolarsTargetWriter, polars_to_frame_type
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.executor import ETLExecutor
from loom.etl.lineage._records import WriteContext
from loom.etl.ports.contracts import TargetWriter
from loom.etl.schema import FrameType, ModelTable
from loom.etl.schema._model import LakeTranslator
from loom.etl.storage._config import AuditConfig, CustomColumnDef, MissingTablePolicy
from loom.stores.sqlalchemy.schema import compile_model
from tests.helpers.loggers import CapturedLog
from tests.unit.etl.engines.doubles import NoParams, RunDateParams
from tests.unit.etl.engines.frame_types import (
    LAKE_ROWS,
    CamelEntity,
    Order,
    Rate,
    SubEntity,
    probe_model,
)

_ORDER = ModelTable.from_model(Order)
_RATE = ModelTable.from_model(Rate)
_PLACED = datetime(2026, 1, 5, tzinfo=UTC)
_SPARK_ACTIONS = ("count", "isEmpty", "collect", "first", "take", "toPandas")


class _Engine(Protocol):
    """What a test needs of a backend: a writer, frames, and what a table stores."""

    writer: TargetWriter

    def frame(self, columns: dict[str, list[Any]]) -> Any: ...

    def stored(self, ref: str) -> list[dict[str, Any]]: ...

    def stored_types(self, ref: str) -> dict[str, FrameType]: ...

    def read_columns(self, source: FromTable) -> list[str]: ...

    def is_json_decode_failure(self, error: Exception) -> bool: ...


def _table_path(root: Path, ref: str) -> str:
    return str(root.joinpath(*ref.split(".")))


class _PolarsEngine:
    def __init__(self, root: Path, audit: AuditConfig | None = None) -> None:
        self._root = root
        self.writer = PolarsTargetWriter(
            str(root), missing_table_policy=MissingTablePolicy.CREATE, audit_config=audit
        )

    def frame(self, columns: dict[str, list[Any]]) -> pl.LazyFrame:
        return pl.LazyFrame(columns)

    def stored(self, ref: str) -> list[dict[str, Any]]:
        return pl.read_delta(_table_path(self._root, ref)).to_dicts()

    def read_columns(self, source: FromTable) -> list[str]:
        frame = PolarsSourceReader(str(self._root)).read(source._to_spec("orders"), None)
        return frame.collect_schema().names()

    def is_json_decode_failure(self, error: Exception) -> bool:
        return PolarsSourceReader(str(self._root)).is_json_decode_failure(error)

    def stored_types(self, ref: str) -> dict[str, FrameType]:
        schema = pl.scan_delta(_table_path(self._root, ref)).collect_schema()
        return {name: polars_to_frame_type(dtype) for name, dtype in schema.items()}


class _SparkEngine:
    def __init__(self, spark: Any, root: Path) -> None:
        from loom.etl.engines.spark import SparkTargetWriter

        self._spark = spark
        self._root = root
        self.writer = SparkTargetWriter(spark, root, missing_table_policy=MissingTablePolicy.CREATE)

    def frame(self, columns: dict[str, list[Any]]) -> Any:
        return self._spark.createDataFrame(list(zip(*columns.values(), strict=True)), list(columns))

    def stored(self, ref: str) -> list[dict[str, Any]]:
        return [row.asDict(recursive=True) for row in self._stored(ref).collect()]

    def read_columns(self, source: FromTable) -> list[str]:
        from loom.etl.engines.spark import SparkSourceReader

        reader = SparkSourceReader(self._spark, str(self._root))
        return reader.read(source._to_spec("orders"), None).columns

    def is_json_decode_failure(self, error: Exception) -> bool:
        from loom.etl.engines.spark import SparkSourceReader

        return SparkSourceReader(self._spark, str(self._root)).is_json_decode_failure(error)

    def stored_types(self, ref: str) -> dict[str, FrameType]:
        from loom.etl.engines.spark._dtype import spark_to_frame_type

        return {f.name: spark_to_frame_type(f.dataType) for f in self._stored(ref).schema.fields}

    def _stored(self, ref: str) -> Any:
        return self._spark.read.format("delta").load(_table_path(self._root, ref))


@pytest.fixture(params=["polars", "spark"])
def engine(request: pytest.FixtureRequest, tmp_path: Path) -> _Engine:
    if request.param == "polars":
        return _PolarsEngine(tmp_path)
    return _SparkEngine(request.getfixturevalue("spark"), tmp_path)


def _spec(factory: Callable[..., TableSpec], ref: str = "sales.order", **fields: Any) -> TableSpec:
    return factory(table_ref=TableRef(ref), model=Order, **fields)


def _orders(engine: _Engine, *order_ids: str, void: Collection[str] | None = None) -> Any:
    count = len(order_ids)
    columns: dict[str, list[Any]] = {
        "order_id": list(order_ids),
        "customer_id": ["c1"] * count,
        "amount": [1.5] * count,
        "placed_at": [_PLACED] * count,
        "noise": [0] * count,
    }
    if void is not None:
        columns["is_void"] = [order_id in void for order_id in order_ids]
    return engine.frame(columns)


_ORDER_WRITES = {
    "append": (lambda: _spec(AppendSpec), None),
    "replace": (lambda: _spec(ReplaceSpec), None),
    "replace-partitions": (
        lambda: _spec(ReplacePartitionsSpec, partition_cols=("customer_id",)),
        None,
    ),
    "replace-partitions-streaming": (
        lambda: _spec(ReplacePartitionsSpec, partition_cols=("customer_id",), streaming=True),
        None,
    ),
    "upsert": (lambda: _spec(UpsertSpec, upsert_keys=("order_id",)), None),
    "update": (
        lambda: _spec(UpdateSpec, keys=("order_id",)),
        lambda: _spec(UpsertSpec, upsert_keys=("order_id",)),
    ),
}


class TestModelAlignTarget:
    @pytest.mark.parametrize("path", _ORDER_WRITES)
    def test_every_path_writes_the_model_types_on_create_and_on_an_existing_table(
        self, engine: _Engine, path: str
    ) -> None:
        spec_factory, creating_factory = _ORDER_WRITES[path]
        creating = (creating_factory or spec_factory)()

        engine.writer.write(_orders(engine, "o1"), creating, None)
        engine.writer.write(_orders(engine, "o1"), spec_factory(), None)

        assert engine.stored_types("sales.order") == _ORDER.column_types

    def test_a_column_the_frame_lacks_is_null_filled_and_an_extra_one_dropped(
        self, engine: _Engine
    ) -> None:
        frame = _orders(engine, "o1").drop("customer_id")

        engine.writer.write(frame, _spec(AppendSpec), None)

        (stored,) = engine.stored("sales.order")
        assert stored.keys() == set(_ORDER.column_names)
        assert stored["customer_id"] is None

    def test_a_second_upsert_updates_without_duplicates(self, engine: _Engine) -> None:
        spec = _spec(UpsertSpec, upsert_keys=("order_id",))

        engine.writer.write(_orders(engine, "o1", "o2"), spec, None)
        engine.writer.write(_orders(engine, "o2", "o3"), spec, None)

        assert sorted(row["order_id"] for row in engine.stored("sales.order")) == [
            "o1",
            "o2",
            "o3",
        ]

    def test_a_historify_result_is_cast_to_the_model_and_logged_once(
        self, engine: _Engine, caplog: pytest.LogCaptureFixture
    ) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("sales.rate"),
            keys=("rate_id",),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
            track=("price",),
            model=Rate,
        )

        with caplog.at_level(logging.INFO):
            engine.writer.write(
                engine.frame({"rate_id": ["r1"], "price": [2.5]}),
                spec,
                RunDateParams(date(2026, 1, 5)),
            )

        assert engine.stored_types("sales.rate") == _RATE.column_types
        assert [row["price"] for row in engine.stored("sales.rate")] == [Decimal("2.50")]
        assert len(CapturedLog(caplog).named(EtlEvent.MODEL_SCHEMA_APPLIED)) == 1


class _Entry(BaseModel):
    __tablename__ = "entries"
    entry_id: int
    profile: SubEntity
    visit: CamelEntity
    tags: list[str]


class _WriteEntries(ETLStep[NoParams]):
    raw: FromTable = FromTable("raw.entries")  # type: ignore[assignment]
    target = IntoTable(_Entry).append()

    def execute(self, params: NoParams, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class _TextReader:
    def __init__(self, engine: _Engine, frame: Any) -> None:
        self._engine = engine
        self._frame = frame

    def read(self, spec: Any, params_instance: Any, /) -> Any:
        return self._frame

    def is_json_decode_failure(self, error: Exception) -> bool:
        return self._engine.is_json_decode_failure(error)


def _entry_text(engine: _Engine, profile: str = '{"street": "Main"}') -> Any:
    return engine.frame(
        {
            "entry_id": [1],
            "profile": [profile],
            "visit": ['{"zipCode": "28001", "seenAt": "2026-01-05T00:00:00Z"}'],
            "tags": ['["a", "b"]'],
        }
    )


class TestJsonTextOnWrite:
    @pytest.fixture(autouse=True)
    def no_python_per_row(self, engine: _Engine, monkeypatch: pytest.MonkeyPatch) -> None:
        def refuse(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("JSON text was decoded by Python per row")

        monkeypatch.setattr(pl.Expr, "map_elements", refuse)
        if isinstance(engine, _SparkEngine):
            from pyspark.sql import functions

            monkeypatch.setattr(functions, "udf", refuse)

    def test_json_text_is_decoded_to_the_model_struct_and_list(self, engine: _Engine) -> None:
        spec = AppendSpec(table_ref=TableRef("entries"), model=_Entry)

        engine.writer.write(_entry_text(engine), spec, None)

        (row,) = engine.stored("entries")
        assert (row["profile"]["street"], row["visit"]["zipCode"], row["tags"]) == (
            "Main",
            "28001",
            ["a", "b"],
        )

    def test_malformed_json_text_fails_naming_the_target_column_and_type(
        self, engine: _Engine
    ) -> None:
        reader = _TextReader(engine, _entry_text(engine, profile="{broken"))

        with pytest.raises(InvalidValue, match=r"target entries\.profile as StructType"):
            ETLExecutor(reader, engine.writer).run_step(
                ETLCompiler().compile_step(_WriteEntries), NoParams()
            )


class TestProjectedModelSource:
    def test_a_projection_casts_only_the_columns_it_selects(self, engine: _Engine) -> None:
        engine.writer.write(engine.frame({"order_id": ["o1"]}), _spec(AppendSpec), None)

        assert engine.read_columns(FromTable(Order).columns("order_id")) == ["order_id"]


class TestPolarsDeleteFlag:
    def test_the_delete_flag_survives_until_used_and_is_not_written(self, tmp_path: Path) -> None:
        engine = _PolarsEngine(tmp_path)
        spec = _spec(UpsertSpec, upsert_keys=("order_id",), delete_flag="is_void")

        engine.writer.write(_orders(engine, "o1", "o2", void=()), spec, None)
        engine.writer.write(_orders(engine, "o1", "o2", void=("o1",)), spec, None)

        (stored,) = engine.stored("sales.order")
        assert stored["order_id"] == "o2"
        assert "is_void" not in stored


class TestLakeTypeRoundTrip:
    @pytest.mark.parametrize(("annotation", "expected"), LAKE_ROWS.values(), ids=list(LAKE_ROWS))
    def test_the_delta_table_reads_back_with_the_lake_type(
        self, engine: _Engine, annotation: Any, expected: FrameType
    ) -> None:
        spec = AppendSpec(table_ref=TableRef("probes"), model=probe_model(annotation))

        engine.writer.write(engine.frame({"unrelated": [1]}), spec, None)

        assert engine.stored_types("probes") == {"value": expected}

    @pytest.mark.parametrize(("annotation", "expected"), LAKE_ROWS.values(), ids=list(LAKE_ROWS))
    def test_spark_reads_a_polars_table_with_the_same_types(
        self, spark: Any, tmp_path: Path, annotation: Any, expected: FrameType
    ) -> None:
        spec = AppendSpec(table_ref=TableRef("probes"), model=probe_model(annotation))
        written = _PolarsEngine(tmp_path)

        written.writer.write(written.frame({"unrelated": [1]}), spec, None)

        assert _SparkEngine(spark, tmp_path).stored_types("probes") == {"value": expected}


class TestPolarsAuditColumns:
    @pytest.mark.parametrize(
        "write_ctx",
        [
            WriteContext(run_id="r1", step="Load", attempt=1, process_run_id="p1"),
            WriteContext(run_id="r1", step="Load", attempt=2, process_run_id=None),
        ],
        ids=["in-a-process", "outside-a-process"],
    )
    def test_a_model_target_keeps_the_audit_columns_a_string_target_keeps(
        self, tmp_path: Path, write_ctx: WriteContext
    ) -> None:
        audit = AuditConfig(enabled=True, custom=(CustomColumnDef(name="region", value="eu"),))
        engine = _PolarsEngine(tmp_path, audit)

        engine.writer.write(_orders(engine, "o1"), _spec(AppendSpec), None, write_ctx=write_ctx)
        engine.writer.write(
            _orders(engine, "o1"),
            AppendSpec(table_ref=TableRef("sales.plain")),
            None,
            write_ctx=write_ctx,
        )

        stamped = {
            name: dtype
            for name, dtype in engine.stored_types("sales.plain").items()
            if name not in (*_ORDER.column_names, "noise")
        }
        assert stamped.keys() >= {"_loom_run_id", "_loom_step", "_loom_attempt", "region"}
        assert engine.stored_types("sales.order") == {**_ORDER.column_types, **stamped}


class TestDottedTableName:
    def test_a_dotted_model_is_a_lake_table_and_no_sql_table(self, tmp_path: Path) -> None:
        engine = _PolarsEngine(tmp_path)

        engine.writer.write(_orders(engine, "o1"), _spec(AppendSpec), None)

        assert [row["order_id"] for row in engine.stored("sales.order")] == ["o1"]
        with pytest.raises(InvalidValue, match=r"Order.*'sales\.order' is dotted.*storage\.tables"):
            compile_model(Order, metadata=MetaData())


class TestModelWriteObservability:
    def test_a_write_logs_the_model_columns_once(
        self, engine: _Engine, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            engine.writer.write(_orders(engine, "o1"), _spec(AppendSpec), None)

        (fields,) = CapturedLog(caplog).named(EtlEvent.MODEL_SCHEMA_APPLIED)
        assert fields[EtlKey.MODEL] == "Order"
        assert fields[EtlKey.TABLE] == "sales.order"
        assert fields[EtlKey.COLUMNS] == list(_ORDER.column_names)

    def test_a_write_resolves_no_model(
        self, engine: _Engine, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resolutions: list[type] = []
        schema_of = LakeTranslator.schema_of

        def counting(self: LakeTranslator, contract: type) -> Any:
            resolutions.append(contract)
            return schema_of(self, contract)

        monkeypatch.setattr(LakeTranslator, "schema_of", counting)
        spec = _spec(AppendSpec)

        for _ in range(3):
            engine.writer.write(_orders(engine, "o1"), spec, None)

        assert resolutions == []


class TestModelWriteRunsNoSparkAction:
    @pytest.fixture
    def actions_refused(self, spark: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
        from pyspark.sql import DataFrame

        def refuse(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("a model write ran a Spark action")

        for action in _SPARK_ACTIONS:
            monkeypatch.setattr(DataFrame, action, refuse)
        yield

    def test_create_replace_and_upsert_run_no_action(
        self, spark: Any, tmp_path: Path, actions_refused: None
    ) -> None:
        engine = _SparkEngine(spark, tmp_path)
        frame = _orders(engine, "o1")

        for factory in (_spec(AppendSpec), _spec(ReplaceSpec), _ORDER_WRITES["upsert"][0]()):
            engine.writer.write(frame, factory, None)
