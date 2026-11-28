"""A write declared with a model aligns and creates to the model's lake schema, on every engine."""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any

import pytest

from loom.core.errors import InvalidValue
from loom.core.model import BaseModel, Field
from loom.etl import ETLStep, FromTable, IntoHistory, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.expr import params as p
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.executor import ETLExecutor
from loom.etl.lineage._records import WriteContext
from loom.etl.schema import LoomDtype, ModelTable
from loom.etl.storage._config import AuditConfig, CustomColumnDef
from tests.helpers.loggers import CapturedLog
from tests.unit.etl.engines.doubles import (
    ModelEngine,
    NoParams,
    PolarsEngine,
    RunDateParams,
    SparkEngine,
)
from tests.unit.etl.models import (
    LAKE_TYPES,
    CamelEntity,
    Order,
    Rate,
    SubEntity,
    lake_model,
)

_ORDER = ModelTable.from_model(Order)
_RATE = ModelTable.from_model(Rate)
_PLACED = datetime(2026, 1, 5, tzinfo=UTC)
_SPARK_ACTIONS = ("count", "isEmpty", "collect", "first", "take", "toPandas")
_ORDER_ID = (Order.c.order_id,)
_BY_CUSTOMER = (Order.c.customer_id,)


def _orders(engine: ModelEngine, *order_ids: str, void: Collection[str] | None = None) -> Any:
    count = len(order_ids)
    columns: dict[str, list[Any]] = {
        Order.c.order_id: list(order_ids),
        Order.c.customer_id: ["c1"] * count,
        Order.c.amount: [1.5] * count,
        Order.c.placed_at: [_PLACED] * count,
        "noise": [0] * count,
    }
    if void is not None:
        columns["is_void"] = [order_id in void for order_id in order_ids]
    return engine.frame(columns)


_REPLACE = IntoTable(Order).replace()
_REPLACE_PARTITIONS = IntoTable(Order).replace_partitions(*_BY_CUSTOMER)
_REPLACE_PARTITIONS_STREAMING = IntoTable(Order).replace_partitions(*_BY_CUSTOMER, streaming=True)
_ORDER_UPSERT = IntoTable(Order).upsert(keys=_ORDER_ID)
_ORDER_WRITES = {
    "replace": (_REPLACE, _REPLACE),
    "replace-partitions": (_REPLACE_PARTITIONS, _REPLACE_PARTITIONS),
    "replace-partitions-streaming": (_REPLACE_PARTITIONS_STREAMING, _REPLACE_PARTITIONS_STREAMING),
    "upsert": (_ORDER_UPSERT, _ORDER_UPSERT),
    "update": (IntoTable(Order).update(keys=_ORDER_ID), _ORDER_UPSERT),
}


class TestModelAlignTarget:
    @pytest.mark.parametrize(("target", "seeding"), _ORDER_WRITES.values(), ids=list(_ORDER_WRITES))
    def test_every_path_writes_the_model_types_on_create_and_on_an_existing_table(
        self, polars_engine: PolarsEngine, target: IntoTable, seeding: IntoTable
    ) -> None:
        polars_engine.writer.write(_orders(polars_engine, "o1"), seeding._to_spec(), None)
        polars_engine.writer.write(_orders(polars_engine, "o1"), target._to_spec(), None)

        assert polars_engine.stored_types("sales.order") == _ORDER.column_types

    def test_a_historify_result_is_cast_to_the_model(self, engine: ModelEngine) -> None:
        target = IntoHistory(Rate, keys=(Rate.c.rate_id,), effective_date=p.run_date)

        engine.writer.write(
            engine.frame({Rate.c.rate_id: ["r1"], Rate.c.price: [2.5]}),
            target._to_spec(),
            RunDateParams(date(2026, 1, 5)),
        )

        assert engine.stored_types("sales.rate") == _RATE.column_types
        assert [row[Rate.c.price] for row in engine.stored("sales.rate")] == [Decimal("2.50")]

    def test_the_delete_flag_survives_until_used_and_is_not_written(
        self, polars_engine: PolarsEngine
    ) -> None:
        target = IntoTable(Order).upsert(keys=_ORDER_ID, delete_flag="is_void")

        polars_engine.writer.write(
            _orders(polars_engine, "o1", "o2", void=()), target._to_spec(), None
        )
        polars_engine.writer.write(
            _orders(polars_engine, "o1", "o2", void=("o1",)), target._to_spec(), None
        )

        (stored,) = polars_engine.stored("sales.order")
        assert stored[Order.c.order_id] == "o2"
        assert "is_void" not in stored

    def test_a_projection_casts_only_the_columns_it_selects(self, engine: ModelEngine) -> None:
        engine.writer.write(
            engine.frame({Order.c.order_id: ["o1"]}), IntoTable(Order).append()._to_spec(), None
        )

        assert engine.read_columns(FromTable(Order).columns(Order.c.order_id)) == [Order.c.order_id]


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


class _PlainReader:
    """A reader that cannot recognise a failed JSON decode."""

    def __init__(self, frame: Any) -> None:
        self._frame = frame

    def read(self, spec: Any, params_instance: Any, /) -> Any:
        return self._frame


def _entry_text(
    engine: ModelEngine,
    profile: str = '{"street": "Main"}',
    visit: str = '{"zipCode": "28001", "seenAt": "2026-01-05T00:00:00Z"}',
) -> Any:
    return engine.frame(
        {
            _Entry.c.entry_id: [1],
            _Entry.c.profile: [profile],
            _Entry.c.visit: [visit],
            _Entry.c.tags: ['["a", "b"]'],
        }
    )


@pytest.mark.usefixtures("no_python_per_row")
class TestJsonTextOnWrite:
    def test_json_text_is_decoded_to_the_model_struct_and_list(self, engine: ModelEngine) -> None:
        engine.writer.write(_entry_text(engine), IntoTable(_Entry).append()._to_spec(), None)

        (row,) = engine.stored("entries")
        assert (row[_Entry.c.profile]["street"], row[_Entry.c.tags]) == ("Main", ["a", "b"])
        assert row[_Entry.c.visit]["zipCode"] == "28001"

    @pytest.mark.usefixtures("session_in_utc")
    def test_json_text_reads_a_timestamp_without_offset_as_utc(self, engine: ModelEngine) -> None:
        visit = '{"zipCode": "28001", "seenAt": "2026-01-05 00:00:00"}'
        engine.writer.write(
            _entry_text(engine, visit=visit), IntoTable(_Entry).append()._to_spec(), None
        )

        (row,) = engine.stored("entries")
        assert row[_Entry.c.visit]["seenAt"].astimezone(UTC) == _PLACED

    def test_malformed_json_text_fails_naming_the_target_column_and_type(
        self, engine: ModelEngine
    ) -> None:
        reader = _PlainReader(_entry_text(engine, profile="{broken"))

        with pytest.raises(InvalidValue, match=r"entries\.profile as StructType"):
            ETLExecutor(reader, engine.writer).run_step(
                ETLCompiler().compile_step(_WriteEntries), NoParams()
            )


class TestLakeTypeRoundTrip:
    def test_the_delta_table_reads_back_with_the_lake_types(self, engine: ModelEngine) -> None:
        engine.writer.write(
            engine.frame({"unrelated": [1]}), IntoTable(lake_model()).append()._to_spec(), None
        )

        assert engine.stored_types("lake") == LAKE_TYPES

    def test_spark_reads_a_polars_table_with_the_same_types(
        self, polars_engine: PolarsEngine, spark_engine: SparkEngine
    ) -> None:
        polars_engine.writer.write(
            polars_engine.frame({"unrelated": [1]}),
            IntoTable(lake_model()).append()._to_spec(),
            None,
        )

        assert spark_engine.stored_types("lake") == LAKE_TYPES


class _Retry(BaseModel):
    __tablename__ = "retry"
    retry_id: Annotated[int, Field(primary_key=True)]
    attempt: int


class TestPolarsAuditColumns:
    def test_a_model_target_keeps_the_audit_columns_a_string_target_keeps(
        self, tmp_path: Path
    ) -> None:
        write_ctx = WriteContext(run_id="r1", step="Load", attempt=1, process_run_id="p1")
        audit = AuditConfig(enabled=True, custom=(CustomColumnDef(name="region", value="eu"),))
        engine = PolarsEngine(tmp_path, audit)

        engine.writer.write(
            _orders(engine, "o1"), IntoTable(Order).append()._to_spec(), None, write_ctx=write_ctx
        )
        engine.writer.write(
            _orders(engine, "o1").drop("noise"),
            IntoTable("sales.plain").append()._to_spec(),
            None,
            write_ctx=write_ctx,
        )

        plain = engine.stored_types("sales.plain")
        assert plain.keys() >= {"_loom_run_id", "_loom_step", "_loom_attempt", "region"}
        assert engine.stored_types("sales.order") == {**plain, **_ORDER.column_types}

    def test_a_model_column_named_like_an_audit_column_keeps_the_model_type(
        self, tmp_path: Path
    ) -> None:
        engine = PolarsEngine(tmp_path, AuditConfig(enabled=True, prefix=""))
        write_ctx = WriteContext(run_id="r1", step="Load", attempt=2)

        engine.writer.write(
            engine.frame({_Retry.c.retry_id: [1], _Retry.c.attempt: [7]}),
            IntoTable(_Retry).append()._to_spec(),
            None,
            write_ctx=write_ctx,
        )

        assert engine.stored_types("retry")[_Retry.c.attempt] == LoomDtype.INT64


_SPARK_TARGETS = {
    "append": IntoTable(Order).append(),
    "replace": _REPLACE,
    "upsert": _ORDER_UPSERT,
}


class TestModelWriteObservability:
    def test_a_write_logs_the_model_columns_once(
        self, engine: ModelEngine, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            engine.writer.write(_orders(engine, "o1"), IntoTable(Order).append()._to_spec(), None)

        (fields,) = CapturedLog(caplog).named(EtlEvent.MODEL_SCHEMA_APPLIED)
        assert fields[EtlKey.MODEL] == "Order"
        assert fields[EtlKey.TABLE] == "sales.order"
        assert fields[EtlKey.COLUMNS] == list(_ORDER.column_names)

    @pytest.fixture
    def actions_refused(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
        from pyspark.sql import DataFrame

        def refuse(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("a model write ran a Spark action")

        for action in _SPARK_ACTIONS:
            monkeypatch.setattr(DataFrame, action, refuse)
        yield

    @pytest.mark.parametrize("target", _SPARK_TARGETS.values(), ids=list(_SPARK_TARGETS))
    def test_a_model_write_runs_no_spark_action(
        self, spark_engine: SparkEngine, actions_refused: None, target: IntoTable
    ) -> None:
        spark_engine.writer.write(_orders(spark_engine, "o1"), target._to_spec(), None)
