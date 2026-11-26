"""The real Spark writer, on a temporary Delta path, writes the model's lake schema."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._table import AppendSpec, ReplaceSpec, UpsertSpec
from loom.etl.engines.polars import PolarsTargetWriter
from loom.etl.engines.spark import SparkTargetWriter
from loom.etl.engines.spark._dtype import frame_type_to_spark, spark_to_frame_type
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.schema import ModelTable
from loom.etl.storage._config import MissingTablePolicy
from tests.helpers.loggers import CapturedLog
from tests.unit.etl.engines.frame_types import LAKE_FRAME_TYPES, Order, table_of_one_column

from .conftest import spark_table_path

_ORDER = ModelTable.from_model(Order)
_ORDER_TYPES = {
    "order_id": T.StringType(),
    "customer_id": T.StringType(),
    "amount": T.DecimalType(12, 2),
    "placed_at": T.TimestampType(),
}
_ACTIONS = ("count", "isEmpty", "collect", "first", "take", "toPandas")


def _orders(spark: SparkSession, *order_ids: str) -> DataFrame:
    placed = datetime(2026, 1, 5, tzinfo=UTC)
    rows = [(order_id, "c1", 1.5, placed, 0) for order_id in order_ids]
    return spark.createDataFrame(
        rows, "order_id string, customer_id string, amount double, placed_at timestamp, noise int"
    )


def _stored_schema(spark: SparkSession, root: Path, ref: str) -> T.StructType:
    path = spark_table_path(root, TableRef(ref))
    return DeltaTable.forPath(spark, str(path)).toDF().schema


@pytest.fixture
def spark_writer_creating(spark: SparkSession, spark_root: Path) -> SparkTargetWriter:
    return SparkTargetWriter(spark, spark_root, missing_table_policy=MissingTablePolicy.CREATE)


@pytest.fixture
def actions_refused(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make every Spark action a log path could feed on raise."""

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a model write ran a Spark action")

    for action in _ACTIONS:
        monkeypatch.setattr(DataFrame, action, refuse)
    yield


class TestModelWriteOnDelta:
    def test_the_table_is_created_and_upserted_with_the_model_types(
        self, spark: SparkSession, spark_root: Path, spark_writer_creating: SparkTargetWriter
    ) -> None:
        spec = UpsertSpec(
            table_ref=TableRef("sales.order"), upsert_keys=("order_id",), model_table=_ORDER
        )

        spark_writer_creating.write(_orders(spark, "o1", "o2"), spec, None)
        spark_writer_creating.write(_orders(spark, "o2", "o3"), spec, None)

        stored = spark.read.format("delta").load(str(spark_table_path(spark_root, spec.table_ref)))
        assert {f.name: f.dataType for f in stored.schema.fields} == _ORDER_TYPES
        assert sorted(row.order_id for row in stored.collect()) == ["o1", "o2", "o3"]

    def test_polars_and_spark_write_the_same_types(
        self, spark: SparkSession, spark_root: Path, tmp_path: Path
    ) -> None:
        import polars as pl

        polars_root = tmp_path / "polars"
        frame = pl.LazyFrame({"order_id": ["o1"], "amount": [1.5]})
        spec = AppendSpec(table_ref=TableRef("sales.order"), model_table=_ORDER)
        PolarsTargetWriter(str(polars_root), missing_table_policy=MissingTablePolicy.CREATE).write(
            frame, spec, None
        )
        SparkTargetWriter(spark, spark_root, missing_table_policy=MissingTablePolicy.CREATE).write(
            _orders(spark, "o1"), spec, None
        )

        for root in (polars_root, spark_root):
            stored = _stored_schema(spark, root, "sales.order")
            assert {f.name: f.dataType for f in stored.fields} == _ORDER_TYPES

    @pytest.mark.parametrize("frame_type", LAKE_FRAME_TYPES.values(), ids=LAKE_FRAME_TYPES)
    def test_the_delta_table_reads_back_with_the_lake_type(
        self,
        spark: SparkSession,
        spark_root: Path,
        spark_writer_creating: SparkTargetWriter,
        frame_type: Any,
    ) -> None:
        spec = AppendSpec(
            table_ref=TableRef("sales.value"), model_table=table_of_one_column(frame_type)
        )

        spark_writer_creating.write(spark.createDataFrame([(1,)], "id int"), spec, None)

        stored = _stored_schema(spark, spark_root, "sales.value")
        assert stored["id"].dataType == T.LongType()
        assert stored["value"].dataType == frame_type_to_spark(frame_type)
        assert spark_to_frame_type(stored["value"].dataType) == frame_type


class TestModelWriteRunsNoSparkAction:
    def test_create_replace_and_upsert_run_no_action(
        self,
        spark: SparkSession,
        spark_writer_creating: SparkTargetWriter,
        actions_refused: None,
    ) -> None:
        ref = TableRef("sales.order")
        frame = _orders(spark, "o1")

        spark_writer_creating.write(frame, AppendSpec(table_ref=ref, model_table=_ORDER), None)
        spark_writer_creating.write(frame, ReplaceSpec(table_ref=ref, model_table=_ORDER), None)
        spark_writer_creating.write(
            frame, UpsertSpec(table_ref=ref, upsert_keys=("order_id",), model_table=_ORDER), None
        )

    def test_the_write_logs_the_model_columns_once(
        self,
        spark: SparkSession,
        spark_writer_creating: SparkTargetWriter,
        actions_refused: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        spec = AppendSpec(table_ref=TableRef("sales.order"), model_table=_ORDER)

        with caplog.at_level(logging.INFO):
            spark_writer_creating.write(_orders(spark, "o1"), spec, None)

        (fields,) = CapturedLog(caplog).named(EtlEvent.MODEL_SCHEMA_APPLIED)
        assert fields[EtlKey.COLUMNS] == list(_ORDER.column_names)
