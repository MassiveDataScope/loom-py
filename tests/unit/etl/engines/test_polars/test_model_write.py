"""A write declared with a model aligns and creates to the model's lake schema."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest

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
from loom.etl.engines.polars import (
    PolarsTargetWriter,
    frame_type_to_polars,
    polars_to_frame_type,
)
from loom.etl.engines.polars._schema import read_delta_physical_schema
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.schema import ModelTable
from loom.etl.schema._model import LakeTranslator
from loom.etl.storage._config import MissingTablePolicy
from tests.helpers.loggers import CapturedLog
from tests.unit.etl.engines.doubles import RunDateParams
from tests.unit.etl.engines.frame_types import (
    LAKE_FRAME_TYPES,
    Order,
    Rate,
    table_of_one_column,
)

from .conftest import table_path

_ORDER = ModelTable.from_model(Order)
_RATE = ModelTable.from_model(Rate)
_ORDER_SCHEMA = {
    "order_id": pl.String,
    "customer_id": pl.String,
    "amount": pl.Decimal(12, 2),
    "placed_at": pl.Datetime("us", "UTC"),
}
_PLACED = datetime(2026, 1, 5, tzinfo=UTC)


def _spec(factory: Callable[..., TableSpec], ref: str = "sales.order", **fields: Any) -> TableSpec:
    return factory(table_ref=TableRef(ref), model_table=_ORDER, **fields)


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


def _orders(*order_ids: str) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "order_id": list(order_ids),
            "customer_id": ["c1"] * len(order_ids),
            "amount": [1.5] * len(order_ids),
            "placed_at": [_PLACED] * len(order_ids),
            "noise": [0] * len(order_ids),
        },
        schema_overrides={"placed_at": pl.Datetime("ms", "UTC")},
    )


def _stored(root: Path, ref: str = "sales.order") -> pl.DataFrame:
    return pl.read_delta(str(table_path(root, TableRef(ref))))


def _physical(root: Path, ref: str = "sales.order") -> pl.Schema:
    physical = read_delta_physical_schema(str(table_path(root, TableRef(ref))))
    assert physical is not None
    return physical.schema


@pytest.fixture
def writer(tmp_path: Path) -> PolarsTargetWriter:
    return PolarsTargetWriter(str(tmp_path), missing_table_policy=MissingTablePolicy.CREATE)


class TestModelAlignTarget:
    @pytest.mark.parametrize("path", _ORDER_WRITES)
    def test_every_path_writes_the_model_types_on_create_and_on_an_existing_table(
        self, writer: PolarsTargetWriter, tmp_path: Path, path: str
    ) -> None:
        spec_factory, creating_factory = _ORDER_WRITES[path]
        creating = (creating_factory or spec_factory)()

        writer.write(_orders("o1"), creating, None)
        writer.write(_orders("o1"), spec_factory(), None)

        assert _physical(tmp_path) == pl.Schema(_ORDER_SCHEMA)

    def test_a_column_the_frame_lacks_is_null_filled_and_an_extra_one_dropped(
        self, writer: PolarsTargetWriter, tmp_path: Path
    ) -> None:
        frame = _orders("o1").drop("customer_id")

        writer.write(frame, _spec(AppendSpec), None)

        stored = _stored(tmp_path)
        assert set(stored.columns) == set(_ORDER_SCHEMA)
        assert stored["customer_id"].to_list() == [None]

    def test_a_second_upsert_updates_without_duplicates(
        self, writer: PolarsTargetWriter, tmp_path: Path
    ) -> None:
        spec = _spec(UpsertSpec, upsert_keys=("order_id",))

        writer.write(_orders("o1", "o2"), spec, None)
        writer.write(_orders("o2", "o3"), spec, None)

        assert sorted(_stored(tmp_path)["order_id"].to_list()) == ["o1", "o2", "o3"]

    def test_the_delete_flag_survives_until_used_and_is_not_written(
        self, writer: PolarsTargetWriter, tmp_path: Path
    ) -> None:
        spec = _spec(UpsertSpec, upsert_keys=("order_id",), delete_flag="is_void")
        flagged = _orders("o1", "o2").with_columns(is_void=pl.col("order_id") == "o1")

        writer.write(_orders("o1", "o2").with_columns(is_void=pl.lit(False)), spec, None)
        writer.write(flagged, spec, None)

        stored = _stored(tmp_path)
        assert stored["order_id"].to_list() == ["o2"]
        assert "is_void" not in stored.columns

    def test_a_historify_result_is_cast_to_the_model(
        self, writer: PolarsTargetWriter, tmp_path: Path
    ) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("sales.rate"),
            keys=("rate_id",),
            effective_date=p.run_date,
            mode=HistorifyInputMode.SNAPSHOT,
            track=("price",),
            model_table=_RATE,
        )
        snapshot = pl.LazyFrame({"rate_id": ["r1"], "price": [2.5]})

        writer.write(snapshot, spec, RunDateParams(date(2026, 1, 5)))

        assert _physical(tmp_path, "sales.rate") == pl.Schema(
            {
                "rate_id": pl.String,
                "price": pl.Decimal(12, 2),
                "valid_from": pl.Date,
                "valid_to": pl.Date,
                "deleted_at": pl.Datetime("us", "UTC"),
            }
        )
        assert _stored(tmp_path, "sales.rate")["price"].to_list() == [Decimal("2.50")]


class TestLakeTypeRoundTrip:
    @pytest.mark.parametrize("frame_type", LAKE_FRAME_TYPES.values(), ids=LAKE_FRAME_TYPES)
    def test_the_delta_table_reads_back_with_the_lake_type(
        self, tmp_path: Path, frame_type: Any
    ) -> None:
        table = table_of_one_column(frame_type)
        spec = AppendSpec(table_ref=TableRef("sales.value"), model_table=table)
        writer = PolarsTargetWriter(str(tmp_path), missing_table_policy=MissingTablePolicy.CREATE)

        writer.write(pl.LazyFrame({"id": [1]}, schema={"id": pl.Int32}), spec, None)

        physical = _physical(tmp_path, "sales.value")
        assert physical["id"] == pl.Int64
        assert physical["value"] == frame_type_to_polars(frame_type)
        assert polars_to_frame_type(physical["value"]) == frame_type


class TestModelWriteObservability:
    def test_a_write_logs_the_model_columns_once(
        self, writer: PolarsTargetWriter, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO):
            writer.write(_orders("o1"), _spec(AppendSpec), None)

        (fields,) = CapturedLog(caplog).named(EtlEvent.MODEL_SCHEMA_APPLIED)
        assert fields[EtlKey.MODEL] == "Order"
        assert fields[EtlKey.TABLE] == "sales.order"
        assert fields[EtlKey.COLUMNS] == list(_ORDER_SCHEMA)

    def test_a_write_resolves_no_model(
        self, writer: PolarsTargetWriter, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resolutions: list[type] = []
        schema_of = LakeTranslator.schema_of

        def counting(self: LakeTranslator, contract: type) -> Any:
            resolutions.append(contract)
            return schema_of(self, contract)

        monkeypatch.setattr(LakeTranslator, "schema_of", counting)
        spec = _spec(AppendSpec)

        for _ in range(3):
            writer.write(_orders("o1"), spec, None)

        assert resolutions == []
