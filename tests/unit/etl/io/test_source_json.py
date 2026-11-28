"""Unit tests for parse_json and with_schema on FromTable / FromFile."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

import msgspec
import pytest

from loom.etl.declarative._format import Format
from loom.etl.declarative.source import FromFile, FromTable, JsonColumnSpec, SourceSpec
from loom.etl.schema._schema import ColumnSchema, ListType, LoomDtype, StructField, StructType


class OrderPayload(msgspec.Struct):
    order_id: int
    amount: float


class EventPayload(msgspec.Struct):
    kind: str
    user_id: int


@dataclasses.dataclass
class CsvRow:
    id: int
    label: str


ORDER_STRUCT = StructType(
    fields=(
        StructField("order_id", LoomDtype.INT64),
        StructField("amount", LoomDtype.FLOAT64),
    )
)
EVENT_STRUCT_LIST = ListType(
    inner=StructType(
        fields=(
            StructField("kind", LoomDtype.UTF8),
            StructField("user_id", LoomDtype.INT64),
        )
    )
)


class TestParseJsonContracts:
    @pytest.mark.parametrize(
        "source_factory,expected",
        [
            (
                lambda: FromTable("raw.events").parse_json("payload", OrderPayload),
                ORDER_STRUCT,
            ),
            (
                lambda: FromTable("raw.t").parse_json("tags", list[str]),
                ListType(inner=LoomDtype.UTF8),
            ),
            (
                lambda: FromTable("raw.t").parse_json("events", list[EventPayload]),
                EVENT_STRUCT_LIST,
            ),
            (
                lambda: FromFile("s3://raw/events.json", format=Format.JSON).parse_json(
                    "payload", OrderPayload
                ),
                ORDER_STRUCT,
            ),
        ],
    )
    def test_parse_json_resolves_expected_frame_type(
        self,
        source_factory: Callable[[], FromTable | FromFile],
        expected: StructType | ListType,
    ) -> None:
        spec = source_factory()._to_spec("events")
        assert len(spec.json_columns) == 1
        assert spec.json_columns[0].frame_type == expected

    def test_from_table_parse_json_with_frame_type_passthrough(self) -> None:
        frame_type = StructType(fields=(StructField("x", LoomDtype.INT64),))
        spec = FromTable("raw.t").parse_json("col", frame_type)._to_spec("t")
        assert spec.json_columns[0].frame_type is frame_type


class TestParseJsonState:
    @pytest.mark.parametrize(
        "base,extended",
        [
            (
                lambda: FromTable("raw.events"),
                lambda: FromTable("raw.events").parse_json("payload", OrderPayload),
            ),
            (
                lambda: FromFile("s3://raw/e.csv", format=Format.CSV),
                lambda: FromFile("s3://raw/e.csv", format=Format.CSV).parse_json(
                    "payload", OrderPayload
                ),
            ),
        ],
    )
    def test_parse_json_is_immutable(
        self,
        base: Callable[[], FromTable | FromFile],
        extended: Callable[[], FromTable | FromFile],
    ) -> None:
        assert base()._to_spec("x").json_columns == ()
        assert len(extended()._to_spec("x").json_columns) == 1

    @pytest.mark.parametrize(
        "source_factory,expected_columns",
        [
            (
                lambda: (
                    FromTable("raw.events")
                    .parse_json("payload", OrderPayload)
                    .parse_json("tags", list[str])
                ),
                ("payload", "tags"),
            ),
            (
                lambda: (
                    FromFile("s3://raw/e.json", format=Format.JSON)
                    .parse_json("payload", OrderPayload)
                    .parse_json("extra", list[int])
                ),
                ("payload", "extra"),
            ),
        ],
    )
    def test_parse_json_accumulates_columns(
        self,
        source_factory: Callable[[], FromTable | FromFile],
        expected_columns: tuple[str, str],
    ) -> None:
        spec = source_factory()._to_spec("events")
        assert len(spec.json_columns) == 2
        assert spec.json_columns[0].column == expected_columns[0]
        assert spec.json_columns[1].column == expected_columns[1]

    def test_from_table_parse_json_preserves_other_state(self) -> None:
        from loom.etl.declarative.expr._refs import col

        spec = (
            FromTable("raw.events")
            .where(col("year") == 2024)
            .columns("id", "payload")
            .parse_json("payload", OrderPayload)
            ._to_spec("events")
        )

        assert len(spec.predicates) == 1
        assert spec.columns == ("id", "payload")
        assert len(spec.json_columns) == 1

    def test_from_file_parse_json_preserves_other_state(self) -> None:
        from loom.etl.declarative._read_options import JsonReadOptions

        spec = (
            FromFile("s3://raw/e.json", format=Format.JSON)
            .with_options(JsonReadOptions(infer_schema_length=None))
            .parse_json("payload", OrderPayload)
            ._to_spec("e")
        )
        assert spec.read_options is not None
        assert len(spec.json_columns) == 1


class TestWithSchema:
    @pytest.mark.parametrize(
        "source_factory,schema,expected",
        [
            (
                lambda: FromTable("raw.orders"),
                OrderPayload,
                (
                    ColumnSchema("order_id", LoomDtype.INT64),
                    ColumnSchema("amount", LoomDtype.FLOAT64),
                ),
            ),
            (
                lambda: FromTable("raw.t"),
                CsvRow,
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("label", LoomDtype.UTF8),
                ),
            ),
            (
                lambda: FromFile("s3://raw/t.csv", format=Format.CSV),
                CsvRow,
                (
                    ColumnSchema("id", LoomDtype.INT64),
                    ColumnSchema("label", LoomDtype.UTF8),
                ),
            ),
            (
                lambda: FromTable("raw.t"),
                (ColumnSchema("x", LoomDtype.INT32),),
                (ColumnSchema("x", LoomDtype.INT32),),
            ),
        ],
    )
    def test_with_schema_accepts_contract_and_tuple(
        self,
        source_factory: Callable[[], FromTable | FromFile],
        schema: tuple[ColumnSchema, ...] | type[Any],
        expected: tuple[ColumnSchema, ...],
    ) -> None:
        source = source_factory()
        spec: SourceSpec = source.with_schema(schema)._to_spec("t")
        assert spec.schema == expected

    def test_with_schema_and_parse_json_combined(self) -> None:
        spec = (
            FromTable("raw.events")
            .with_schema(CsvRow)
            .parse_json("payload", OrderPayload)
            ._to_spec("events")
        )
        assert len(spec.schema) == 2
        assert len(spec.json_columns) == 1


class TestJsonColumnSpec:
    def test_json_column_spec_is_frozen(self) -> None:
        spec = JsonColumnSpec(column="col", frame_type=LoomDtype.UTF8)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            spec.column = "other"  # type: ignore[misc]

    def test_json_column_spec_equality(self) -> None:
        a = JsonColumnSpec(column="x", frame_type=LoomDtype.INT64)
        b = JsonColumnSpec(column="x", frame_type=LoomDtype.INT64)
        assert a == b
