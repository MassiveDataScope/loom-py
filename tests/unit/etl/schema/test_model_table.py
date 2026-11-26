"""The lake table of a model: what each column kind becomes, and what the lake refuses."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import msgspec
import polars as pl
import pytest

from loom.core.errors import InvalidType, InvalidValue
from loom.core.model import BaseModel
from loom.core.model.types import (
    BigInteger,
    ColumnKind,
    DateTime,
    Float,
    Integer,
    JsonStr,
    Numeric,
    Postgres,
    SmallInteger,
    String,
    Text,
    TinyInteger,
)
from loom.etl.engines.polars import frame_type_to_polars, polars_to_frame_type
from loom.etl.schema import (
    ColumnSchema,
    DatetimeType,
    DecimalType,
    FrameType,
    ListType,
    LoomDtype,
    ModelTable,
    StructField,
    StructType,
)
from loom.etl.schema._model import _RULES


class SubEntity(msgspec.Struct):
    street: str
    zip_code: str | None = None


class CamelEntity(msgspec.Struct, rename="camel"):
    zip_code: str
    seen_at: datetime


class PricedEntity(msgspec.Struct):
    price: Decimal


def _model(annotation: Any) -> type[BaseModel]:
    return type(
        "Probe",
        (BaseModel,),
        {"__tablename__": "probes", "__annotations__": {"value": annotation}},
    )


_STREET = StructField("street", LoomDtype.UTF8)
_ZIP = StructField("zip_code", LoomDtype.UTF8)
_LAKE_ROWS = {
    "string": (Annotated[str, String(20)], LoomDtype.UTF8),
    "text": (Annotated[str, Text], LoomDtype.UTF8),
    "json-str": (JsonStr, LoomDtype.UTF8),
    "uuid": (UUID, LoomDtype.UTF8),
    "tiny": (Annotated[int, TinyInteger], LoomDtype.INT8),
    "small": (Annotated[int, SmallInteger], LoomDtype.INT16),
    "integer": (Annotated[int, Integer], LoomDtype.INT32),
    "int": (int, LoomDtype.INT64),
    "bigint": (Annotated[int, BigInteger], LoomDtype.INT64),
    "float": (Annotated[float, Float], LoomDtype.FLOAT64),
    "bool": (bool, LoomDtype.BOOLEAN),
    "bytes": (bytes, LoomDtype.BINARY),
    "date": (date, LoomDtype.DATE),
    "datetime-tz": (datetime, DatetimeType("us", "UTC")),
    "datetime-naive": (Annotated[datetime, DateTime(tz=False)], DatetimeType("us", None)),
    "numeric-scale-0": (Annotated[Decimal, Numeric(precision=12)], DecimalType(12)),
    "numeric": (Annotated[Decimal, Numeric(precision=38, scale=4)], DecimalType(38, 4)),
    "json-opaque": (dict[str, Any], LoomDtype.UTF8),
    "json-struct": (SubEntity, StructType((_STREET, _ZIP))),
    "json-struct-camel": (
        CamelEntity,
        StructType(
            (
                StructField("zipCode", LoomDtype.UTF8),
                StructField("seenAt", DatetimeType("us", "UTC")),
            )
        ),
    ),
    "json-list-str": (list[str], ListType(LoomDtype.UTF8)),
    "json-list-struct": (list[SubEntity], ListType(StructType((_STREET, _ZIP)))),
    "jsonb": (Annotated[dict[str, Any], Postgres.JSONB], LoomDtype.UTF8),
    "postgres-uuid": (Annotated[UUID, Postgres.UUID], LoomDtype.UTF8),
    "postgres-inet": (Annotated[str, Postgres.INET], LoomDtype.UTF8),
    "postgres-array": (Annotated[list[str], Postgres.ARRAY(String())], ListType(LoomDtype.UTF8)),
}
_REFUSALS = {
    "unbounded-numeric": (Annotated[Decimal, Numeric()], "unbounded Numeric"),
    "bare-decimal": (Decimal, "unbounded Numeric"),
    "wide-numeric": (Annotated[Decimal, Numeric(precision=39)], "precision 39"),
    "time": (time, "Time"),
    "tsvector": (Annotated[str, Postgres.TSVECTOR], "tsvector"),
    "undeclared": (Any, "no column type"),
    "sub-entity-decimal": (PricedEntity, r"Probe\.value\.price .*unbounded Numeric"),
    "list-of-sub-entity-decimal": (list[PricedEntity], r"Probe\.value\.item\.price"),
}


class TestLakeTable:
    @pytest.mark.parametrize(("annotation", "expected"), _LAKE_ROWS.values(), ids=list(_LAKE_ROWS))
    def test_a_column_becomes_its_lake_type(self, annotation: Any, expected: FrameType) -> None:
        table = ModelTable.from_model(_model(annotation))

        assert table.schema == (ColumnSchema("value", expected),)

    @pytest.mark.parametrize(("annotation", "expected"), _LAKE_ROWS.values(), ids=list(_LAKE_ROWS))
    def test_a_delta_table_reads_back_with_the_type_of_the_row(
        self, tmp_path: Path, annotation: Any, expected: FrameType
    ) -> None:
        dtype = frame_type_to_polars(ModelTable.from_model(_model(annotation)).schema[0].dtype)
        pl.DataFrame(schema={"value": dtype}).write_delta(str(tmp_path))

        assert polars_to_frame_type(pl.read_delta(str(tmp_path)).schema["value"]) == expected

    @pytest.mark.parametrize(("annotation", "message"), _REFUSALS.values(), ids=list(_REFUSALS))
    def test_a_column_the_lake_cannot_hold_is_refused_naming_the_field(
        self, annotation: Any, message: str
    ) -> None:
        with pytest.raises(InvalidValue, match=message):
            ModelTable.from_model(_model(annotation))

    def test_every_column_kind_has_a_rule_or_a_named_refusal(self) -> None:
        assert set(_RULES) == set(ColumnKind)

    def test_the_table_carries_the_model_and_its_logical_name(self) -> None:
        model = _model(int)

        table = ModelTable.from_model(model)

        assert (table.model, table.name) == (model, "probes")

    def test_the_table_is_translated_once_per_model(self) -> None:
        model = _model(int)

        assert ModelTable.from_model(model) is ModelTable.from_model(model)

    def test_a_model_without_a_table_name_is_refused(self) -> None:
        with pytest.raises(InvalidType, match="__tablename__"):
            ModelTable.from_model(type("Nameless", (BaseModel,), {"__annotations__": {"id": int}}))
