"""Every frame type the lake table produces, and the models the engine tests share."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

import msgspec

from loom.core.model import BaseModel, Field
from loom.core.model.types import (
    BigInteger,
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
from loom.etl.schema import (
    DatetimeType,
    DecimalType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)


class Buyer(msgspec.Struct):
    name: str
    order_count: int


class Priced(msgspec.Struct):
    price: Decimal


class Order(BaseModel):
    __tablename__ = "sales.order"
    order_id: Annotated[str, Field(primary_key=True)]
    customer_id: Annotated[str, Field(index=True)]
    amount: Annotated[Decimal, Numeric(precision=12, scale=2)]
    placed_at: Annotated[datetime, DateTime(tz=True)]


class Rate(BaseModel):
    __tablename__ = "sales.rate"
    rate_id: Annotated[str, Field(primary_key=True)]
    price: Annotated[Decimal, Numeric(precision=12, scale=2)]
    valid_from: date
    valid_to: date | None
    deleted_at: Annotated[datetime, DateTime(tz=True)] | None


class SubEntity(msgspec.Struct):
    street: str
    zip_code: str | None = None


class CamelEntity(msgspec.Struct, rename="camel"):
    zip_code: str
    seen_at: datetime


def probe_model(annotation: Any) -> type[BaseModel]:
    """A model of one table whose only field, ``value``, is annotated with *annotation*."""
    return type(
        "Probe",
        (BaseModel,),
        {"__tablename__": "probes", "__annotations__": {"value": annotation}},
    )


_STREET = StructField("street", LoomDtype.UTF8)
_ZIP = StructField("zip_code", LoomDtype.UTF8)
LAKE_ROWS = {
    "string": (Annotated[str, String(20)], LoomDtype.UTF8),
    "text": (Annotated[str, Text], LoomDtype.UTF8),
    "json_str": (JsonStr, LoomDtype.UTF8),
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
    "datetime_tz": (datetime, DatetimeType("us", "UTC")),
    "datetime_naive": (Annotated[datetime, DateTime(tz=False)], DatetimeType("us", None)),
    "numeric_scale_0": (Annotated[Decimal, Numeric(precision=12)], DecimalType(12)),
    "numeric": (Annotated[Decimal, Numeric(precision=38, scale=4)], DecimalType(38, 4)),
    "json_opaque": (dict[str, Any], LoomDtype.UTF8),
    "json_struct": (SubEntity, StructType((_STREET, _ZIP))),
    "json_struct_camel": (
        CamelEntity,
        StructType(
            (
                StructField("zipCode", LoomDtype.UTF8),
                StructField("seenAt", DatetimeType("us", "UTC")),
            )
        ),
    ),
    "json_list_str": (list[str], ListType(LoomDtype.UTF8)),
    "json_list_struct": (list[SubEntity], ListType(StructType((_STREET, _ZIP)))),
    "jsonb": (Annotated[dict[str, Any], Postgres.JSONB], LoomDtype.UTF8),
    "postgres_uuid": (Annotated[UUID, Postgres.UUID], LoomDtype.UTF8),
    "postgres_inet": (Annotated[str, Postgres.INET], LoomDtype.UTF8),
    "postgres_array": (Annotated[list[str], Postgres.ARRAY(String())], ListType(LoomDtype.UTF8)),
}
LAKE_TYPES = {name: frame_type for name, (_, frame_type) in LAKE_ROWS.items()}
LAKE_FRAME_TYPES = {
    **LAKE_TYPES,
    "struct_nested_decimal": StructType(
        (
            StructField("id", LoomDtype.INT64),
            StructField("tags", ListType(DecimalType(18, 2))),
        )
    ),
}


def lake_model() -> type[BaseModel]:
    """A model with one column per lake row."""
    annotations = {name: annotation for name, (annotation, _) in LAKE_ROWS.items()}
    return type("Lake", (BaseModel,), {"__tablename__": "lake", "__annotations__": annotations})
