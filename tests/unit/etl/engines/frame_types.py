"""Every frame type the lake table produces, and the models the engine write tests share."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from loom.core.model import BaseModel, Field
from loom.core.model.types import DateTime, Numeric
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

_SCALARS = (
    LoomDtype.INT8,
    LoomDtype.INT16,
    LoomDtype.INT32,
    LoomDtype.INT64,
    LoomDtype.FLOAT64,
    LoomDtype.BOOLEAN,
    LoomDtype.UTF8,
    LoomDtype.BINARY,
    LoomDtype.DATE,
)
LAKE_FRAME_TYPES = {
    **{str(scalar): scalar for scalar in _SCALARS},
    "decimal-scale-0": DecimalType(10),
    "decimal": DecimalType(38, 4),
    "datetime-utc": DatetimeType("us", "UTC"),
    "datetime-naive": DatetimeType("us", None),
    "list": ListType(LoomDtype.UTF8),
    "struct": StructType(
        (
            StructField("id", LoomDtype.INT64),
            StructField("seen", DatetimeType("us", None)),
            StructField("tags", ListType(DecimalType(18, 2))),
        )
    ),
}


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


def table_of_one_column(frame_type: FrameType) -> ModelTable:
    """A lake table of an ``id`` and one ``value`` column of *frame_type*."""
    return ModelTable(
        Order,
        "sales.order",
        (ColumnSchema("id", LoomDtype.INT64), ColumnSchema("value", frame_type)),
    )
