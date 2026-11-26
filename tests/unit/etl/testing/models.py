"""Models, and the Outcome step of spec 023, for the runner tests written from models."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

import msgspec
import polars as pl

from loom.core.model import BaseModel, Field
from loom.core.model.types import Numeric
from loom.etl import ETLParams, ETLStep, FromTable, IntoTable, col, params
from tests.unit.etl.engines.frame_types import Order


class CustomerTotal(BaseModel):
    __tablename__ = "sales.customer_total"
    customer_id: Annotated[str, Field(primary_key=True)]
    spent: Annotated[Decimal, Numeric(precision=14, scale=2)]
    orders: int


class Tier(StrEnum):
    GOLD = "gold"


class Stop(msgspec.Struct, rename="camel"):
    zip_code: str
    seen_at: datetime


class Visitor(BaseModel):
    __tablename__ = "sales.visitor"
    visitor_id: Annotated[UUID, Field(primary_key=True)]
    tier: Tier
    home: Stop
    stops: list[Stop]
    extras: dict[str, Any]
    note: str | None


class Since(ETLParams):  # type: ignore[misc]
    since: datetime


class TotalCustomers(ETLStep[Since]):
    orders = FromTable(Order).where(col(Order.c.placed_at) >= params.since)
    target = IntoTable(CustomerTotal).upsert()

    def execute(self, params: Since, *, orders: Any) -> Any:  # type: ignore[override]
        return orders.group_by(Order.c.customer_id).agg(
            pl.col(Order.c.amount).sum().alias(CustomerTotal.c.spent),
            pl.len().cast(pl.Int32).alias(CustomerTotal.c.orders),
        )
