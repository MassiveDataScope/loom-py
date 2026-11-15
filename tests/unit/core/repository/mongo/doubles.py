"""Doubles shared by the Mongo repository unit tests."""

from __future__ import annotations

from loom.core.model import BaseModel, ColumnField


class Counter(BaseModel):
    __tablename__ = "counters"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    value: int = ColumnField()
