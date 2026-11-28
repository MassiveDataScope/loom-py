from __future__ import annotations

from loom.core.model import BaseModel, ColumnField
from loom.core.model.types import Integer, Text


class Widget(BaseModel):
    __tablename__ = "widgets"
    id: int = ColumnField(Integer, primary_key=True)
    name: str = ColumnField(Text)


class Gadget(BaseModel):
    __tablename__ = "gadgets"
    __store__ = "audit"
    id: int = ColumnField(Integer, primary_key=True)
