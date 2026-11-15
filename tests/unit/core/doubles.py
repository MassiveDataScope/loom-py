"""Doubles shared by more than one core unit-test package."""

from __future__ import annotations

from loom.core.model.audited import Audited
from loom.core.model.base import BaseModel
from loom.core.model.field import ColumnField
from loom.core.model.privilege import Privilege
from loom.core.model.types import Integer, Text


class AcmeClock:
    pass


class IOrderRepo:
    pass


class FakeOrderRepo(IOrderRepo):
    pass


class AcmeNote:
    """Empty stand-in shared by introspection and projection descriptor tests."""


class Catalog(BaseModel):
    __tablename__ = "catalog"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(Integer, primary_key=True)


class Note(BaseModel, Audited):
    __tablename__ = "notes"
    id: int = ColumnField(Integer, primary_key=True)
    body: str = ColumnField(Text)


class Plain(BaseModel):
    __tablename__ = "plain"
    id: int = ColumnField(Integer, primary_key=True)
