"""Synthetic product 2: rows scoped by an integer region, read-only, plus a global table."""

from __future__ import annotations

from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped, ScopedField
from loom.core.model.types import Integer, Numeric, Text

SCHEMA = "sites"
GROUPS = ("sites_readers", "sites_writers")
USERS = {"sites_rw": "write", "sites_ops": "bypass"}
SCOPE_BINDINGS = {"region": "identity.region"}


class SiteReading(BaseModel, RowScoped):
    __tablename__ = "site_readings"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    region: int = ScopedField(Integer, primary_key=True, scope="region")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    value: float = ColumnField(Numeric())


class SiteKind(BaseModel):
    __tablename__ = "site_kinds"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(Integer, primary_key=True)
    name: str = ColumnField(Text)


MODELS = (SiteReading, SiteKind)
