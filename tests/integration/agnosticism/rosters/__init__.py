"""Synthetic product 4: rows scoped by a tenant, with named checks and a partial unique.

Exercises what the model declares beyond keys: a CHECK constraint, a unique
index restricted by a predicate, and a naming convention that names every
constraint loom compiles, the composite FK included.
"""

from __future__ import annotations

from loom.core.model import BaseModel, ColumnField, OnDelete, RowScoped, ScopedField
from loom.core.model.types import Boolean, Integer, String, Text

SCHEMA = "rosters"
SCOPE_BINDINGS = {"tenant": "identity.tenant"}
NAMING_CONVENTION = {
    "pk": "pk_%(table_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
}


class Roster(BaseModel, RowScoped):
    __tablename__ = "rosters"
    __unique__ = (("tenant_id", "code"),)
    tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    code: str = ColumnField(Text)


class Seat(BaseModel, RowScoped):
    __tablename__ = "seats"
    __checks__ = {"status_code": "status_code IN ('active', 'removed')"}
    __partial_unique__ = {"owner": (("tenant_id", "roster_id"), "is_owner")}
    __indexes__ = (("tenant_id", "status_code"),)
    tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    roster_id: int = ColumnField(Integer, foreign_key="rosters.id", on_delete=OnDelete.CASCADE)
    status_code: str = ColumnField(Text)
    is_owner: bool = ColumnField(Boolean)


MODELS = (Roster, Seat)
