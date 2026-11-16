"""Synthetic product 4: rows of a store other than ``default``, scoped by the caller's tenant."""

from __future__ import annotations

from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer, String, Text

SCHEMA = "memos"
SCOPE_BINDINGS = {"owner": "identity.tenant"}


class Memo(BaseModel, RowScoped):
    __tablename__ = "memos"
    __store__ = "audit"
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    body: str = ColumnField(Text)


MODELS = (Memo,)
