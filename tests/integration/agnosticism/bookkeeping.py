"""Synthetic product: audited rows, scoped and unscoped, plus one table that is not audited.

``Account`` is scoped by tenant, audited, excludes ``internal_note`` and has a
credential column (``api_token``) the audit log never writes. ``Memo`` is
audited without scopes. ``Draft`` is neither, and must cost nothing.
"""

from __future__ import annotations

import datetime as dt

from loom.core.model import READ_WRITE, Audited, BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Boolean, DateTime, Integer, String, Text

SCHEMA = "bookkeeping"
SCOPE_BINDINGS = {"tenant": "identity.subject"}


class Account(BaseModel, RowScoped, Audited):
    __tablename__ = "accounts"
    __audit_exclude__ = frozenset({"internal_note"})
    tenant: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    name: str = ColumnField(Text)
    email: str = ColumnField(Text)
    balance: int = ColumnField(Integer, default=0)
    currency: str = ColumnField(String(3), default="EUR")
    active: bool = ColumnField(Boolean, default=True)
    opened_at: dt.datetime | None = ColumnField(DateTime(), nullable=True)
    internal_note: str = ColumnField(Text, default="")
    api_token: str = ColumnField(Text, default="")


class Memo(BaseModel, Audited):
    __tablename__ = "memos"
    __privileges__ = {"writers": READ_WRITE}
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    body: str = ColumnField(Text)


class Draft(BaseModel):
    __tablename__ = "drafts"
    __privileges__ = {"writers": READ_WRITE}
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    body: str = ColumnField(Text)


MODELS = (Account, Memo, Draft)
