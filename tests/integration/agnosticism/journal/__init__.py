"""Synthetic product: one range-partitioned table that is both scoped and audited.

Its partitions are created through the guard, after ``create_schema``; each
must carry exactly one effective audit trigger, the one the parent clones.
"""

from __future__ import annotations

import datetime as dt

from loom.core.model import Audited, BaseModel, ColumnField, Privilege, RowScoped, ScopedField
from loom.core.model.types import DateTime, String, Text

SCHEMA = "journal"
SCOPE_BINDINGS = {"tenant": "identity.subject"}


class Entry(BaseModel, RowScoped, Audited):
    __tablename__ = "entries"
    __scope_privileges__ = frozenset({Privilege.SELECT, Privilege.INSERT})
    __partition_by__ = ("RANGE", "at")
    tenant: str = ScopedField(String(36), primary_key=True, scope="tenant")
    at: dt.datetime = ColumnField(DateTime(), primary_key=True)
    body: str = ColumnField(Text)


MODELS = (Entry,)
