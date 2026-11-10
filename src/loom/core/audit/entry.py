"""Introspection-only audit log model."""

from __future__ import annotations

from datetime import datetime
from functools import cache
from typing import Any

from loom.core.model import BaseModel, ColumnField, NotMaterialized
from loom.core.persistence.stores import store_key


class AuditEntry(BaseModel, NotMaterialized):
    """One visible audit log row, never materialized as a model table."""

    at: datetime = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    op: str = ColumnField()
    schema_name: str = ColumnField()
    table: str = ColumnField()
    row_key: str = ColumnField()
    changed: dict[str, Any] = ColumnField()
    subject: str = ColumnField()
    mechanism: str = ColumnField()
    on_behalf_of: str = ColumnField()
    trace_id: str = ColumnField()
    elevated: tuple[str, ...] = ColumnField()
    origin: str = ColumnField()


@cache
def audit_entry_for(key: str) -> type[AuditEntry]:
    """Return the audit model bound to a store."""
    key = store_key(key)
    return type(f"AuditEntry_{key}", (AuditEntry,), {"__store__": key})
