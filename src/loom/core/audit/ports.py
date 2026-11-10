"""Audit log reader boundary."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from loom.core.audit.entry import AuditEntry
from loom.core.audit.subjects import AuditActors
from loom.core.repository.abc import CursorResult, QuerySpec


@runtime_checkable
class AuditLogReader(Protocol):
    """Read a cursor page of audit entries for resolved actors."""

    async def read(self, query: QuerySpec, actors: AuditActors) -> CursorResult[AuditEntry]: ...
