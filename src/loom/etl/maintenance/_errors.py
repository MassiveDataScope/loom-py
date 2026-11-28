"""Errors raised when maintaining Delta tables."""

from __future__ import annotations

from loom.core.errors import SystemError
from loom.etl.maintenance._protocol import TableMaintenanceResult


class MaintenanceError(SystemError):
    """Raised by :meth:`MaintenanceReport.raise_if_errors` when any table failed."""

    def __init__(self, failed: list[TableMaintenanceResult]) -> None:
        self.failed = failed
        refs = [r.table_ref for r in failed]
        super().__init__(f"Maintenance failed for {len(failed)} table(s): {refs}")
