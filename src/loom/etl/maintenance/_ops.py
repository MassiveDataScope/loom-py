"""Operation specs for Delta table maintenance.

These dataclasses are the compiled form of MaintainTable / MaintainSchema
declarations: they carry the final parameter values the backend executes.

Internal module — import them from :mod:`loom.etl.maintenance`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from loom.etl.maintenance._protocol import (
        DeltaTableMaintainer,
        OpSpec,
        OptimizeResult,
        VacuumResult,
    )
    from loom.etl.storage._locator import TableLocation


@dataclass(frozen=True)
class VacuumSpec:
    """Parameters for a VACUUM operation.

    Args:
        retention_hours: Files older than this are eligible for deletion.
            ``None`` uses the table's ``delta.deletedFileRetentionDuration``
            property (default 168 hours / 7 days).
        dry_run: When ``True`` (the default), lists eligible files without
            deleting them.  Set ``dry_run=False`` explicitly in production.
        full: When ``True``, also deletes files under the table directory
            that the Delta log does not reference (aborted writes, files whose
            remove entries the log no longer holds), provided they are older
            than the retention.  It lists the whole table directory, which
            costs one storage listing per run.  ``False`` (the default)
            deletes only the files the log records as removed.
    """

    name: ClassVar[str] = "vacuum"
    retention_hours: int | None = None
    dry_run: bool = True
    full: bool = False

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> VacuumResult:
        return maintainer.vacuum(uri, self, location)


@dataclass(frozen=True)
class CompactSpec:
    """Parameters for a file compaction (bin-packing) operation.

    Args:
        target_size: Target output file size in bytes.  ``None`` lets
            delta-rs choose based on the table's configuration.
    """

    name: ClassVar[str] = "compact"
    target_size: int | None = None

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> OptimizeResult:
        return maintainer.compact(uri, self, location)


@dataclass(frozen=True)
class ZOrderSpec:
    """Parameters for a Z-Order clustering operation.

    Args:
        columns: Columns to Z-Order by.  Must be non-empty.
        target_size: Target output file size in bytes.  ``None`` uses
            delta-rs default.
    """

    name: ClassVar[str] = "z_order"
    columns: list[str] = field(default_factory=list)
    target_size: int | None = None

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> OptimizeResult:
        return maintainer.z_order(uri, self, location)


@dataclass(frozen=True)
class MaintenanceSpec:
    """The ops to run, in order, against one table.

    :meth:`~loom.etl.maintenance.OperationDeclaration.resolve` returns one per
    table; the runner resolves ``table_ref`` to a location and runs each op.

    Args:
        table_ref: Logical table reference (e.g. ``"raw.events"``).
        ops: Operations to execute against the table, in order.
    """

    table_ref: str
    ops: tuple[OpSpec, ...] = ()
