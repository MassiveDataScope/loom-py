"""Backend protocol and result types for Delta table maintenance."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:
    from loom.etl.maintenance._ops import CompactSpec, MaintenanceSpec, VacuumSpec, ZOrderSpec
    from loom.etl.storage._config import StorageConfig
    from loom.etl.storage._locator import TableLocation


@runtime_checkable
class OperationDeclaration(Protocol):
    """Unified interface for declarative operation builders.

    Both :class:`~loom.etl.maintenance.MaintainTable` and
    :class:`~loom.etl.maintenance.MaintainSchema` implement this Protocol
    so the runner can iterate them uniformly without isinstance branching.
    """

    def resolve(self, config: StorageConfig) -> list[MaintenanceSpec]: ...


@dataclass
class VacuumResult:
    """Result of a VACUUM operation.

    Args:
        files_deleted: Paths of the files that were deleted (or that *would*
            be deleted when ``dry_run=True``).
    """

    files_deleted: list[str] = field(default_factory=list)


@dataclass
class OptimizeResult:
    """Result of a compaction or Z-Order operation.

    Uses counts (not lists) — consistent with the ``numFilesAdded`` /
    ``numFilesRemoved`` keys returned by the deltalake metrics dict.
    """

    num_files_added: int = 0
    num_files_removed: int = 0


@runtime_checkable
class OpSpec(Protocol):
    """Protocol implemented by VacuumSpec, CompactSpec, ZOrderSpec — and any future op.

    Each spec knows how to dispatch itself against a DeltaTableMaintainer.
    Adding a new operation type requires only a new spec class; the runner
    does not change.
    """

    name: ClassVar[str]

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> VacuumResult | OptimizeResult: ...


@dataclass
class TableMaintenanceResult:
    """Per-table maintenance outcome.

    Args:
        table_ref: Logical table reference (e.g. ``"raw.events"``).
        op_results: Keyed results for each op that ran, by ``op.name``.
        error: Exception caught during execution, or ``None`` on success.
        duration_seconds: Wall-clock time for all ops on this table.
    """

    table_ref: str
    op_results: dict[str, VacuumResult | OptimizeResult] = field(default_factory=dict)
    error: Exception | None = None
    duration_seconds: float = 0.0

    @property
    def ok(self) -> bool:
        """``True`` when no error occurred."""
        return self.error is None

    @property
    def vacuum(self) -> VacuumResult | None:
        result = self.op_results.get("vacuum")
        return result if isinstance(result, VacuumResult) else None

    @property
    def compact(self) -> OptimizeResult | None:
        result = self.op_results.get("compact")
        return result if isinstance(result, OptimizeResult) else None

    @property
    def z_order(self) -> OptimizeResult | None:
        result = self.op_results.get("z_order")
        return result if isinstance(result, OptimizeResult) else None


@runtime_checkable
class DeltaTableMaintainer(Protocol):
    """Protocol for executing maintenance operations on a single Delta table.

    Implementations receive the full :class:`~loom.etl.storage._locator.TableLocation`
    so they can propagate ``storage_options``, ``writer_properties``, and
    ``commit_properties`` to every delta-rs call — matching the behaviour of
    :class:`~loom.etl.backends.polars.PolarsTargetWriter`.
    """

    def vacuum(
        self,
        uri: str,
        spec: VacuumSpec,
        location: TableLocation,
    ) -> VacuumResult:
        """Run VACUUM on the table at *uri*."""
        ...

    def compact(
        self,
        uri: str,
        spec: CompactSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run bin-packing compaction on the table at *uri*."""
        ...

    def z_order(
        self,
        uri: str,
        spec: ZOrderSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run Z-Order clustering on the table at *uri*."""
        ...
