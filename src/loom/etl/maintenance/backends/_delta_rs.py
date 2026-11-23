"""delta-rs (deltalake) implementation of DeltaTableMaintainer.

This module is only imported when DeltaRsMaintainer is used — it is not part
of the ``loom.etl.maintenance`` top-level import path — so importing deltalake
at module level is safe and avoids hidden lazy-import latency at call time.
"""

from __future__ import annotations

import logging

from deltalake import CommitProperties, DeltaTable, WriterProperties

from loom.etl.event import EtlEvent, EtlKey
from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
from loom.etl.maintenance._protocol import OptimizeResult, VacuumResult
from loom.etl.storage._locator import TableLocation

_log = logging.getLogger(__name__)


class DeltaRsMaintainer:
    """Execute maintenance operations via the ``deltalake`` Python package (delta-rs)."""

    def _open(
        self, uri: str, location: TableLocation
    ) -> tuple[DeltaTable, WriterProperties | None, CommitProperties | None]:
        """Open a DeltaTable and build write-time config objects from location."""
        dt = DeltaTable(uri, storage_options=location.storage_options or {})
        writer = WriterProperties(**location.writer) if location.writer else None
        commit = CommitProperties(**location.commit) if location.commit else None
        return dt, writer, commit

    def vacuum(
        self,
        uri: str,
        spec: VacuumSpec,
        location: TableLocation,
    ) -> VacuumResult:
        """Run VACUUM on the Delta table at *uri*."""
        dt, _writer, commit = self._open(uri, location)
        deleted = dt.vacuum(
            retention_hours=spec.retention_hours,
            dry_run=spec.dry_run,
            full=spec.full,
            commit_properties=commit,
        )
        _log.info(
            EtlEvent.TABLE_VACUUMED,
            extra={EtlKey.URI: uri, EtlKey.FILES: len(deleted)},
        )
        return VacuumResult(files_deleted=deleted)

    def compact(
        self,
        uri: str,
        spec: CompactSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run bin-packing compaction on the Delta table at *uri*."""
        dt, writer, commit = self._open(uri, location)
        result = dt.optimize.compact(
            target_size=spec.target_size,
            writer_properties=writer,
            commit_properties=commit,
        )
        _log.info(
            EtlEvent.TABLE_COMPACTED,
            extra={EtlKey.URI: uri},
        )
        return OptimizeResult(
            num_files_added=result["numFilesAdded"],
            num_files_removed=result["numFilesRemoved"],
        )

    def z_order(
        self,
        uri: str,
        spec: ZOrderSpec,
        location: TableLocation,
    ) -> OptimizeResult:
        """Run Z-Order clustering on the Delta table at *uri*."""
        dt, writer, commit = self._open(uri, location)
        result = dt.optimize.z_order(
            columns=spec.columns,
            target_size=spec.target_size,
            writer_properties=writer,
            commit_properties=commit,
        )
        _log.info(
            EtlEvent.TABLE_Z_ORDERED,
            extra={EtlKey.URI: uri, EtlKey.COLUMNS: spec.columns},
        )
        return OptimizeResult(
            num_files_added=result["numFilesAdded"],
            num_files_removed=result["numFilesRemoved"],
        )
