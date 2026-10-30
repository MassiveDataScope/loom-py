"""Delta tables of a ``PolarsStepRunner(delta_root=...)``: real reader and writer on one root.

Imported only when a runner is built with ``delta_root``, because it pulls in
``deltalake`` through the Polars backend.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target import SchemaMode, TargetSpec
from loom.etl.declarative.target._table import ReplaceSpec, UpdateSpec, UpsertSpec
from loom.etl.storage import MissingTablePolicy, PrefixLocator
from loom.etl.testing._result import MergeMetrics

_MERGE = "MERGE"


class DeltaTables:
    """Tables of one local Delta root, read and written as production does.

    Args:
        root: Local directory holding the tables, e.g. pytest's ``tmp_path``.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = str(root)
        self._locator = PrefixLocator(self._root)
        self.reader = PolarsSourceReader(self._root)
        self.writer = PolarsTargetWriter(self._root, missing_table_policy=MissingTablePolicy.CREATE)

    def seed(self, ref: str, frame: pl.LazyFrame) -> None:
        """Write *frame* as the whole content of table *ref*, creating it if missing."""
        spec = ReplaceSpec(table_ref=TableRef(ref), schema_mode=SchemaMode.OVERWRITE)
        self.writer.write(frame, spec, None)

    def read(self, ref: str) -> pl.DataFrame:
        """Return the current content of table *ref*."""
        spec = TableSourceSpec(alias=ref, table_ref=TableRef(ref))
        return self.reader.read(spec, None).collect()

    def version(self, spec: TargetSpec) -> int | None:
        """Return the Delta version of the upsert or update target of *spec*, if any."""
        if not isinstance(spec, (UpsertSpec, UpdateSpec)):
            return None
        table = self._open(spec.table_ref)
        return None if table is None else table.version()

    def merge_metrics(self, spec: TargetSpec, version_before: int | None) -> MergeMetrics | None:
        """Return what the MERGE of the last write applied, or ``None`` when none ran.

        A write that committed nothing applied no row.  A first write that
        created the table is not a MERGE and has no merge metrics.

        Args:
            spec: Target spec of the step that just ran.
            version_before: Table version read before the step ran.
        """
        if not isinstance(spec, (UpsertSpec, UpdateSpec)):
            return None
        table = self._open(spec.table_ref)
        if table is None:
            return None
        if version_before is not None and table.version() == version_before:
            return MergeMetrics(inserted=0, updated=0, deleted=0)
        commit = table.history(1)[0]
        if commit.get("operation") != _MERGE:
            return None
        metrics: dict[str, Any] = commit.get("operationMetrics") or {}
        return MergeMetrics(
            inserted=int(metrics.get("num_target_rows_inserted", 0)),
            updated=int(metrics.get("num_target_rows_updated", 0)),
            deleted=int(metrics.get("num_target_rows_deleted", 0)),
        )

    def _open(self, ref: TableRef) -> DeltaTable | None:
        location = self._locator.locate(ref)
        try:
            return DeltaTable(location.uri, storage_options=location.storage_options or None)
        except TableNotFoundError:
            return None
