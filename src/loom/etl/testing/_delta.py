"""Delta root of a ``PolarsStepRunner(delta_root=...)``: real reader and writer on one root."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from loom.etl.backends.polars import PolarsSourceReader, PolarsTargetWriter
from loom.etl.backends.polars._writer import _last_commit, _open_delta_table
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target import SchemaMode, TargetSpec
from loom.etl.declarative.target._table import ReplaceSpec, UpdateSpec, UpsertSpec
from loom.etl.storage import MissingTablePolicy, PrefixLocator
from loom.etl.testing._result import MergeMetrics

_MERGE = "MERGE"


class _DeltaRoot:
    """One local Delta root whose tables are read and written as production does.

    :meth:`write` is a target writer: it delegates to the real
    :class:`PolarsTargetWriter` and keeps, in :attr:`last_merge`, what the
    MERGE of an upsert or update applied.

    Args:
        root: Local directory holding the tables, e.g. pytest's ``tmp_path``.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = str(root)
        self._locator = PrefixLocator(self._root)
        self.reader = PolarsSourceReader(self._root)
        self.writer = PolarsTargetWriter(self._root, missing_table_policy=MissingTablePolicy.CREATE)
        self.last_merge: MergeMetrics | None = None

    def seed(self, ref: str, frame: pl.LazyFrame) -> None:
        """Write *frame* as the whole content of table *ref*, creating it if missing."""
        spec = ReplaceSpec(table_ref=TableRef(ref), schema_mode=SchemaMode.OVERWRITE)
        self.writer.write(frame, spec, None)

    def read(self, ref: str) -> pl.DataFrame:
        """Return the current content of table *ref*."""
        spec = TableSourceSpec(alias=ref, table_ref=TableRef(ref))
        return self.reader.read(spec, None).collect()

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        params_instance: Any,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        """Write with the real writer, recording the MERGE metrics of an upsert or update.

        :attr:`last_merge` is ``None`` when the target is not an upsert or
        update, or when the table did not exist and this write created it.
        """
        self.last_merge = None
        table = None
        if isinstance(spec, (UpsertSpec, UpdateSpec)):
            table = _open_delta_table(self._locator.locate(spec.table_ref))
        version_before = None if table is None else table.version()
        self.writer.write(frame, spec, params_instance, streaming=streaming, write_ctx=write_ctx)
        if table is None:
            return
        table.update_incremental()
        version, commit = _last_commit(table)
        if version == version_before:
            self.last_merge = MergeMetrics(inserted=0, updated=0, deleted=0)
        elif commit.get("operation") == _MERGE:
            self.last_merge = _merge_metrics(commit.get("operationMetrics") or {})


def _merge_metrics(metrics: dict[str, Any]) -> MergeMetrics:
    """Read delta-rs MERGE ``operationMetrics`` (snake_case keys)."""
    return MergeMetrics(
        inserted=int(metrics.get("num_target_rows_inserted", 0)),
        updated=int(metrics.get("num_target_rows_updated", 0)),
        deleted=int(metrics.get("num_target_rows_deleted", 0)),
    )
