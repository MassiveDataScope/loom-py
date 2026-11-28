"""Streaming-write helpers for the Polars Delta backend.

Bridges a ``pl.LazyFrame`` to an Arrow ``RecordBatchReader`` via an
intermediate IPC spool on disk.  Memory is bounded by Polars' streaming
engine plus one batch in flight.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc

from loom.core.environment import SPOOL_DIR_VARIABLE, loom_variable


class StreamingSpool(NamedTuple):
    reader: pa.RecordBatchReader
    spool_path: Path


def _resolve_spool_dir(spool_dir: str | os.PathLike[str] | None) -> str | None:
    if spool_dir is not None:
        return str(spool_dir)
    env_dir = loom_variable(SPOOL_DIR_VARIABLE)
    return env_dir if env_dir else None


@contextmanager
def spool_lazy_to_arrow_reader(
    frame: pl.LazyFrame,
    *,
    spool_dir: str | os.PathLike[str] | None = None,
) -> Iterator[StreamingSpool]:
    """Sink *frame* to an IPC file and yield an Arrow ``RecordBatchReader``.

    The spool file lives in a temporary directory removed on context exit
    even when the consumer raises.  The yielded reader is single-pass;
    callers needing a separate scan (e.g. for partition pruning) should
    ``pl.scan_ipc(spool_path)`` while the context is active.

    Args:
        frame:     Lazy frame to materialise to a streaming Arrow source.
        spool_dir: Base directory for the temporary spool.  When ``None``
                   defaults to ``$LOOM_SPOOL_DIR`` if set, else the
                   system temp dir.  Set this on container workloads
                   where ``/tmp`` is a ``tmpfs`` mount (e.g. Fargate)
                   to avoid spilling back to RAM.

    Yields:
        A :class:`StreamingSpool` with the reader and the spool path.
    """
    base_dir = _resolve_spool_dir(spool_dir)
    with tempfile.TemporaryDirectory(prefix="loom-spool-", dir=base_dir) as tmp_dir:
        spool_path = Path(tmp_dir) / "spool.arrow"
        frame.sink_ipc(spool_path, compression="lz4")
        source = ipc.open_file(spool_path)

        def _iter_batches() -> Iterator[pa.RecordBatch]:
            for index in range(source.num_record_batches):
                yield source.get_batch(index)

        reader = pa.RecordBatchReader.from_batches(source.schema, _iter_batches())
        yield StreamingSpool(reader=reader, spool_path=spool_path)


def partition_rows_from_spool(
    spool_path: Path,
    partition_cols: tuple[str, ...],
) -> list[dict[str, object]]:
    """Return distinct partition combinations from the spool file."""
    frame = (
        pl.scan_ipc(spool_path).select(list(partition_cols)).unique().collect(engine="streaming")
    )
    return list(frame.iter_rows(named=True))
