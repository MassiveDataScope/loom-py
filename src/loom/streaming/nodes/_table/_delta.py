"""Delta implementation of the streaming table sink."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import suppress
from tempfile import SpooledTemporaryFile
from time import monotonic
from typing import Any
from uuid import uuid4

import pyarrow as _pa
import pyarrow.ipc as _pa_ipc
from deltalake.writer import WriterProperties, write_deltalake

from loom.core.model.redaction import reveal_for_driver
from loom.streaming.nodes._table.common import (
    DeltaSinkConfig,
    _column_names,
    _set_column_values,
)


def build_writer_properties(config: Mapping[str, Any]) -> WriterProperties:
    kwargs = dict(config)
    compression = kwargs.get("compression")
    if isinstance(compression, str):
        kwargs["compression"] = compression.upper()
    if not kwargs:
        kwargs["max_row_group_size"] = 10_000
    return WriterProperties(**kwargs)


class _DeltaTablePartition:
    """Stage rows as a PyArrow IPC stream and append them to Delta.

    Rows accumulate in a plain ``list[dict]``; every ``mini_batch_size`` rows
    they are converted to a single ``pa.RecordBatch`` and written to a
    ``SpooledTemporaryFile``-backed IPC stream.  At flush the stream is closed,
    rewound, and passed directly to ``write_deltalake`` as a
    ``RecordBatchReader`` — delta-rs iterates the batches without collecting
    them into a single in-memory frame.

    Args:
        settings:     Resolved Delta sink config.
        payload_type: Struct type whose column fields map to table columns.

    Raises:
        ImportError: If ``deltalake``, ``polars``, or ``pyarrow`` are not installed.
    """

    def __init__(
        self,
        settings: DeltaSinkConfig,
        payload_type: type[Any],
        logger: Any = None,
    ) -> None:
        self._logger = logger
        self._column_names = _column_names(payload_type)
        self._target = f"Delta table {settings.table!r}"
        self._uri = settings.uri
        self._mode = settings.mode
        self._storage_options = dict(settings.storage_options)
        self._partition_by = settings.partition_by
        self._buffer_policy = settings.buffer_policy
        self._spool_max_bytes = settings.spool_max_bytes
        self._target_file_size = settings.target_file_size
        self._staging_compression = settings.staging_compression
        self._writer_properties = settings.writer_properties
        self._mini_batch_size = settings.mini_batch_size
        self._mini_batch: list[dict[str, Any]] = []
        self._ipc_writer: Any = None  # pa.ipc.RecordBatchStreamWriter | None
        self._spool: Any = self._new_spool()
        self._staged_records = 0
        self._staged_bytes = 0
        self._buffer_started_at: float | None = None
        self._closed = False

    def _new_spool(self) -> Any:
        return SpooledTemporaryFile(max_size=self._spool_max_bytes)  # noqa: SIM115

    def _ipc_compression(self) -> Any:
        if self._staging_compression == "uncompressed":
            return None
        return self._staging_compression  # "lz4" | "zstd"

    def write_batch(self, items: Sequence[Any]) -> int:
        """Accumulate items into mini-batches and flush when a threshold is reached.

        Args:
            items: Struct instances to write.  An empty batch still checks
                the age threshold; Bytewax 0.21.1 calls ``write_batch`` only
                when items arrive, so it does not send one on idle epochs.

        Returns:
            How many buffered items this call wrote to the Delta table.
        """
        if not items:
            if self._staged_records and self._should_flush():
                return self._flush()
            return 0
        durable = 0
        for item in items:
            if self._staged_records == 0:
                self._buffer_started_at = monotonic()
            self._stage_item(item)
            if self._should_flush():
                durable += self._flush()
        return durable

    def _stage_item(self, item: Any) -> None:
        values = _set_column_values(item, self._column_names, self._target)
        self._mini_batch.append(dict(zip(self._column_names, values, strict=True)))
        self._staged_records += 1
        if len(self._mini_batch) >= self._mini_batch_size:
            self._commit_mini_batch()

    def _commit_mini_batch(self) -> None:
        if not self._mini_batch:
            return
        batch = _pa.RecordBatch.from_pylist(self._mini_batch)
        if self._ipc_writer is None:
            options = _pa_ipc.IpcWriteOptions(compression=self._ipc_compression())
            self._ipc_writer = _pa_ipc.new_stream(self._spool, batch.schema, options=options)
            if self._logger is not None:
                self._logger.debug(
                    "delta_staging_stream_opened",
                    uri=self._uri,
                    schema_fields=len(batch.schema),
                    compression=self._staging_compression,
                )
        self._ipc_writer.write_batch(batch)
        pos = self._spool.tell()
        self._staged_bytes = pos
        if self._logger is not None:
            self._logger.debug(
                "delta_mini_batch_written",
                uri=self._uri,
                batch_rows=len(self._mini_batch),
                spool_bytes=pos,
            )
        self._mini_batch.clear()

    def _should_flush(self) -> bool:
        if self._closed or not self._staged_records:
            return False
        if self._staged_records >= self._buffer_policy.max_records:
            return True
        if self._staged_bytes >= self._buffer_policy.max_bytes:
            if self._logger is not None:
                self._logger.warning(
                    "delta_staging_max_bytes_reached",
                    uri=self._uri,
                    staged_bytes=self._staged_bytes,
                    max_bytes=self._buffer_policy.max_bytes,
                )
            return True
        if self._buffer_started_at is None:
            return False
        return monotonic() - self._buffer_started_at >= self._buffer_policy.max_age_s

    def _flush(self) -> int:
        self._commit_mini_batch()
        writer = self._ipc_writer
        if writer is None:
            return 0
        n = self._staged_records
        spool = self._spool
        self._spool = self._new_spool()
        self._ipc_writer = None
        self._staged_records = 0
        self._staged_bytes = 0
        self._buffer_started_at = None
        trace_id = uuid4().hex
        correlation_id = uuid4().hex
        t0 = monotonic()
        try:
            writer.close()
            spool.seek(0)
            reader = _pa_ipc.open_stream(spool)
            self._write_to_delta(reader, n, trace_id, correlation_id, t0)
        except Exception:
            if self._logger is not None:
                self._logger.error(
                    "sink_flush_error",
                    backend="delta",
                    uri=self._uri,
                    records=n,
                )
            raise
        finally:
            spool.close()
        return n

    def _write_to_delta(
        self,
        reader: Any,
        n: int,
        trace_id: str,
        correlation_id: str,
        t0: float,
    ) -> None:
        kwargs: dict[str, Any] = {
            "mode": self._mode,
            "storage_options": reveal_for_driver(self._storage_options) or None,
        }
        if self._partition_by:
            kwargs["partition_by"] = list(self._partition_by)
        if self._target_file_size is not None:
            kwargs["target_file_size"] = self._target_file_size
        if self._writer_properties is not None:
            kwargs["writer_properties"] = self._writer_properties
        if self._logger is not None:
            self._logger.debug(
                "delta_write_start",
                uri=self._uri,
                records=n,
                mode=self._mode,
            )
        write_deltalake(self._uri.reveal(), reader, **kwargs)
        if self._logger is not None:
            self._logger.info(
                "sink_flush",
                trace_id=trace_id,
                correlation_id=correlation_id,
                backend="delta",
                uri=self._uri,
                records=n,
                duration_ms=round((monotonic() - t0) * 1000, 1),
            )

    def close(self) -> int:
        """Flush staged records if configured and release all resources.

        Returns:
            How many buffered items the final flush wrote to the Delta table.
        """
        if self._closed:
            return 0
        self._closed = True
        try:
            if self._staged_records and self._buffer_policy.flush_on_shutdown:
                return self._flush()
            if self._staged_records and self._logger is not None:
                self._logger.warning(
                    "delta_staged_records_discarded_on_close",
                    uri=self._uri,
                    records=self._staged_records,
                )
            return 0
        finally:
            if self._ipc_writer is not None:
                with suppress(Exception):
                    self._ipc_writer.close()
            self._spool.close()
