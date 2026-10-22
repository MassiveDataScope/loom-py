"""ClickHouse implementation of the streaming table sink."""

from __future__ import annotations

import math
from collections.abc import Sequence
from time import monotonic
from typing import Any
from uuid import uuid4

import msgspec

from loom.core.connections import ClickHouseConnection
from loom.core.connections.clients.clickhouse import build_clickhouse_client
from loom.streaming.nodes._table.common import (
    ClickHouseSinkConfig,
    _column_names,
    _set_column_values,
)


def _to_ch_row(item: Any, column_names: list[str], target: str) -> list[Any]:
    """Return *item*'s values for *column_names*, in order, for a ``clickhouse-connect`` insert.

    The values come from :func:`_set_column_values`, which refuses an ``UNSET``
    value and converts the others as core converts a column value. They then
    get the minimal coercions ClickHouse native types require:

    - ``bool``      → ``int`` (ClickHouse ``UInt8`` expects 0/1, not ``True``/``False``)
    - ``dict``      → JSON text from ``msgspec.json``, keys sorted (no universal
      Map type in CH); a struct arrives here as a dict, and a ``datetime`` or
      ``Decimal`` inside it is encoded as an ISO-8601 or decimal string

    All other values (``str``, ``int``, ``float``, ``datetime``, ``date``,
    ``Decimal``, ``UUID``, ``list``, ``None``) are passed through unchanged, a
    ``list`` as it arrives, its structs already turned into dicts;
    ``clickhouse-connect`` writes a ``Date`` column from ``date`` objects only.
    """
    result: list[Any] = []
    for value in _set_column_values(item, column_names, target):
        if isinstance(value, bool):
            result.append(int(value))
        elif isinstance(value, dict):
            result.append(msgspec.json.encode(value, order="sorted").decode())
        else:
            result.append(value)
    return result


def _bounded(settings: ClickHouseSinkConfig) -> ClickHouseConnection:
    """Apply ``insert_timeout`` to a connection that declares no timeout of its own."""
    connection = settings.connection
    declares = connection.connect_timeout is not None or connection.send_receive_timeout is not None
    if settings.insert_timeout is None or declares:
        return connection
    # The driver takes whole seconds above zero; round a fraction up, never to 0.
    seconds = max(1, math.ceil(settings.insert_timeout))
    return msgspec.structs.replace(
        connection, connect_timeout=seconds, send_receive_timeout=seconds
    )


class _ClickHouseTablePartition:
    """Bulk-insert epoch batches into ClickHouse via ``clickhouse-connect``.

    Uses the native HTTP/2 client — no SQLAlchemy, no async bridge required.
    The buffer and flush policy mirror :class:`_DeltaTablePartition`: data
    accumulates in memory and is flushed as a single ``INSERT`` when any
    of the configured thresholds is reached.

    **Column-oriented buffer layout** — the internal buffer is a list of
    *column* lists rather than a list of *row* lists::

        buffer = [
            [col0_row0, col0_row1, ...],   # one list per column
            [col1_row0, col1_row1, ...],
            ...
        ]

    This matches the ``column_oriented=True`` path in ``clickhouse-connect``,
    which skips the internal ``pivot()`` transposition and sends the data
    directly when the batch fits in a single network block (the common case).

    ``list[str]`` and other collection fields are handled by the native
    ClickHouse protocol without JSON serialisation workarounds.

    Args:
        settings:     Resolved ClickHouse sink config.
        payload_type: Struct type whose fields define the column layout.

    Raises:
        ImportError: If ``clickhouse-connect`` is not installed.
    """

    def __init__(
        self,
        settings: ClickHouseSinkConfig,
        payload_type: type[Any],
        logger: Any = None,
    ) -> None:
        self._client = build_clickhouse_client(_bounded(settings))
        self._logger = logger
        self._table = settings.table
        self._target = f"ClickHouse table {settings.table!r}"
        self._column_names = _column_names(payload_type)
        self._buffer_policy = settings.buffer_policy
        # Column-oriented: one inner list per column, grows with each row.
        self._buffer: list[list[Any]] = [[] for _ in self._column_names]
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._buffer_started_at: float | None = None
        self._closed = False

    def write_batch(self, items: Sequence[Any]) -> int:
        """Buffer items and flush to ClickHouse when a threshold is reached.

        Args:
            items: Struct instances to write.  An empty batch still checks
                the age threshold; Bytewax 0.21.1 calls ``write_batch`` only
                when items arrive, so it does not send one on idle epochs.

        Returns:
            How many buffered items this call inserted into ClickHouse.
        """
        if not items:
            if self._buffer_records and self._should_flush():
                return self._flush()
            return 0
        durable = 0
        for item in items:
            if not self._buffer_records:
                self._buffer_started_at = monotonic()
            row = _to_ch_row(item, self._column_names, self._target)
            row_bytes = sum(len(str(v)) for v in row)
            for col_idx, value in enumerate(row):
                self._buffer[col_idx].append(value)
            self._buffer_records += 1
            self._buffer_bytes += row_bytes
            if self._should_flush():
                durable += self._flush()
        return durable

    def close(self) -> int:
        """Flush remaining buffer and close the ClickHouse client.

        Returns:
            How many buffered items the final flush inserted into ClickHouse.
        """
        if self._closed:
            return 0
        self._closed = True
        try:
            if self._buffer_records and self._buffer_policy.flush_on_shutdown:
                return self._flush()
            return 0
        finally:
            self._client.close()

    def _should_flush(self) -> bool:
        if self._closed or not self._buffer_records:
            return False
        if self._buffer_records >= self._buffer_policy.max_records:
            return True
        if self._buffer_bytes >= self._buffer_policy.max_bytes:
            return True
        if self._buffer_started_at is None:
            return False
        return monotonic() - self._buffer_started_at >= self._buffer_policy.max_age_s

    def _flush(self) -> int:
        if not self._buffer_records:
            return 0
        n = self._buffer_records
        columns = self._buffer
        trace_id = uuid4().hex
        correlation_id = uuid4().hex
        t0 = monotonic()
        try:
            self._client.insert(
                self._table,
                columns,
                column_names=self._column_names,
                column_oriented=True,
            )
        except Exception:
            if self._logger is not None:
                self._logger.error(
                    "sink_flush_error",
                    backend="clickhouse",
                    table=self._table,
                    records=n,
                )
            raise
        self._buffer = [[] for _ in self._column_names]
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._buffer_started_at = None
        if self._logger is not None:
            self._logger.info(
                "sink_flush",
                trace_id=trace_id,
                correlation_id=correlation_id,
                backend="clickhouse",
                table=self._table,
                records=n,
                duration_ms=round((monotonic() - t0) * 1000, 1),
            )
        return n
