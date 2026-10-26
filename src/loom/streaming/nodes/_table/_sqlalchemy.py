"""SQLAlchemy implementation of the streaming table sink."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from time import monotonic
from typing import Any
from uuid import uuid4

from sqlalchemy import insert as _sa_insert

from loom.core.async_bridge import AsyncBridge
from loom.core.backend.sqlalchemy import sa_table_for
from loom.core.repository.sqlalchemy.values import to_column_row
from loom.streaming.nodes._table.common import (
    SqlAlchemySinkConfig,
    _row_size_bytes,
    _SessionManagerLike,
)


class _SQLAlchemyTablePartition:
    """Bulk-insert epoch batches into a relational table via SQLAlchemy async sessions.

    The partition reuses a shared SessionManager owned by the Bytewax adapter
    and uses the Bytewax async bridge to execute batch inserts without exposing
    event-loop management to callers.
    Columns and their types are those core compiles for the payload's fields,
    and each value takes the form core's repository writes; no ORM mapping or
    reflection is required.

    Args:
        settings:        Resolved SQLAlchemy sink config.
        payload_type:    Struct type whose fields map to table columns.
        bridge:          Bytewax async bridge used to run async writes.
        session_manager: Shared SQLAlchemy session manager for this DB config.

    Raises:
        ImportError: If ``sqlalchemy`` is not installed.
    """

    def __init__(
        self,
        settings: SqlAlchemySinkConfig,
        payload_type: type[Any],
        bridge: AsyncBridge,
        session_manager: _SessionManagerLike,
        logger: Any = None,
    ) -> None:
        self._bridge = bridge
        self._session_manager = session_manager
        self._logger = logger
        self._table = sa_table_for(payload_type, settings.table)
        self._chunk_size = settings.chunk_size
        self._buffer_policy = settings.buffer_policy
        self._buffer: deque[tuple[dict[str, Any], int]] = deque()
        self._buffer_records = 0
        self._buffer_bytes = 0
        self._buffer_started_at: float | None = None
        self._closed = False

    def write_batch(self, items: Sequence[Any]) -> int:
        """Buffer one epoch batch and bulk-insert it when a threshold is reached.

        Args:
            items: Struct instances to insert.  An empty batch still checks
                the age threshold; Bytewax 0.21.1 calls ``write_batch`` only
                when items arrive, so it does not send one on idle epochs.

        Returns:
            How many buffered items this call committed to the table.
        """
        if not items:
            if self._buffer and self._should_flush():
                return self._bridge.run(self._flush_async())
            return 0
        durable = 0
        for item in items:
            row = to_column_row(item, self._table.columns)
            row_bytes = _row_size_bytes(row)
            if not self._buffer:
                self._buffer_started_at = monotonic()
            self._buffer.append((row, row_bytes))
            self._buffer_records += 1
            self._buffer_bytes += row_bytes
            if self._should_flush():
                durable += self._bridge.run(self._flush_async())
        return durable

    def close(self) -> int:
        """Release partition resources.

        The shared session manager is owned by the adapter runtime and disposed
        centrally when the Bytewax worker shuts down.

        Returns:
            How many buffered items the final flush committed to the table.
        """
        if self._closed:
            return 0
        self._closed = True
        if self._buffer and self._buffer_policy.flush_on_shutdown:
            return self._bridge.run(self._flush_async())
        return 0

    def _should_flush(self) -> bool:
        if self._closed or not self._buffer:
            return False
        if self._buffer_records >= self._buffer_policy.max_records:
            return True
        if self._buffer_bytes >= self._buffer_policy.max_bytes:
            return True
        if self._buffer_started_at is None:
            return False
        return monotonic() - self._buffer_started_at >= self._buffer_policy.max_age_s

    async def _flush_async(self) -> int:
        statement = _sa_insert(self._table)
        trace_id = uuid4().hex
        correlation_id = uuid4().hex
        async with self._session_manager.session() as session:
            try:
                n = 0
                t0 = monotonic()
                while self._buffer:
                    rows: list[dict[str, Any]] = []
                    while self._buffer and len(rows) < self._chunk_size:
                        row, row_bytes = self._buffer.popleft()
                        rows.append(row)
                        self._buffer_records -= 1
                        self._buffer_bytes -= row_bytes
                    for same_columns in _rows_by_column_set(rows):
                        await session.execute(statement, same_columns)
                    n += len(rows)
                await session.commit()
                if self._logger is not None:
                    self._logger.info(
                        "sink_flush",
                        trace_id=trace_id,
                        correlation_id=correlation_id,
                        backend="sqlalchemy",
                        table=self._table.name,
                        records=n,
                        duration_ms=round((monotonic() - t0) * 1000, 1),
                    )
            except Exception:
                await session.rollback()
                raise
        self._buffer_started_at = None
        return n


def _rows_by_column_set(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group *rows* by the columns they set, keeping their order within a group.

    One executemany INSERT takes its columns from the first row: a later row
    missing one fails, and a column only a later row sets is dropped.
    """
    groups: dict[frozenset[str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(frozenset(row), []).append(row)
    return list(groups.values())
