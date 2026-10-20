"""IntoTable's SQLAlchemy columns come from core's model introspection."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from sqlalchemy import Insert, MetaData
from sqlalchemy.dialects import postgresql

from loom.core.async_bridge import AsyncBridge
from loom.core.backend.sqlalchemy import compile_model
from loom.core.model import BaseModel, ColumnField
from loom.streaming.nodes._table import Backend, IntoTable, SqlAlchemySinkConfig


class _Reading(BaseModel):
    __tablename__ = "readings"

    label: str
    count: int
    ratio: float
    active: bool
    amount: Decimal
    taken_at: datetime
    taken_on: date
    opens_at: time
    blob: bytes
    id: int = ColumnField(primary_key=True)


class _RecordingSession:
    def __init__(self, statements: list[Insert]) -> None:
        self._statements = statements

    async def execute(self, statement: Insert, rows: list[dict[str, Any]]) -> None:
        del rows
        self._statements.append(statement)

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class _RecordingSessionManager:
    def __init__(self) -> None:
        self.statements: list[Insert] = []

    @asynccontextmanager
    async def session(self) -> AsyncIterator[_RecordingSession]:
        yield _RecordingSession(self.statements)


def _ddl_types(columns: Any) -> dict[str, str]:
    dialect = postgresql.dialect()
    return {column.name: column.type.compile(dialect=dialect) for column in columns}


def test_into_table_columns_match_core_columns_for_the_same_payload() -> None:
    bridge = AsyncBridge()
    session_manager = _RecordingSessionManager()
    node = IntoTable(payload=_Reading, table="readings", backend=Backend.SQLALCHEMY)
    partition = node.build_partition(
        SqlAlchemySinkConfig.from_config({"table": "readings"}, default_table="readings"),
        worker_index=0,
        worker_count=1,
        bridge=bridge,
        session_manager=session_manager,
    )
    try:
        partition.write_batch(
            [
                _Reading(
                    id=1,
                    label="a",
                    count=1,
                    ratio=0.5,
                    active=True,
                    amount=Decimal("1.50"),
                    taken_at=datetime(2026, 1, 2, 8, 30),
                    taken_on=date(2026, 1, 2),
                    opens_at=time(8, 30),
                    blob=b"\x00\x01",
                )
            ]
        )
        partition.close()
    finally:
        bridge.shutdown()

    core_table = compile_model(_Reading, metadata=MetaData()).__table__
    (statement,) = session_manager.statements
    assert _ddl_types(statement.table.columns) == _ddl_types(core_table.columns)
