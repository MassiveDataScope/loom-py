"""A field that admits ``None`` stores ``None`` through the SQLAlchemy repository."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Gauge(BaseModel):
    __tablename__ = "nullable_gauges"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    label: str = ColumnField(length=32)
    level: int | None = None


class _CreateGauge(Command, frozen=True):
    label: str
    level: int | None = None


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Gauge)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    compiled = get_compiled(_Gauge)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    yield manager
    await manager.dispose()


async def test_an_optional_field_left_as_none_is_stored_and_read_back(
    session_manager: SessionManager,
) -> None:
    repo: RepositorySQLAlchemy[_Gauge, int] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Gauge,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )

    created = await repo.create(_CreateGauge(label="idle"))
    stored = await repo.get_by_id(created.id)

    assert created.level is None
    assert stored is not None
    assert (stored.label, stored.level) == ("idle", None)
