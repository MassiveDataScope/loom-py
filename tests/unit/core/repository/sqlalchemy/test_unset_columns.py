"""A field annotated ``X | UnsetType`` is stored as ``X`` through the SQLAlchemy repository."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import msgspec
import pytest
from sqlalchemy import text

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Badge(BaseModel):
    __tablename__ = "unset_badges"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    label: str | msgspec.UnsetType = msgspec.UNSET
    note: str | None | msgspec.UnsetType = msgspec.UNSET


class _CreateBadge(Command, frozen=True):
    label: str
    note: str | None | msgspec.UnsetType = msgspec.UNSET


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Badge)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    compiled = get_compiled(_Badge)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    yield manager
    await manager.dispose()


@pytest.fixture
def repo(session_manager: SessionManager) -> RepositorySQLAlchemy[_Badge, int]:
    return RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Badge,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


async def _stored_row(session_manager: SessionManager, badge_id: int) -> tuple[Any, ...]:
    """The row as the database holds it, bypassing the repository's decoding."""
    async with session_manager.engine.connect() as connection:
        result = await connection.execute(
            text("SELECT label, note FROM unset_badges WHERE id = :id"), {"id": badge_id}
        )
        return tuple(result.one())


async def test_an_unset_field_left_out_on_create_is_stored_as_null(
    repo: RepositorySQLAlchemy[_Badge, int], session_manager: SessionManager
) -> None:
    created = await repo.create(_CreateBadge(label="first"))

    assert await _stored_row(session_manager, created.id) == ("first", None)
    assert await repo.get_by_id(created.id) == _Badge(id=created.id, label="first", note=None)


async def test_an_unset_field_given_on_create_is_stored_as_its_scalar_value(
    repo: RepositorySQLAlchemy[_Badge, int], session_manager: SessionManager
) -> None:
    created = await repo.create(_CreateBadge(label="first", note="kept"))

    assert await _stored_row(session_manager, created.id) == ("first", "kept")
    assert await repo.get_by_id(created.id) == _Badge(id=created.id, label="first", note="kept")
