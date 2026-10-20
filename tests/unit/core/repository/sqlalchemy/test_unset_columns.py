"""A field annotated ``X | UnsetType`` round-trips through the SQLAlchemy repository."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal

import msgspec
import pytest
from sqlalchemy import MetaData

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


def test_an_unset_field_compiles_to_its_scalar_column() -> None:
    compile_all(_Badge)
    compiled = get_compiled(_Badge)
    assert compiled is not None
    columns = compiled.metadata.tables["unset_badges"].c

    assert (type(columns.label.type).__name__, columns.label.nullable) == ("String", False)
    assert (type(columns.note.type).__name__, columns.note.nullable) == ("String", True)


def test_an_unset_decimal_needs_a_declared_column_type_like_a_decimal() -> None:
    class _Priced(BaseModel):
        __tablename__ = "unset_priced"

        id: int = ColumnField(primary_key=True)
        price: Decimal | msgspec.UnsetType = msgspec.UNSET

    metadata = MetaData()

    with pytest.raises(ValueError, match="no column type is inferred from a Decimal"):
        compile_all(_Priced, metadata=metadata)


async def test_an_unset_field_left_out_on_create_is_read_back_as_null(
    repo: RepositorySQLAlchemy[_Badge, int],
) -> None:
    created = await repo.create(_CreateBadge(label="first"))
    stored = await repo.get_by_id(created.id)

    assert stored is not None
    assert (stored.label, stored.note) == ("first", None)


async def test_an_unset_field_given_on_create_is_read_back(
    repo: RepositorySQLAlchemy[_Badge, int],
) -> None:
    created = await repo.create(_CreateBadge(label="first", note="kept"))
    stored = await repo.get_by_id(created.id)

    assert stored is not None
    assert (stored.label, stored.note) == ("first", "kept")
