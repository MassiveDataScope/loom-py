"""Creating through the SQLAlchemy repository stores a field left at its default."""

from __future__ import annotations

from collections.abc import AsyncIterator

import msgspec
import pytest

from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.repository import CursorCodec
from loom.stores.sqlalchemy.repository import RepositorySQLAlchemy
from loom.stores.sqlalchemy.schema import compile_all, get_compiled
from loom.stores.sqlalchemy.session_manager import SessionManager


class _Asset(BaseModel):
    __tablename__ = "default_assets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=32)
    fields_present: list[str] = ColumnField()
    rank: int = ColumnField()


class _CreateAsset(Command, frozen=True):
    name: str
    fields_present: list[str] = []
    rank: int = 0


class _UpdateAsset(Command, frozen=True):
    name: str | msgspec.UnsetType = msgspec.UNSET
    rank: int = 0
    fields_present: list[str] = []


@pytest.fixture
async def repo() -> AsyncIterator[RepositorySQLAlchemy[_Asset, int]]:
    compile_all(_Asset)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
        store=DEFAULT_STORE,
    )
    compiled = get_compiled(_Asset)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    yield RepositorySQLAlchemy(
        session_manager=manager,
        model=_Asset,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    await manager.dispose()


class TestCreateKeepsFieldDefaults:
    async def test_create_stores_a_field_left_at_its_default(
        self, repo: RepositorySQLAlchemy[_Asset, int]
    ) -> None:
        created = await repo.create(_CreateAsset(name="a"))

        assert (created.fields_present, created.rank) == ([], 0)

    async def test_create_object_holds_a_field_left_at_its_default(
        self, repo: RepositorySQLAlchemy[_Asset, int]
    ) -> None:
        obj = repo.create_object(_CreateAsset(name="a"))

        assert (obj.fields_present, obj.rank) == ([], 0)

    async def test_create_many_stores_a_field_left_at_its_default_in_every_row(
        self, repo: RepositorySQLAlchemy[_Asset, int]
    ) -> None:
        created = await repo.create_many([_CreateAsset(name="a"), _CreateAsset(name="b")])

        assert [(row.fields_present, row.rank) for row in created] == [([], 0), ([], 0)]

    async def test_create_many_keeps_a_value_set_in_one_row_only(
        self, repo: RepositorySQLAlchemy[_Asset, int]
    ) -> None:
        created = await repo.create_many([_CreateAsset(name="a", rank=3), _CreateAsset(name="b")])

        assert [row.rank for row in created] == [3, 0]


class TestUpdateKeepsUnsentFields:
    async def test_update_does_not_overwrite_a_field_the_caller_did_not_send(
        self, repo: RepositorySQLAlchemy[_Asset, int]
    ) -> None:
        created = await repo.create(_CreateAsset(name="a", fields_present=["x"], rank=5))

        updated = await repo.update(created.id, _UpdateAsset(name="b"))

        assert updated is not None
        assert (updated.name, updated.fields_present, updated.rank) == ("b", ["x"], 5)
