"""Creating on Postgres stores a field left at its default; a missing value names its column."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.command import Command
from loom.core.errors import Conflict
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

pytestmark = pytest.mark.integration


class AcmeAsset(BaseModel):
    __tablename__ = "loom_it_default_assets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=32)
    fields_present: list[str] = ColumnField()


class CreateAsset(Command, frozen=True):
    name: str
    fields_present: list[str] = []


class CreateNamelessAsset(Command, frozen=True):
    fields_present: list[str] = []


@pytest.fixture
async def repo(pg_admin_uri: str) -> AsyncIterator[RepositorySQLAlchemy[AcmeAsset, int]]:
    reset_registry()
    compile_all(AcmeAsset)
    manager = SessionManager(
        pg_admin_uri, pool_size=1, max_overflow=0, pool_pre_ping=False, store=DEFAULT_STORE
    )
    async with manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().drop_all)
        await conn.run_sync(get_metadata().create_all)
    try:
        yield RepositorySQLAlchemy(
            session_manager=manager,
            model=AcmeAsset,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await manager.dispose()
        reset_registry()


class TestCreateKeepsFieldDefaults:
    async def test_create_stores_a_field_left_at_its_default(
        self, repo: RepositorySQLAlchemy[AcmeAsset, int]
    ) -> None:
        created = await repo.create(CreateAsset(name="a"))

        assert created.fields_present == []

    async def test_create_many_stores_a_field_left_at_its_default_in_every_row(
        self, repo: RepositorySQLAlchemy[AcmeAsset, int]
    ) -> None:
        created = await repo.create_many([CreateAsset(name="a"), CreateAsset(name="b")])

        assert [row.fields_present for row in created] == [[], []]


class TestNotNullViolationNamesItsColumn:
    async def test_create_names_the_column_that_is_required(
        self, repo: RepositorySQLAlchemy[AcmeAsset, int]
    ) -> None:
        with pytest.raises(Conflict, match="Field 'name' is required"):
            await repo.create(CreateNamelessAsset())
