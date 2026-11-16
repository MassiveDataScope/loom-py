"""Without cursor settings no codec exists: a cursor page is refused, an offset page is not."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from loom.core.command import Command
from loom.core.errors import UnsupportedQuery
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.repository import CursorCodec, PaginationMode, QuerySpec, SortSpec
from loom.stores.sqlalchemy.repository import RepositorySQLAlchemy
from loom.stores.sqlalchemy.schema import compile_all, get_compiled
from loom.stores.sqlalchemy.session_manager import SessionManager


class _Row(BaseModel):
    __tablename__ = "cursor_disabled_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()


class _CreateRow(Command, frozen=True):
    rank: int


@pytest.fixture
async def repository() -> AsyncIterator[RepositorySQLAlchemy[_Row, int]]:
    compile_all(_Row)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
        store=DEFAULT_STORE,
    )
    compiled = get_compiled(_Row)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    repo: RepositorySQLAlchemy[_Row, int] = RepositorySQLAlchemy(
        session_manager=manager, model=_Row, cursor_codec=None
    )
    await repo.create_many([_CreateRow(rank=rank) for rank in (1, 2, 3)])
    yield repo
    await manager.dispose()


class TestCursorPagingWithoutACodec:
    async def test_a_cursor_page_is_refused_naming_the_section(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        query = QuerySpec(
            pagination=PaginationMode.CURSOR, sort=(SortSpec("rank", "ASC"),), limit=2
        )

        with pytest.raises(UnsupportedQuery, match=r"stores\.settings\.cursors"):
            await repository.list_with_query(query)

    async def test_offset_paging_is_unaffected(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        page = await repository.list_with_query(QuerySpec(sort=(SortSpec("rank", "ASC"),), limit=2))

        assert [row.rank for row in page.items] == [1, 2]


class TestRequiredCodec:
    def test_the_codec_is_returned_when_present(self) -> None:
        codec = CursorCodec(signing="ephemeral")

        assert CursorCodec.required(codec) is codec

    def test_a_missing_codec_raises_the_documented_error(self) -> None:
        with pytest.raises(UnsupportedQuery, match=r"cursor pagination is not enabled"):
            CursorCodec.required(None)
