"""The disabled codec refuses a cursor page and leaves an offset page alone."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from loom.core.command import Command
from loom.core.errors import UnsupportedQuery
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.repository import (
    Cursor,
    CursorCodec,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
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
        session_manager=manager, model=_Row, cursor_codec=CursorCodec.disabled()
    )
    await repo.create_many([_CreateRow(rank=rank) for rank in (1, 2, 3)])
    yield repo
    await manager.dispose()


class TestCursorPagingWithTheDisabledCodec:
    async def test_a_cursor_page_is_refused(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        query = QuerySpec(
            pagination=PaginationMode.CURSOR, sort=(SortSpec("rank", "ASC"),), limit=2
        )

        with pytest.raises(UnsupportedQuery, match=r"cursor pagination is not enabled"):
            await repository.list_with_query(query)

    async def test_offset_paging_is_unaffected(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        page = await repository.list_with_query(QuerySpec(sort=(SortSpec("rank", "ASC"),), limit=2))

        assert [row.rank for row in page.items] == [1, 2]


class TestDisabledCodec:
    def test_it_is_one_shared_instance_that_reports_itself_disabled(self) -> None:
        assert CursorCodec.disabled() is CursorCodec.disabled()
        assert not CursorCodec.disabled().is_enabled
        assert CursorCodec(signing="ephemeral").is_enabled

    def test_encode_refuses_with_the_documented_error(self) -> None:
        cursor = Cursor(backend="sqlalchemy", keys=(1,), tie_breaker=1)

        with pytest.raises(UnsupportedQuery, match=r"cursor pagination is not enabled"):
            CursorCodec.disabled().encode(cursor, model="m", query=QuerySpec(), scope="")

    def test_decode_refuses_with_the_documented_error(self) -> None:
        with pytest.raises(UnsupportedQuery, match=r"cursor pagination is not enabled"):
            CursorCodec.disabled().decode(
                "token",
                backend="sqlalchemy",
                model="m",
                query=QuerySpec(),
                scope="",
                columns={},
                primary_key=("id",),
            )
