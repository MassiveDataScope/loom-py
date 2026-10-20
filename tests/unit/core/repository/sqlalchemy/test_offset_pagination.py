from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import CursorCodec, FilterParams, PageParams, QuerySpec, SortSpec
from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Row(BaseModel):
    __tablename__ = "offset_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()


class _CreateRow(Command, frozen=True):
    rank: int


_RANKS = [1, 2, 2, 3, 3]


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Row)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    compiled = get_compiled(_Row)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    yield manager
    await manager.dispose()


@pytest.fixture
async def repository(session_manager: SessionManager) -> RepositorySQLAlchemy[_Row, int]:
    repo: RepositorySQLAlchemy[_Row, int] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Row,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    await repo.create_many([_CreateRow(rank=rank) for rank in _RANKS])
    return repo


class TestPagePastTheEnd:
    async def test_list_paginated_reports_the_filtered_total(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        result = await repository.list_paginated(
            PageParams(page=9, limit=2), FilterParams(filters={"rank": 3})
        )

        assert (result.items, result.total_count, result.has_next) == ((), 2, False)


class TestOffsetOrdering:
    def test_offset_statement_orders_by_id_last(self) -> None:
        """The id tie-breaker is pinned on the statement: sqlite's scan order hides its absence."""
        compile_all(_Row)
        sa_model = get_compiled(_Row)
        assert sa_model is not None
        compiler = QuerySpecCompiler(
            sa_model,
            sa_model.id,
            frozenset(),
            backend="sqlalchemy",
            model=_Row,
            cursor_codec=CursorCodec(signing="ephemeral"),
            scope="",
            columns={},
        )

        sql = str(compiler.compile_offset(QuerySpec(sort=(SortSpec("rank", "DESC"),), limit=2)))

        assert sql.split("ORDER BY", 1)[1].split("LIMIT")[0].strip() == (
            "offset_rows.rank DESC, offset_rows.id"
        )
