from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField, DateTime
from loom.core.repository.abc import (
    Cursor,
    CursorCodec,
    CursorResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
    UnsupportedQuery,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Row(BaseModel):
    __tablename__ = "cursor_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()
    label: str = ColumnField(length=16)


class _CreateRow(Command, frozen=True):
    rank: int
    label: str


class _Reading(BaseModel):
    __tablename__ = "cursor_naive_readings"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    taken_at: datetime = ColumnField(DateTime(tz=False))


class _CreateReading(Command, frozen=True):
    taken_at: datetime


_CODEC = CursorCodec(signing="ephemeral")


# Duplicate (rank, label) pairs straddle page boundaries at limit=2, so only the id
# tie-breaker keeps the walk exact.
_SEED = [
    (3, "b"),
    (3, "a"),
    (3, "a"),
    (1, "a"),
    (2, "b"),
    (2, "a"),
    (3, "a"),
    (2, "a"),
]


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Row, _Reading)
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
        session_manager=session_manager, model=_Row, cursor_codec=_CODEC
    )
    await repo.create_many([_CreateRow(rank=rank, label=label) for rank, label in _SEED])
    return repo


class TestRejectedTokens:
    @staticmethod
    def _query(token: str | None = None) -> QuerySpec:
        return QuerySpec(
            sort=(SortSpec(field="rank", direction="DESC"),),
            pagination=PaginationMode.CURSOR,
            limit=3,
            cursor=token,
        )

    def _token(self, cursor: Cursor) -> str:
        return _CODEC.encode(cursor, model="_Row", query=self._query(), scope="")

    async def test_token_from_another_backend(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        query = self._query(self._token(Cursor(backend="dynamodb", keys=(3,), tie_breaker=1)))

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(query)

        assert excinfo.value.backend == "sqlalchemy"
        assert excinfo.value.model == "_Row"

    async def test_token_issued_for_a_different_sort(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        token = self._token(Cursor(backend="sqlalchemy", keys=(3, "a"), tie_breaker=1))

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(self._query(token))

        assert "sort" in excinfo.value.reason

    async def test_token_signed_by_another_process(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        other = CursorCodec(signing="ephemeral")
        token = other.encode(
            Cursor(backend="sqlalchemy", keys=(3,), tie_breaker=1),
            model="_Row",
            query=self._query(),
            scope="",
        )

        with pytest.raises(UnsupportedQuery):
            await repository.list_with_query(self._query(token))


class TestNaiveDateTimeSort:
    """A ``DateTime(tz=False)`` sort key walks every page: tokens carry UTC, columns stay naive."""

    async def test_every_row_is_visited_once_in_order(
        self, session_manager: SessionManager
    ) -> None:
        repository: RepositorySQLAlchemy[_Reading, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Reading, cursor_codec=_CODEC
        )
        start = datetime(2026, 9, 6, 12, 0)
        stamps = [start + timedelta(minutes=minutes) for minutes in (5, 1, 3, 1, 4, 2)]
        await repository.create_many([_CreateReading(taken_at=stamp) for stamp in stamps])

        seen: list[datetime] = []
        cursor: str | None = None
        while True:
            query = QuerySpec(
                sort=(SortSpec(field="taken_at"),),
                pagination=PaginationMode.CURSOR,
                limit=2,
                cursor=cursor,
            )
            page = await repository.list_with_query(query)
            assert isinstance(page, CursorResult)
            seen.extend(item.taken_at for item in page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

        assert seen == sorted(stamps)
        assert all(stamp.tzinfo is None for stamp in seen)
