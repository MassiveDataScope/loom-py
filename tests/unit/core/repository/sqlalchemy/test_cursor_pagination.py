from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, time, timedelta, timezone
from enum import Enum

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.errors.codes import ErrorCode
from loom.core.model import BaseModel, BoundaryValidationError, ColumnField, DateTime, String
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


class _Shade(Enum):
    DARK = "dark"
    LIGHT = "light"
    PALE = "pale"


class _Swatch(BaseModel):
    __tablename__ = "cursor_swatches"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    shade: _Shade = ColumnField(String(8))


class _CreateSwatch(Command, frozen=True):
    shade: _Shade


class _Rated(BaseModel):
    __tablename__ = "cursor_rated_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    score: int | None = ColumnField(nullable=True, default=None)
    payload: bytes = ColumnField()
    tags: list[str] = ColumnField()


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
        return _CODEC.encode(cursor, model=f"{_Row.__module__}._Row", query=self._query(), scope="")

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
            model=f"{_Row.__module__}._Row",
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


class TestSortFieldsACursorCannotCarry:
    """Refused before the query runs, instead of a first page whose token no page accepts."""

    @pytest.mark.parametrize(
        "field",
        ["score", "payload", "tags", "rank.value", "missing"],
        ids=["nullable", "bytes", "json", "dotted", "unknown"],
    )
    async def test_the_first_page_is_refused_naming_the_field(
        self, session_manager: SessionManager, field: str
    ) -> None:
        compile_all(_Rated)
        repo: RepositorySQLAlchemy[_Rated, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Rated, cursor_codec=_CODEC
        )
        query = QuerySpec(sort=(SortSpec(field),), pagination=PaginationMode.CURSOR, limit=2)

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repo.list_with_query(query)

        assert f"cannot order by {field!r}" in excinfo.value.reason

    async def test_an_empty_cursor_is_refused_like_any_invalid_token(
        self, repository: RepositorySQLAlchemy[_Row, int]
    ) -> None:
        query = QuerySpec(
            sort=(SortSpec("rank"),), pagination=PaginationMode.CURSOR, limit=2, cursor=""
        )

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(query)

        assert excinfo.value.reason == "cursor token is not valid"


async def test_a_plain_enum_sort_walks_every_page(session_manager: SessionManager) -> None:
    """The decoded key is the enum member; the predicate binds what the column stores."""
    compile_all(_Swatch)
    compiled = get_compiled(_Swatch)
    assert compiled is not None
    async with session_manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    repo: RepositorySQLAlchemy[_Swatch, int] = RepositorySQLAlchemy(
        session_manager=session_manager, model=_Swatch, cursor_codec=_CODEC
    )
    await repo.create_many([_CreateSwatch(shade=shade) for shade in _Shade])
    query = QuerySpec(sort=(SortSpec("shade"),), pagination=PaginationMode.CURSOR, limit=2)

    first = await repo.list_with_query(query)
    assert isinstance(first, CursorResult)
    second = await repo.list_with_query(
        QuerySpec(sort=query.sort, pagination=query.pagination, limit=2, cursor=first.next_cursor)
    )

    assert isinstance(second, CursorResult)
    walked = [item.shade for item in (*first.items, *second.items)]
    assert walked == sorted(_Shade, key=lambda shade: shade.value)


class _Shift(BaseModel):
    __tablename__ = "cursor_shifts"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    starts_at: time = ColumnField()


async def test_a_forged_aware_time_for_a_naive_time_column_is_refused(
    session_manager: SessionManager,
) -> None:
    """A position is compared as a filter value is, offset rule included."""
    compile_all(_Shift)
    compiled = get_compiled(_Shift)
    assert compiled is not None
    async with session_manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    unsigned = CursorCodec(signing="unsigned")
    repo: RepositorySQLAlchemy[_Shift, int] = RepositorySQLAlchemy(
        session_manager=session_manager, model=_Shift, cursor_codec=unsigned
    )
    query = QuerySpec(sort=(SortSpec("starts_at"),), pagination=PaginationMode.CURSOR, limit=2)
    aware = time(9, 30, tzinfo=timezone(timedelta(hours=2)))
    token = unsigned.encode(
        Cursor(backend="sqlalchemy", keys=(aware,), tie_breaker=1),
        model=_Shift,
        query=query,
        scope="",
    )
    forged = QuerySpec(sort=query.sort, pagination=query.pagination, limit=2, cursor=token)

    with pytest.raises(BoundaryValidationError) as excinfo:
        await repo.list_with_query(forged)

    assert excinfo.value.code == ErrorCode.QUERY_VALIDATION
