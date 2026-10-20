from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import PageParams
from loom.core.repository.abc import (
    Cursor,
    CursorCodec,
    CursorResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
    UnsupportedQuery,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.query_compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import (
    reset_active_mutations,
    set_active_mutations,
)
from tests.unit.core.conftest import SessionManagerFactory

_Key = tuple[int, int]
_CODEC = CursorCodec(signing="ephemeral")


class _Entry(BaseModel):
    __tablename__ = "composite_entries"

    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    rank: int = ColumnField()


class _CreateEntry(Command, frozen=True):
    account_id: int
    id: int
    rank: int


class _PatchEntry(Command, frozen=True):
    rank: int


# Both accounts reuse the ids 1-3 and the ranks repeat, so neither key column alone
# nor the sort key orders the rows totally.
_SEED = [(7, 1, 2), (7, 2, 1), (7, 3, 2), (7, 4, 1), (7, 5, 2), (8, 1, 1), (8, 2, 2), (8, 3, 1)]


@pytest.fixture
async def session_manager(sqlite_manager_for: SessionManagerFactory) -> SessionManager:
    return await sqlite_manager_for(_Entry)


@pytest.fixture
async def repository(session_manager: SessionManager) -> RepositorySQLAlchemy[_Entry, _Key]:
    repo: RepositorySQLAlchemy[_Entry, _Key] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Entry,
        cursor_codec=_CODEC,
    )
    await repo.create_many(
        [_CreateEntry(account_id=account, id=ident, rank=rank) for account, ident, rank in _SEED]
    )
    return repo


@pytest.fixture
def mutations() -> Iterator[list[MutationEvent]]:
    events, token = set_active_mutations()
    yield events
    reset_active_mutations(token)


async def _walk(
    repository: RepositorySQLAlchemy[_Entry, _Key], sort: tuple[SortSpec, ...]
) -> list[_Key]:
    seen: list[_Key] = []
    cursor: str | None = None
    for _ in range(len(_SEED) + 1):
        query = QuerySpec(sort=sort, pagination=PaginationMode.CURSOR, limit=2, cursor=cursor)
        result = await repository.list_with_query(query)
        assert isinstance(result, CursorResult)
        seen += [(item.account_id, item.id) for item in result.items]
        cursor = result.next_cursor
        if cursor is None:
            break
    return seen


class TestCursorPagination:
    async def test_walk_without_sort_visits_every_row_in_key_order(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        seen = await _walk(repository, ())

        assert seen == sorted((account, ident) for account, ident, _rank in _SEED)

    async def test_walk_by_a_repeated_sort_key_visits_every_row(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        seen = await _walk(repository, (SortSpec(field="rank", direction="DESC"),))

        expected = sorted(_SEED, key=lambda row: (-row[2], row[0], row[1]))
        assert seen == [(account, ident) for account, ident, _rank in expected]

    async def test_token_with_a_single_key_value_is_rejected(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        first_page = QuerySpec(pagination=PaginationMode.CURSOR, limit=2)
        token = _CODEC.encode(
            Cursor(backend="sqlalchemy", keys=(), tie_breaker=7),
            model="_Entry",
            query=first_page,
            scope="",
        )
        query = QuerySpec(pagination=PaginationMode.CURSOR, limit=2, cursor=token)

        with pytest.raises(UnsupportedQuery) as excinfo:
            await repository.list_with_query(query)

        assert "primary key" in excinfo.value.reason

    def test_offset_statement_orders_by_every_key_column(self) -> None:
        compile_all(_Entry)
        sa_model = get_compiled(_Entry)
        assert sa_model is not None
        compiler = QuerySpecCompiler(
            sa_model,
            (sa_model.account_id, sa_model.id),
            frozenset(),
            backend="sqlalchemy",
            model_name="_Entry",
            cursor_codec=_CODEC,
            scope="",
            columns={},
        )

        sql = str(compiler.compile_offset(QuerySpec(sort=(SortSpec("rank", "DESC"),), limit=2)))

        assert sql.split("ORDER BY", 1)[1].split("LIMIT")[0].strip() == (
            "composite_entries.rank DESC, composite_entries.account_id, composite_entries.id"
        )


class TestById:
    async def test_get_by_id_matches_every_key_column(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        entry = await repository.get_by_id((8, 2))

        assert entry is not None
        assert (entry.account_id, entry.id, entry.rank) == (8, 2, 2)

    async def test_get_by_id_refuses_a_partial_key(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        with pytest.raises(ValueError, match=r"\(account_id, id\)"):
            await repository.get_by_id(7)  # type: ignore[arg-type]

    async def test_exists_matches_every_key_column(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        assert await repository.exists((8, 3))
        assert not await repository.exists((8, 4))

    async def test_update_changes_only_the_addressed_row(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        updated = await repository.update((8, 2), _PatchEntry(rank=9))

        assert updated is not None
        assert (updated.account_id, updated.id, updated.rank) == (8, 2, 9)
        untouched = await repository.get_by_id((7, 2))
        assert untouched is not None
        assert untouched.rank == 1

    async def test_delete_removes_only_the_addressed_row(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        assert await repository.delete((8, 2))
        assert not await repository.exists((8, 2))
        assert await repository.exists((7, 2))

    async def test_delete_refuses_a_partial_key(
        self, repository: RepositorySQLAlchemy[_Entry, _Key]
    ) -> None:
        with pytest.raises(ValueError, match=r"\(account_id, id\)"):
            await repository.delete(2)  # type: ignore[arg-type]


async def test_create_many_reselects_rows_by_every_key_column(
    session_manager: SessionManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(session_manager.engine.dialect, "insert_returning", False)
    repository: RepositorySQLAlchemy[_Entry, _Key] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Entry,
        cursor_codec=_CODEC,
    )

    created = await repository.create_many(
        [_CreateEntry(account_id=9, id=2, rank=5), _CreateEntry(account_id=9, id=1, rank=6)]
    )

    assert [(item.account_id, item.id, item.rank) for item in created] == [(9, 2, 5), (9, 1, 6)]


async def test_list_paginated_orders_by_every_key_column(session_manager: SessionManager) -> None:
    repository: RepositorySQLAlchemy[_Entry, _Key] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Entry,
        cursor_codec=_CODEC,
    )
    keys = [(7, 3), (7, 1), (7, 2)]
    await repository.create_many(
        [_CreateEntry(account_id=account, id=ident, rank=0) for account, ident in keys]
    )

    page = await repository.list_paginated(PageParams(page=1, limit=10))

    assert [(item.account_id, item.id) for item in page.items] == sorted(keys)


async def test_create_records_the_full_key_in_ids_and_tags(
    session_manager: SessionManager, mutations: list[MutationEvent]
) -> None:
    repository: RepositorySQLAlchemy[_Entry, _Key] = RepositorySQLAlchemy(
        session_manager=session_manager,
        model=_Entry,
        cursor_codec=_CODEC,
    )

    await repository.create(_CreateEntry(account_id=8, id=4, rank=1))

    (created,) = mutations
    assert created.ids == ((8, 4),)
    assert "composite_entries:id:(8, 4)" in created.tags
