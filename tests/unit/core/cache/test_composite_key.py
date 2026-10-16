"""Cached list reads over a SQLAlchemy model whose primary key is composite.

The cached index stores each row's full key; msgpack hands it back as a list,
so the wrapper must restore the tuple the entity entries and ``get_by_id`` use.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, NamedTuple

import pytest
from sqlalchemy import event

from loom.core.cache import (
    CacheConfig,
    CachedRepository,
    GenerationalDependencyResolver,
    cache_query,
)
from loom.core.cache.keys import entity_key
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import PageParams, PageResult
from loom.core.repository.abc import CursorResult, PaginationMode, QuerySpec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.unit.core.conftest import SessionManagerFactory

from ._doubles import CountingCacheBackend

_Key = tuple[int, int]
_PROFILE = "default"
# Account 7 holds two rows, so the first key column alone repeats.
_KEYS: list[_Key] = [(7, 1), (7, 2), (8, 1)]


class _Entry(BaseModel):
    __tablename__ = "cached_composite_entries"

    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    rank: int = ColumnField()


class _CreateEntry(Command, frozen=True):
    account_id: int
    id: int
    rank: int


class _PatchEntry(Command, frozen=True):
    rank: int


class _EntryRepository(RepositorySQLAlchemy[_Entry, _Key]):
    @cache_query(scope="entity")
    async def rank_of(self, key: _Key) -> int:
        entry = await self.get_by_id(key)
        return -1 if entry is None else entry.rank


_Cached = CachedRepository[_Entry, _CreateEntry, _PatchEntry, _Key]
_Read = Callable[[_Cached], Awaitable[PageResult[_Entry] | CursorResult[_Entry]]]


class _Env(NamedTuple):
    wrapper: _Cached
    backend: CountingCacheBackend
    resolver: GenerationalDependencyResolver
    statements: list[str]


def _read_paginated(wrapper: _Cached) -> Awaitable[PageResult[_Entry]]:
    return wrapper.list_paginated(PageParams(page=1, limit=10))


def _read_cursor(wrapper: _Cached) -> Awaitable[PageResult[_Entry] | CursorResult[_Entry]]:
    return wrapper.list_with_query(QuerySpec(pagination=PaginationMode.CURSOR, limit=10))


_READS = pytest.mark.parametrize(
    "read", [_read_paginated, _read_cursor], ids=["list_paginated", "list_with_query_cursor"]
)


@pytest.fixture
async def session_manager(sqlite_session_manager: SessionManagerFactory) -> SessionManager:
    return await sqlite_session_manager(_Entry)


@pytest.fixture
async def env(session_manager: SessionManager, cache_config: CacheConfig) -> AsyncIterator[_Env]:
    repository = _EntryRepository(session_manager=session_manager, model=_Entry)
    await repository.create_many(
        [_CreateEntry(account_id=account, id=ident, rank=ident) for account, ident in _KEYS]
    )
    backend = CountingCacheBackend()
    resolver = GenerationalDependencyResolver(backend)
    wrapper: _Cached = CachedRepository(
        repository, config=cache_config, cache=backend, dependency_resolver=resolver
    )
    statements: list[str] = []

    def _capture(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        statements.append(statement)

    sync_engine = session_manager.engine.sync_engine
    event.listen(sync_engine, "before_cursor_execute", _capture)
    yield _Env(wrapper, backend, resolver, statements)
    event.remove(sync_engine, "before_cursor_execute", _capture)


def _keys(result: PageResult[_Entry] | CursorResult[_Entry]) -> list[_Key]:
    return [(item.account_id, item.id) for item in result.items]


async def _evict(env: _Env, key: _Key) -> None:
    tags = env.resolver.entity_tags(env.wrapper.entity_name, key)
    fingerprint = await env.resolver.fingerprint(tags)
    del env.backend.data[entity_key(env.wrapper.entity_name, key, _PROFILE, fingerprint)]


class TestCachedList:
    @_READS
    async def test_warm_read_serves_every_row_from_the_cache(self, env: _Env, read: _Read) -> None:
        await read(env.wrapper)
        env.statements.clear()

        result = await read(env.wrapper)

        assert _keys(result) == _KEYS
        assert env.statements == []

    @_READS
    async def test_evicted_entity_is_reloaded_by_its_full_key(self, env: _Env, read: _Read) -> None:
        await read(env.wrapper)
        await _evict(env, (7, 2))

        result = await read(env.wrapper)

        assert _keys(result) == _KEYS

    async def test_several_evicted_entities_are_reloaded_by_their_full_keys(
        self, env: _Env
    ) -> None:
        await _read_paginated(env.wrapper)
        for key in _KEYS:
            await _evict(env, key)

        result = await _read_paginated(env.wrapper)

        assert _keys(result) == _KEYS
        assert [item.rank for item in result.items] == [ident for _account, ident in _KEYS]


class TestEntityScopedRead:
    async def test_takes_the_full_key(self, env: _Env) -> None:
        assert await env.wrapper.rank_of((7, 2)) == 2

    async def test_refuses_a_partial_key(self, env: _Env) -> None:
        with pytest.raises(TypeError, match=r"received a int .*expected tuple\[int, int\]"):
            await env.wrapper.rank_of(7)
