"""The repository cache on Postgres row-level security: one scope's rows stay in it."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextvars import ContextVar
from typing import Any, cast

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.cache import (
    CacheBackend,
    CacheConfig,
    CachedRepository,
    CacheGateway,
    GenerationalDependencyResolver,
)
from loom.core.cache.decorators import cache_query
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import PageParams, Repository
from loom.core.repository.abc import CursorCodec, PaginationMode, QuerySpec, SortSpec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

pytestmark = pytest.mark.integration

_ROLE = "loom_it_cache_scope_reader"
_PASSWORD = "loom_it_cache_scope_reader"
_TENANT_SETTING = "loom.scope.tenant"
_ELEVATED_SETTING = "loom.scope.tenant.any"
_APP_TENANT_SETTING = "app.tenant_id"
_settings: ContextVar[Mapping[str, str] | None] = ContextVar("_settings", default=None)


class Note(BaseModel):
    __tablename__ = "loom_it_cache_scope_notes"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    tenant: str = ColumnField()
    body: str = ColumnField()


class NoteRepository(RepositorySQLAlchemy[Note, int]):
    """Adds the two kinds of custom cached read on top of the stock repository."""

    @cache_query(scope="list")
    async def bodies(self) -> list[str]:
        async with self.session_manager.session() as session:
            rows = await session.execute(text(f"SELECT body FROM {_TABLE} ORDER BY body"))
            return list(rows.scalars())

    @cache_query(scope="entity")
    async def body_of(self, note_id: int) -> str:
        async with self.session_manager.session() as session:
            rows = await session.execute(
                text(f"SELECT body FROM {_TABLE} WHERE id = :id"), {"id": note_id}
            )
            return rows.scalar_one_or_none() or ""


_TABLE = Note.__tablename__
_RLS_STATEMENTS = (
    f"ALTER TABLE {_TABLE} ENABLE ROW LEVEL SECURITY",
    f"ALTER TABLE {_TABLE} FORCE ROW LEVEL SECURITY",
    # The tenant comes from loom's scope setting or, for a product that sets
    # its own, from app.tenant_id; the elevation flag shows every row.
    f"CREATE POLICY tenant_notes ON {_TABLE} USING ("
    f"tenant = COALESCE(NULLIF(current_setting('{_TENANT_SETTING}', true), ''),"
    f" NULLIF(current_setting('{_APP_TENANT_SETTING}', true), ''))"
    f" OR current_setting('{_ELEVATED_SETTING}', true) = 'on')",
    f"DROP ROLE IF EXISTS {_ROLE}",
    f"CREATE ROLE {_ROLE} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{_PASSWORD}'",
    f"GRANT SELECT ON {_TABLE} TO {_ROLE}",
    f"INSERT INTO {_TABLE} (tenant, body) VALUES ('a', 'a1'), ('a', 'a2'), ('b', 'b1')",
)
_TEARDOWN_STATEMENTS = (
    f"DROP TABLE IF EXISTS {_TABLE}",
    f"DROP OWNED BY {_ROLE}",
    f"DROP ROLE IF EXISTS {_ROLE}",
)


def _scope_settings() -> Mapping[str, str]:
    return _settings.get() or {}


def _tenant(tenant: str) -> Mapping[str, str]:
    return {_TENANT_SETTING: tenant}


async def _run_as_admin(uri: str, statements: tuple[str, ...]) -> None:
    engine = create_async_engine(uri, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            for statement in statements:
                await conn.execute(text(statement))
    finally:
        await engine.dispose()


@pytest.fixture
async def repository(pg_admin_uri: str) -> AsyncIterator[NoteRepository]:
    reset_registry()
    compile_all(Note)
    admin = create_async_engine(pg_admin_uri, poolclass=NullPool)
    try:
        async with admin.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
            await conn.run_sync(get_metadata().create_all)
    finally:
        await admin.dispose()
    await _run_as_admin(pg_admin_uri, _RLS_STATEMENTS)
    reader_uri = (
        make_url(pg_admin_uri)
        .set(username=_ROLE, password=_PASSWORD)
        .render_as_string(hide_password=False)
    )
    manager = SessionManager(
        reader_uri,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=False,
        session_settings=_scope_settings,
    )
    try:
        yield NoteRepository(
            session_manager=manager, model=Note, cursor_codec=CursorCodec(signing="ephemeral")
        )
    finally:
        await manager.dispose()
        await _run_as_admin(pg_admin_uri, _TEARDOWN_STATEMENTS)
        reset_registry()


@pytest.fixture
async def cached(
    repository: NoteRepository,
) -> AsyncIterator[CachedRepository[Note, Any, Any, int]]:
    CacheGateway.configure(
        {
            "default": {"cache": "aiocache.SimpleMemoryCache"},
            "cache_scope_data": {
                "cache": "aiocache.SimpleMemoryCache",
                "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
                "namespace": "cache_scope_data",
            },
            "cache_scope_counters": {
                "cache": "aiocache.SimpleMemoryCache",
                "namespace": "cache_scope_counters",
            },
        }
    )
    data = CacheGateway(alias="cache_scope_data")
    counters = CacheGateway(alias="cache_scope_counters")
    config = CacheConfig(
        enabled=True,
        aiocache_alias="cache_scope_data",
        counter_alias="cache_scope_counters",
        default_ttl=120,
        default_list_ttl=60,
    )
    try:
        yield CachedRepository(
            repository=cast(Repository[Note, Any, Any, int], repository),
            config=config,
            cache=cast(CacheBackend, data),
            dependency_resolver=GenerationalDependencyResolver(cast(CacheBackend, counters)),
        )
    finally:
        await data.clear()
        await counters.clear()
        await data.close()
        await counters.close()


async def _as(settings: Mapping[str, str], read: Any) -> Any:
    token = _settings.set(settings)
    try:
        return await read()
    finally:
        _settings.reset(token)


async def _as_tenant(tenant: str, read: Any) -> Any:
    return await _as(_tenant(tenant), read)


async def test_the_reader_role_is_subject_to_row_level_security(
    repository: NoteRepository,
) -> None:
    async with repository.session_manager.session() as session:
        row = (
            await session.execute(
                text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user")
            )
        ).one()
    assert tuple(row) == (False, False)


async def test_a_row_cached_by_id_for_one_scope_is_not_served_to_another(
    repository: NoteRepository,
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    a_page = await _as_tenant("a", lambda: repository.list_paginated(PageParams(page=1, limit=10)))
    a_id = a_page.items[0].id
    assert await _as_tenant("b", lambda: repository.get_by_id(a_id)) is None

    seen_by_a = await _as_tenant("a", lambda: cached.get_by_id(a_id))
    seen_by_b = await _as_tenant("b", lambda: cached.get_by_id(a_id))

    assert seen_by_a is not None
    assert seen_by_a.tenant == "a"
    assert seen_by_b is None


async def test_a_page_cached_for_one_scope_is_not_served_to_another(
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    page = PageParams(page=1, limit=10)

    seen_by_a = await _as_tenant("a", lambda: cached.list_paginated(page))
    seen_by_b = await _as_tenant("b", lambda: cached.list_paginated(page))

    assert sorted(note.body for note in seen_by_a.items) == ["a1", "a2"]
    assert [note.body for note in seen_by_b.items] == ["b1"]
    assert seen_by_b.total_count == 1


async def test_a_custom_list_read_cached_for_one_scope_is_not_served_to_another(
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    seen_by_a = await _as_tenant("a", lambda: cached.bodies())
    seen_by_b = await _as_tenant("b", lambda: cached.bodies())

    assert seen_by_a == ["a1", "a2"]
    assert seen_by_b == ["b1"]


async def test_a_custom_entity_read_cached_for_one_scope_is_not_served_to_another(
    repository: NoteRepository,
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    a_page = await _as_tenant("a", lambda: repository.list_paginated(PageParams(page=1, limit=10)))
    a_id = a_page.items[0].id

    seen_by_a = await _as_tenant("a", lambda: cached.body_of(a_id))
    seen_by_b = await _as_tenant("b", lambda: cached.body_of(a_id))

    assert seen_by_a == "a1"
    assert seen_by_b == ""


async def test_a_query_page_cached_for_one_scope_is_not_served_to_another(
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    query = QuerySpec(sort=(SortSpec(field="id"),), pagination=PaginationMode.OFFSET, limit=10)

    seen_by_a = await _as_tenant("a", lambda: cached.list_with_query(query))
    seen_by_b = await _as_tenant("b", lambda: cached.list_with_query(query))

    assert [note.body for note in seen_by_a.items] == ["a1", "a2"]
    assert [note.body for note in seen_by_b.items] == ["b1"]


async def test_a_page_cached_for_one_product_setting_is_not_served_to_another(
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    """A policy may read a setting outside loom.scope.*; the cache keeps it apart too."""
    page = PageParams(page=1, limit=10)

    seen_by_a = await _as({_APP_TENANT_SETTING: "a"}, lambda: cached.list_paginated(page))
    seen_by_b = await _as({_APP_TENANT_SETTING: "b"}, lambda: cached.list_paginated(page))

    assert sorted(note.body for note in seen_by_a.items) == ["a1", "a2"]
    assert [note.body for note in seen_by_b.items] == ["b1"]


async def test_a_page_cached_for_an_elevated_caller_is_not_served_to_a_plain_one(
    cached: CachedRepository[Note, Any, Any, int],
) -> None:
    page = PageParams(page=1, limit=10)
    elevated = {_TENANT_SETTING: "a", _ELEVATED_SETTING: "on"}
    plain = {_TENANT_SETTING: "a", _ELEVATED_SETTING: ""}

    seen_elevated = await _as(elevated, lambda: cached.list_paginated(page))
    seen_plain = await _as(plain, lambda: cached.list_paginated(page))

    assert sorted(note.body for note in seen_elevated.items) == ["a1", "a2", "b1"]
    assert sorted(note.body for note in seen_plain.items) == ["a1", "a2"]
