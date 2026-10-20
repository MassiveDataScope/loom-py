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
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import PageParams, Repository
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

pytestmark = pytest.mark.integration

_ROLE = "loom_it_cache_scope_reader"
_PASSWORD = "loom_it_cache_scope_reader"
_TENANT_SETTING = "loom.scope.tenant"
_tenant: ContextVar[str] = ContextVar("_tenant", default="")


class Note(BaseModel):
    __tablename__ = "loom_it_cache_scope_notes"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    tenant: str = ColumnField()
    body: str = ColumnField()


_TABLE = Note.__tablename__
_RLS_STATEMENTS = (
    f"ALTER TABLE {_TABLE} ENABLE ROW LEVEL SECURITY",
    f"ALTER TABLE {_TABLE} FORCE ROW LEVEL SECURITY",
    f"CREATE POLICY tenant_notes ON {_TABLE}"
    f" USING (tenant = NULLIF(current_setting('{_TENANT_SETTING}', true), ''))",
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
    return {_TENANT_SETTING: _tenant.get()}


async def _run_as_admin(uri: str, statements: tuple[str, ...]) -> None:
    engine = create_async_engine(uri, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            for statement in statements:
                await conn.execute(text(statement))
    finally:
        await engine.dispose()


@pytest.fixture
async def repository(pg_admin_uri: str) -> AsyncIterator[RepositorySQLAlchemy[Note, int]]:
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
        yield RepositorySQLAlchemy(
            session_manager=manager, model=Note, cursor_codec=CursorCodec(signing="ephemeral")
        )
    finally:
        await manager.dispose()
        await _run_as_admin(pg_admin_uri, _TEARDOWN_STATEMENTS)
        reset_registry()


@pytest.fixture
async def cached(
    repository: RepositorySQLAlchemy[Note, int],
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


async def _as_tenant(tenant: str, read: Any) -> Any:
    token = _tenant.set(tenant)
    try:
        return await read()
    finally:
        _tenant.reset(token)


async def test_the_reader_role_is_subject_to_row_level_security(
    repository: RepositorySQLAlchemy[Note, int],
) -> None:
    async with repository.session_manager.session() as session:
        row = (
            await session.execute(
                text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user")
            )
        ).one()
    assert tuple(row) == (False, False)


async def test_a_row_cached_by_id_for_one_scope_is_not_served_to_another(
    repository: RepositorySQLAlchemy[Note, int],
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
