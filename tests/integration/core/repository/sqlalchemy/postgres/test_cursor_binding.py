"""Cursor pagination on Postgres: naive timestamps, the scope binding and typed values."""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator, Mapping
from contextvars import ContextVar
from datetime import datetime, timedelta
from typing import Any, cast

import msgspec
import pytest

from loom.core.cache import (
    CacheConfig,
    CachedRepository,
    CacheGateway,
    CacheStore,
    GenerationalDependencyResolver,
)
from loom.core.command import Command
from loom.core.errors import UnsupportedQuery
from loom.core.model import BaseModel, ColumnField, DateTime
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.repository import (
    CursorCodec,
    CursorResult,
    PaginationMode,
    QuerySpec,
    Repository,
    SortSpec,
)
from loom.stores.sqlalchemy.repository import SQLAlchemyRepository
from loom.stores.sqlalchemy.schema import compile_all, get_metadata, reset_registry
from loom.stores.sqlalchemy.session_manager import SessionManager
from loom.testing import make

pytestmark = pytest.mark.integration

_tenant: ContextVar[str] = ContextVar("_tenant", default="")
_START = datetime(2026, 9, 6, 12, 0)
_QUERY = make(
    QuerySpec, sort=(SortSpec(field="taken_at"),), pagination=PaginationMode.CURSOR, limit=2
)


class Reading(BaseModel):
    __tablename__ = "loom_it_cursor_readings"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    taken_at: datetime = ColumnField(DateTime(tz=False))


class CreateReading(Command, frozen=True):
    taken_at: datetime


def _scope_settings() -> Mapping[str, str]:
    return {"loom.scope.tenant": _tenant.get()}


@pytest.fixture
async def repository(pg_admin_uri: str) -> AsyncIterator[SQLAlchemyRepository[Reading, int]]:
    reset_registry()
    compile_all(Reading)
    manager = SessionManager(
        pg_admin_uri,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=False,
        session_settings=_scope_settings,
        store=DEFAULT_STORE,
    )
    async with manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().drop_all)
        await conn.run_sync(get_metadata().create_all)
    repo: SQLAlchemyRepository[Reading, int] = SQLAlchemyRepository(
        session_manager=manager, model=Reading, cursor_codec=CursorCodec(signing="ephemeral")
    )
    stamps = [_START + timedelta(minutes=minutes) for minutes in (5, 1, 3, 1, 4)]
    await repo.create_many([CreateReading(taken_at=stamp) for stamp in stamps])
    try:
        yield repo
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await manager.close()
        reset_registry()


async def _page(
    repository: SQLAlchemyRepository[Reading, int], cursor: str | None
) -> CursorResult[Reading]:
    page = await repository.list_with_query(
        make(
            QuerySpec,
            sort=_QUERY.sort,
            pagination=_QUERY.pagination,
            limit=_QUERY.limit,
            cursor=cursor,
        )
    )
    assert isinstance(page, CursorResult)
    return page


_AT = {"type": "datetime", "v": "2026-09-06T12:01:00+00:00"}


@pytest.fixture
async def cached(
    repository: SQLAlchemyRepository[Reading, int],
) -> AsyncIterator[CachedRepository[Reading, Any, Any, int]]:
    CacheGateway.configure(
        {
            "default": {"cache": "aiocache.SimpleMemoryCache"},
            "cursor_scope_data": {
                "cache": "aiocache.SimpleMemoryCache",
                "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
                "namespace": "cursor_scope_data",
            },
            "cursor_scope_counters": {
                "cache": "aiocache.SimpleMemoryCache",
                "namespace": "cursor_scope_counters",
            },
        }
    )
    data, counters = (
        CacheGateway(alias="cursor_scope_data"),
        CacheGateway(alias="cursor_scope_counters"),
    )
    config = CacheConfig(
        enabled=True,
        aiocache_alias="cursor_scope_data",
        counter_alias="cursor_scope_counters",
        default_ttl=120,
        default_list_ttl=60,
    )
    try:
        yield CachedRepository(
            repository=cast(Repository[Reading, Any, Any, int], repository),
            config=config,
            cache=cast(CacheStore, data),
            dependency_resolver=GenerationalDependencyResolver(cast(CacheStore, counters)),
        )
    finally:
        await data.clear()
        await counters.clear()
        await data.close()
        await counters.close()


class TestCursorBinding:
    async def test_a_naive_timestamp_sort_walks_every_page(
        self,
        repository: SQLAlchemyRepository[Reading, int],
    ) -> None:
        seen: list[datetime] = []
        cursor: str | None = None
        while True:
            page = await _page(repository, cursor)
            seen.extend(item.taken_at for item in page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

        assert seen == sorted(_START + timedelta(minutes=m) for m in (5, 1, 3, 1, 4))

    async def test_a_cursor_issued_for_one_tenant_is_refused_for_another(
        self,
        repository: SQLAlchemyRepository[Reading, int],
    ) -> None:
        issued = _tenant.set("tenant-a")
        try:
            cursor = (await _page(repository, None)).next_cursor
        finally:
            _tenant.reset(issued)
        assert cursor is not None
        replayed = _tenant.set("tenant-b")

        try:
            with pytest.raises(UnsupportedQuery):
                await _page(repository, cursor)
        finally:
            _tenant.reset(replayed)

    @pytest.mark.parametrize(
        "position",
        [{"k": [_AT], "id": "1"}, {"k": ["2026-09-06T12:01:00+00:00"], "id": 1}],
        ids=["text-for-the-integer-key", "text-for-the-timestamp"],
    )
    async def test_a_forged_value_of_another_type_is_refused_before_the_query(
        self, repository: SQLAlchemyRepository[Reading, int], position: dict[str, object]
    ) -> None:
        unsigned: SQLAlchemyRepository[Reading, int] = SQLAlchemyRepository(
            session_manager=repository.session_manager,
            model=Reading,
            cursor_codec=CursorCodec(signing="unsigned"),
        )
        payload = msgspec.json.encode({"b": "sqlalchemy", **position})
        forged = base64.urlsafe_b64encode(payload).rstrip(b"=").decode()

        with pytest.raises(UnsupportedQuery):
            await unsigned.list_with_query(
                make(
                    QuerySpec,
                    sort=_QUERY.sort,
                    pagination=_QUERY.pagination,
                    limit=2,
                    cursor=forged,
                )
            )

    async def test_a_cursor_issued_for_one_sort_is_refused_for_another(
        self,
        repository: SQLAlchemyRepository[Reading, int],
    ) -> None:
        cursor = (await _page(repository, None)).next_cursor
        assert cursor is not None
        by_id = make(
            QuerySpec,
            sort=(SortSpec(field="id"),),
            pagination=PaginationMode.CURSOR,
            limit=2,
            cursor=cursor,
        )

        with pytest.raises(UnsupportedQuery):
            await repository.list_with_query(by_id)

    async def test_a_cached_first_page_is_not_served_to_another_scope(
        self,
        cached: CachedRepository[Reading, Any, Any, int],
    ) -> None:
        """Tokens are scope-bound, so the query cache key carries the scope too."""
        first_pages: dict[str, str | None] = {}
        for tenant in ("tenant-a", "tenant-b"):
            token = _tenant.set(tenant)
            try:
                page = await cached.list_with_query(_QUERY)
                assert isinstance(page, CursorResult)
                first_pages[tenant] = page.next_cursor
            finally:
                _tenant.reset(token)
        token = _tenant.set("tenant-b")

        try:
            second = await cached.list_with_query(
                make(
                    QuerySpec,
                    sort=_QUERY.sort,
                    pagination=_QUERY.pagination,
                    limit=_QUERY.limit,
                    cursor=first_pages["tenant-b"],
                )
            )
        finally:
            _tenant.reset(token)

        assert first_pages["tenant-a"] != first_pages["tenant-b"]
        assert isinstance(second, CursorResult)
        assert second.items
