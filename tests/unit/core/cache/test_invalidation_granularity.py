"""A write to one row evicts that row, every list and every dependent; nothing else.

The generational resolver bumps ``entity:list`` and ``entity:id:<k>`` per id
on every event; it never bumps the bare entity tag.
Whether a key survived is judged by the repository call count, the only
observable the cache-aside path offers.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from loom.core.cache import CacheConfig
from loom.core.stores.repository import PageParams
from loom.core.stores.repository.query import QuerySpec

from ._doubles import (
    AcmeCacheWidget,
    CachedEnv,
    ParentRepository,
    WidgetCreate,
    WidgetUpdate,
    WritableRepository,
    rewrap_with_cache,
    wrap_with_cache,
)

ROW_A = 1
ROW_B = 2
LIST_TAG_KEY = "tag:widget:list"

Write = Callable[[CachedEnv[AcmeCacheWidget]], Awaitable[object]]


async def _create(env: CachedEnv[AcmeCacheWidget]) -> object:
    return await env.wrapper.create(WidgetCreate(name="c"))


async def _update(env: CachedEnv[AcmeCacheWidget]) -> object:
    return await env.wrapper.update(ROW_A, WidgetUpdate(name="a-updated"))


async def _delete(env: CachedEnv[AcmeCacheWidget]) -> object:
    return await env.wrapper.delete(ROW_A)


every_write = pytest.mark.parametrize(
    "write", [_create, _update, _delete], ids=["create", "update", "delete"]
)


@pytest.fixture
def env(cache_config: CacheConfig) -> CachedEnv[AcmeCacheWidget]:
    rows = [AcmeCacheWidget(id=ROW_A, name="a"), AcmeCacheWidget(id=ROW_B, name="b")]
    return wrap_with_cache(WritableRepository(rows, AcmeCacheWidget), cache_config)


def _writable(env: CachedEnv[AcmeCacheWidget]) -> WritableRepository[AcmeCacheWidget]:
    """Narrow the env's repository to the writable double the fixture built."""
    assert isinstance(env.repository, WritableRepository)
    return env.repository


async def _warm_both_rows(env: CachedEnv[AcmeCacheWidget]) -> None:
    await env.wrapper.get_by_id(ROW_A)
    await env.wrapper.get_by_id(ROW_B)
    assert env.repository.get_by_id_calls == 2


class TestPerIdInvalidation:
    """AC5: a write to row A leaves the cached read of row B untouched."""

    @pytest.mark.asyncio
    async def test_update_evicts_only_the_updated_row(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        await _warm_both_rows(env)

        await env.wrapper.update(ROW_A, WidgetUpdate(name="a-updated"))

        other = await env.wrapper.get_by_id(ROW_B)
        assert other is not None
        assert env.repository.get_by_id_calls == 2
        updated = await env.wrapper.get_by_id(ROW_A)
        assert updated is not None
        assert updated.name == "a-updated"
        assert env.repository.get_by_id_calls == 3

    @pytest.mark.asyncio
    async def test_delete_evicts_only_the_deleted_row(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        await _warm_both_rows(env)

        assert await env.wrapper.delete(ROW_A) is True

        other = await env.wrapper.get_by_id(ROW_B)
        assert other is not None
        assert env.repository.get_by_id_calls == 2
        assert await env.wrapper.get_by_id(ROW_A) is None
        assert env.repository.get_by_id_calls == 3

    @pytest.mark.asyncio
    async def test_entity_scoped_read_of_another_row_stays_warm(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        assert await env.wrapper.note_count(ROW_A) == 1
        assert await env.wrapper.note_count(ROW_B) == 1
        assert env.repository.note_count_calls == 2

        await env.wrapper.update(ROW_B, WidgetUpdate(name="bbb"))

        assert await env.wrapper.note_count(ROW_A) == 1
        assert env.repository.note_count_calls == 2
        assert await env.wrapper.note_count(ROW_B) == 3
        assert env.repository.note_count_calls == 3


class TestListsAndDependentsDie:
    """AC6: every list index, list-scoped read and dependent read misses after a write."""

    @every_write
    @pytest.mark.asyncio
    async def test_every_list_read_misses_after_a_write(
        self, env: CachedEnv[AcmeCacheWidget], write: Write
    ) -> None:
        page = PageParams(page=1, limit=10)
        query = QuerySpec(page=1, limit=10)
        for _ in range(2):
            await env.wrapper.list_paginated(page)
            await env.wrapper.list_with_query(query)
            await env.wrapper.all_names()
        repository = _writable(env)
        assert repository.list_paginated_calls == 1
        assert repository.list_with_query_calls == 1
        assert repository.all_names_calls == 1

        await write(env)

        await env.wrapper.list_paginated(page)
        await env.wrapper.list_with_query(query)
        await env.wrapper.all_names()
        assert repository.list_paginated_calls == 2
        assert repository.list_with_query_calls == 2
        assert repository.all_names_calls == 2

    @every_write
    @pytest.mark.asyncio
    async def test_dependent_parent_read_misses_after_a_write(
        self, env: CachedEnv[AcmeCacheWidget], cache_config: CacheConfig, write: Write
    ) -> None:
        parent = ParentRepository([AcmeCacheWidget(id=7, name="p")], AcmeCacheWidget)
        parent_wrapper = rewrap_with_cache(env, parent, cache_config)
        await parent_wrapper.get_by_id(7)
        await parent_wrapper.get_by_id(7)
        assert parent.get_by_id_calls == 1

        await write(env)

        await parent_wrapper.get_by_id(7)
        assert parent.get_by_id_calls == 2


class TestBumpAccounting:
    """AC7/AC8 on the direct path: which counters a write increments."""

    @pytest.mark.asyncio
    async def test_create_bumps_the_list_and_id_tags_only(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        created = await env.wrapper.create(WidgetCreate(name="c"))

        assert set(env.backend.incr_keys) == {LIST_TAG_KEY, f"tag:widget:id:{created.id}"}
        assert len(env.backend.incr_keys) == 2

    @pytest.mark.asyncio
    async def test_create_many_bumps_the_list_once_and_every_id(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        created = await env.wrapper.create_many(
            [WidgetCreate(name="c"), WidgetCreate(name="d"), WidgetCreate(name="e")]
        )

        assert len(created) == 3
        assert set(env.backend.incr_keys) == {LIST_TAG_KEY} | {
            f"tag:widget:id:{row.id}" for row in created
        }
        assert len(env.backend.incr_keys) == 4

    @pytest.mark.asyncio
    async def test_update_bumps_the_list_and_id_tags_only(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        await env.wrapper.update(ROW_A, WidgetUpdate(name="a-updated"))

        assert sorted(env.backend.incr_keys) == [f"tag:widget:id:{ROW_A}", LIST_TAG_KEY]

    @pytest.mark.asyncio
    async def test_delete_bumps_the_list_and_id_tags_only(
        self, env: CachedEnv[AcmeCacheWidget]
    ) -> None:
        await env.wrapper.delete(ROW_A)

        assert sorted(env.backend.incr_keys) == [f"tag:widget:id:{ROW_A}", LIST_TAG_KEY]
