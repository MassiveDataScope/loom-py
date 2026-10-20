"""Round-trip behaviour of the cached list/query read path.

A warm list index that has lost some of its entity entries must be refilled
with a bounded number of round trips: batched repository queries for the
missing ids, one cache read per batch of generation counters, and a write-back
limited to the entries that were actually missing.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Generic, NamedTuple
from uuid import UUID, uuid5

import pytest

from loom.core.cache import CacheConfig, CachedRepository, GenerationalDependencyResolver
from loom.core.cache._batching import COUNTER_BATCH_SIZE, REFILL_BATCH_SIZE
from loom.core.cache.keys import entity_key, stable_hash
from loom.core.repository import PageParams, PageResult
from loom.core.repository.abc.query import FilterOp, PaginationMode, QuerySpec
from loom.core.repository.mutation import MutationEvent

from ._doubles import (
    CodeWidget,
    CountingCacheBackend,
    CountingRepository,
    DateWidget,
    RestrictedFilterRepository,
    RowT,
    UuidWidget,
    Widget,
    WidgetCreate,
    WidgetUpdate,
)

PROFILE = "default"
ROW_COUNT = 5
MISSING_IDS = (2, 3, 5)
UUID_NAMESPACE = UUID("11111111-1111-1111-1111-111111111111")


class _Env(NamedTuple, Generic[RowT]):
    repository: CountingRepository[RowT]
    backend: CountingCacheBackend
    resolver: GenerationalDependencyResolver
    wrapper: CachedRepository[RowT, WidgetCreate, WidgetUpdate, object]


def _make_env(
    rows: Sequence[RowT],
    row_type: type[RowT],
    cache_config: CacheConfig,
    repository: CountingRepository[RowT] | None = None,
) -> _Env[RowT]:
    inner = repository if repository is not None else CountingRepository(rows, row_type)
    backend = CountingCacheBackend()
    resolver = GenerationalDependencyResolver(backend)
    wrapper: CachedRepository[RowT, WidgetCreate, WidgetUpdate, object] = CachedRepository(
        inner,
        config=cache_config,
        cache=backend,
        dependency_resolver=resolver,
    )
    return _Env(inner, backend, resolver, wrapper)


def _cursor_query(limit: int) -> QuerySpec:
    return QuerySpec(pagination=PaginationMode.CURSOR, limit=limit)


def _widgets(count: int) -> list[Widget]:
    return [Widget(id=index, name=f"w{index}") for index in range(1, count + 1)]


def _uuid_widgets(count: int) -> list[UuidWidget]:
    return [
        UuidWidget(id=uuid5(UUID_NAMESPACE, str(index)), name=f"w{index}")
        for index in range(1, count + 1)
    ]


def _date_widgets(count: int) -> list[DateWidget]:
    return [
        DateWidget(id=date(2020, 1, 1) + timedelta(days=index), name=f"w{index}")
        for index in range(1, count + 1)
    ]


def _code_widgets(count: int) -> list[CodeWidget]:
    return [CodeWidget(code=index, name=f"w{index}") for index in range(1, count + 1)]


@pytest.fixture
def env(cache_config: CacheConfig) -> _Env[Widget]:
    """Cached repository over counting doubles, seeded with five rows."""
    return _make_env(_widgets(ROW_COUNT), Widget, cache_config)


async def _entity_cache_key(env: _Env[RowT], obj_id: object) -> str:
    tags = env.resolver.entity_tags(env.wrapper.entity_name, obj_id)
    fingerprint = await env.resolver.fingerprint(tags)
    return entity_key(env.wrapper.entity_name, obj_id, PROFILE, fingerprint, scope="")


async def _warm_index(env: _Env[RowT], limit: int = ROW_COUNT) -> None:
    await env.wrapper.list_paginated(PageParams(page=1, limit=limit))
    env.repository.reset_counters()
    env.backend.reset_counters()


async def _evict_entities(env: _Env[RowT], ids: Sequence[object]) -> None:
    for obj_id in ids:
        del env.backend.data[await _entity_cache_key(env, obj_id)]
    env.backend.reset_counters()


async def _list_all(env: _Env[RowT], limit: int = ROW_COUNT) -> PageResult[RowT]:
    page = await env.wrapper.list_paginated(PageParams(page=1, limit=limit))
    assert isinstance(page, PageResult)
    return page


class TestPartiallyWarmIndex:
    """A warm index missing several entities refills them in one query."""

    @pytest.mark.asyncio
    async def test_refills_missing_entities_with_a_single_repository_query(
        self, env: _Env[Widget]
    ) -> None:
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        page = await _list_all(env)

        assert env.repository.list_with_query_calls == 1
        assert env.repository.get_by_id_calls == 0
        assert env.repository.list_paginated_calls == 0
        assert [(item.id, item.name) for item in page.items] == [
            (index, f"w{index}") for index in range(1, ROW_COUNT + 1)
        ]

    @pytest.mark.asyncio
    async def test_queries_the_missing_ids_with_a_single_in_filter(self, env: _Env[Widget]) -> None:
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        await _list_all(env)

        (query,) = env.repository.queries
        assert query.filters is not None
        assert query.limit == len(MISSING_IDS)
        (spec,) = query.filters.filters
        assert spec.field == "id"
        assert spec.op is FilterOp.IN
        assert set(spec.value) == set(MISSING_IDS)

    @pytest.mark.asyncio
    async def test_reads_entity_fingerprints_in_one_cache_round_trip(
        self, env: _Env[Widget]
    ) -> None:
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        await _list_all(env)

        # One round trip for the list index tags, one for the tags of all ids.
        assert env.backend.tag_multi_get_calls == 2

    @pytest.mark.asyncio
    async def test_fingerprint_round_trips_do_not_grow_with_the_index_size(
        self, cache_config: CacheConfig, env: _Env[Widget]
    ) -> None:
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)
        await _list_all(env)
        small = env.backend.tag_multi_get_calls

        larger = _make_env(_widgets(20), Widget, cache_config)
        await _warm_index(larger, limit=20)
        await _evict_entities(larger, range(1, 21, 2))
        await _list_all(larger, limit=20)

        assert larger.backend.tag_multi_get_calls == small

    @pytest.mark.asyncio
    async def test_writes_back_only_the_missing_entries(self, env: _Env[Widget]) -> None:
        await _warm_index(env)
        await _evict_entities(env, (3,))

        await _list_all(env)

        assert [len(batch) for batch in env.backend.multi_set_batches] == [1]


class TestNonPrimitivePrimaryKey:
    """A key msgpack renders as a string is restored to its declared type."""

    @pytest.mark.asyncio
    async def test_refills_uuid_ids_without_falling_back(self, cache_config: CacheConfig) -> None:
        rows = _uuid_widgets(ROW_COUNT)
        env = _make_env(rows, UuidWidget, cache_config)
        await _warm_index(env)
        await _evict_entities(env, [row.id for row in rows[1:4]])

        page = await _list_all(env)

        assert env.repository.list_with_query_calls == 1
        assert env.repository.list_paginated_calls == 0
        assert [(item.id, item.name) for item in page.items] == [(row.id, row.name) for row in rows]

    @pytest.mark.asyncio
    async def test_refills_date_ids_without_falling_back(self, cache_config: CacheConfig) -> None:
        rows = _date_widgets(ROW_COUNT)
        env = _make_env(rows, DateWidget, cache_config)
        await _warm_index(env)
        await _evict_entities(env, [row.id for row in rows[1:4]])

        page = await _list_all(env)

        assert env.repository.list_with_query_calls == 1
        assert env.repository.list_paginated_calls == 0
        assert [(item.id, item.name) for item in page.items] == [(row.id, row.name) for row in rows]

    @pytest.mark.asyncio
    async def test_queries_the_declared_id_type_not_its_string_form(
        self, cache_config: CacheConfig
    ) -> None:
        rows = _uuid_widgets(ROW_COUNT)
        env = _make_env(rows, UuidWidget, cache_config)
        await _warm_index(env)
        await _evict_entities(env, [row.id for row in rows[1:4]])

        await _list_all(env)

        (query,) = env.repository.queries
        assert query.filters is not None
        (spec,) = query.filters.filters
        assert all(isinstance(value, UUID) for value in spec.value)


class TestSingleMissingEntity:
    """One miss is cheaper through ``get_by_id`` than through a paginated query."""

    @pytest.mark.asyncio
    async def test_uses_get_by_id(self, env: _Env[Widget]) -> None:
        await _warm_index(env)
        await _evict_entities(env, (3,))

        page = await _list_all(env)

        assert env.repository.get_by_id_calls == 1
        assert env.repository.list_with_query_calls == 0
        assert env.repository.list_paginated_calls == 0
        assert [item.id for item in page.items] == [1, 2, 3, 4, 5]


class TestOversizedIndex:
    """An index wider than the repository's pagination limit still refills."""

    @pytest.mark.asyncio
    async def test_splits_the_refill_into_batches(self, cache_config: CacheConfig) -> None:
        row_count = REFILL_BATCH_SIZE * 2 + 1
        rows = _widgets(row_count)
        env = _make_env(rows, Widget, cache_config)
        # The index is written by the cursor path, which no backend limits.
        await env.wrapper.list_with_query(
            _cursor_query(row_count),
            profile=PROFILE,
        )
        env.repository.reset_counters()
        env.backend.reset_counters()
        await _evict_entities(env, [row.id for row in rows])

        result = await env.wrapper.list_with_query(_cursor_query(row_count), profile=PROFILE)

        assert env.repository.list_with_query_calls == 3
        assert [_in_filter_size(query) for query in env.repository.queries] == [
            REFILL_BATCH_SIZE,
            REFILL_BATCH_SIZE,
            1,
        ]
        # The cursor index was built from the double's reverse insertion order.
        assert [item.id for item in result.items] == [row.id for row in reversed(rows)]


def _in_filter_size(query: QuerySpec) -> int:
    assert query.filters is not None
    (spec,) = query.filters.filters
    return len(spec.value)


class TestRestrictedFilterFields:
    """A repository that forbids filtering on ``id`` is served per id."""

    @pytest.mark.asyncio
    async def test_falls_back_to_get_by_id(self, cache_config: CacheConfig) -> None:
        rows = _widgets(ROW_COUNT)
        env = _make_env(
            rows,
            Widget,
            cache_config,
            repository=RestrictedFilterRepository(rows, Widget),
        )
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        page = await _list_all(env)

        assert env.repository.get_by_id_calls == len(MISSING_IDS)
        assert env.repository.list_with_query_calls == 0
        assert [item.id for item in page.items] == [1, 2, 3, 4, 5]


class TestDeclaredPrimaryKeyName:
    """A model keyed by ``code`` is refilled and matched through ``code``."""

    @pytest.mark.asyncio
    async def test_refills_with_a_filter_on_the_declared_key(
        self, cache_config: CacheConfig
    ) -> None:
        rows = _code_widgets(ROW_COUNT)
        env = _make_env(rows, CodeWidget, cache_config)
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        page = await _list_all(env)

        (query,) = env.repository.queries
        assert query.filters is not None
        (spec,) = query.filters.filters
        assert (spec.field, spec.op, set(spec.value)) == ("code", FilterOp.IN, set(MISSING_IDS))
        assert env.repository.get_by_id_calls == 0
        assert [item.code for item in page.items] == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_allowlist_without_the_declared_key_is_served_per_id(
        self, cache_config: CacheConfig
    ) -> None:
        rows = _code_widgets(ROW_COUNT)
        env = _make_env(
            rows,
            CodeWidget,
            cache_config,
            repository=RestrictedFilterRepository(rows, CodeWidget),
        )
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        page = await _list_all(env)

        assert env.repository.get_by_id_calls == len(MISSING_IDS)
        assert env.repository.list_with_query_calls == 0
        assert [item.code for item in page.items] == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_allowlist_with_the_declared_key_is_refilled_in_one_query(
        self, cache_config: CacheConfig
    ) -> None:
        rows = _code_widgets(ROW_COUNT)
        env = _make_env(
            rows,
            CodeWidget,
            cache_config,
            repository=RestrictedFilterRepository(
                rows, CodeWidget, allowed_filter_fields=frozenset({"name", "code"})
            ),
        )
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        page = await _list_all(env)

        assert env.repository.list_with_query_calls == 1
        assert env.repository.get_by_id_calls == 0
        (query,) = env.repository.queries
        assert query.filters is not None
        (spec,) = query.filters.filters
        assert spec.field == "code"
        assert [item.code for item in page.items] == [1, 2, 3, 4, 5]


class TestStaleIndex:
    """An index pointing at a deleted row falls back to the repository."""

    @pytest.mark.asyncio
    async def test_missing_id_in_the_refill_query_triggers_the_list_fallback(
        self, env: _Env[Widget]
    ) -> None:
        await _warm_index(env)
        await _evict_entities(env, (3, 5))
        del env.repository.storage[3]

        page = await _list_all(env)

        assert env.repository.list_with_query_calls == 1
        assert env.repository.list_paginated_calls == 1
        assert [item.id for item in page.items] == [1, 2, 4, 5]


class TestFullyWarmIndex:
    """A fully warm index is served without touching the repository."""

    @pytest.mark.asyncio
    async def test_issues_no_repository_calls(self, env: _Env[Widget]) -> None:
        await _warm_index(env)

        page = await _list_all(env)

        assert env.repository.get_by_id_calls == 0
        assert env.repository.list_with_query_calls == 0
        assert env.repository.list_paginated_calls == 0
        assert [item.id for item in page.items] == [1, 2, 3, 4, 5]


class _PerGroupResolver:
    """Resolver implementing only the ``DependencyResolver`` members."""

    def __init__(self, delegate: GenerationalDependencyResolver) -> None:
        self._delegate = delegate

    async def fingerprint(self, tags: list[str]) -> str:
        return await self._delegate.fingerprint(tags)

    async def bump_from_events(self, events: tuple[MutationEvent, ...]) -> None:
        await self._delegate.bump_from_events(events)

    def entity_tags(self, entity: str, entity_id: object | None) -> list[str]:
        return self._delegate.entity_tags(entity, entity_id)

    def list_tags(self, entity: str, filter_fingerprint: str) -> list[str]:
        return self._delegate.list_tags(entity, filter_fingerprint)


class TestResolverWithoutBatchSupport:
    """A resolver that does not implement the batch protocol still works."""

    @pytest.mark.asyncio
    async def test_serves_the_page_with_per_group_fingerprints(
        self, cache_config: CacheConfig
    ) -> None:
        rows = _widgets(ROW_COUNT)
        repository = CountingRepository(rows, Widget)
        backend = CountingCacheBackend()
        batching = GenerationalDependencyResolver(backend)
        resolver = _PerGroupResolver(batching)
        wrapper: CachedRepository[Widget, WidgetCreate, WidgetUpdate, object] = CachedRepository(
            repository,
            config=cache_config,
            cache=backend,
            dependency_resolver=resolver,
        )
        env = _Env(repository, backend, batching, wrapper)
        await _warm_index(env)
        await _evict_entities(env, MISSING_IDS)

        page = await _list_all(env)

        assert [item.id for item in page.items] == [1, 2, 3, 4, 5]
        assert repository.list_with_query_calls == 1
        # One counter read per id, plus the list index header: the fallback.
        assert backend.tag_multi_get_calls == 1 + ROW_COUNT
        groups = [batching.entity_tags("widget", index) for index in range(1, ROW_COUNT + 1)]
        assert [await resolver.fingerprint(tags) for tags in groups] == (
            await batching.fingerprint_many(groups)
        )


class TestBatchFingerprint:
    """The batched fingerprint keeps the per-tag-set composite hash."""

    @pytest.mark.asyncio
    async def test_matches_the_per_call_fingerprint(self) -> None:
        backend = CountingCacheBackend()
        resolver = GenerationalDependencyResolver(backend)
        await backend.incr("tag:widget", delta=2)
        await backend.incr("tag:widget:id:1", delta=3)
        groups = [
            ["widget", "widget:id:1"],
            ["widget", "widget:id:2"],
            # Two dependency specs on the same entity repeat its tags.
            ["widget", "widget:list", "widget", "widget:list", "widget:id:1"],
            [],
        ]

        batched = await resolver.fingerprint_many(groups)

        assert batched == [await resolver.fingerprint(tags) for tags in groups]
        assert batched == [
            stable_hash("2|3"),
            stable_hash("2|0"),
            stable_hash("2|0|2|0|3"),
            "0",
        ]

    @pytest.mark.asyncio
    async def test_reads_every_group_in_one_round_trip(self) -> None:
        backend = CountingCacheBackend()
        resolver = GenerationalDependencyResolver(backend)

        await resolver.fingerprint_many([["widget", f"widget:id:{index}"] for index in range(10)])

        assert backend.tag_multi_get_calls == 1

    @pytest.mark.asyncio
    async def test_splits_a_wide_tag_set_into_batches(self) -> None:
        backend = CountingCacheBackend()
        resolver = GenerationalDependencyResolver(backend)
        groups = [[f"widget:id:{index}"] for index in range(COUNTER_BATCH_SIZE + 1)]

        fingerprints = await resolver.fingerprint_many(groups)

        assert [len(batch) for batch in backend.multi_get_batches] == [COUNTER_BATCH_SIZE, 1]
        assert fingerprints == [stable_hash("0") for _ in groups]
