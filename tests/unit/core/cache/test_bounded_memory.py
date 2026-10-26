"""``BoundedMemoryCache`` and the ``apply_config`` switch that installs it."""

from __future__ import annotations

import asyncio

import pytest
from aiocache import caches

from loom.core.cache import BoundedMemoryCache, CacheConfig, CacheGateway
from loom.core.cache.serializer import MsgspecSerializer
from loom.core.model.redaction import RedactedMap

pytestmark = pytest.mark.asyncio


def _payload(size: int) -> bytes:
    return b"x" * size


class TestByteBound:
    async def test_the_least_recently_used_entry_goes_first(self) -> None:
        cache = BoundedMemoryCache(max_bytes=30)
        await cache.set("a", _payload(10))
        await cache.set("b", _payload(10))
        await cache.set("c", _payload(10))
        await cache.get("a")  # a becomes the most recent; b is now the oldest
        await cache.set("d", _payload(10))
        assert await cache.get("b") is None
        assert await cache.get("a") == _payload(10)
        assert await cache.get("c") == _payload(10)
        assert await cache.get("d") == _payload(10)
        assert cache.bytes == 30
        assert cache.evictions == 1

    async def test_overwriting_a_key_counts_only_the_new_payload(self) -> None:
        cache = BoundedMemoryCache(max_bytes=100)
        await cache.set("a", _payload(40))
        await cache.set("a", _payload(10))
        assert cache.bytes == 10
        assert cache.evictions == 0

    async def test_a_value_over_the_whole_bound_is_kept_alone(self) -> None:
        cache = BoundedMemoryCache(max_bytes=10)
        await cache.set("small", _payload(5))
        await cache.set("huge", _payload(50))
        assert await cache.get("huge") == _payload(50)
        assert await cache.get("small") is None
        assert cache.bytes == 50

    async def test_delete_and_clear_release_bytes(self) -> None:
        cache = BoundedMemoryCache(max_bytes=100)
        await cache.set("a", _payload(10))
        await cache.set("b", _payload(10))
        await cache.delete("a")
        assert cache.bytes == 10
        await cache.clear()
        assert cache.bytes == 0
        assert await cache.get("b") is None

    async def test_a_raw_value_mutated_after_set_is_refunded_what_it_was_charged(self) -> None:
        # Default serializer is NullSerializer: the cache keeps the caller's object.
        cache = BoundedMemoryCache(max_bytes=10_000)
        payload: dict[str, int] = {}
        await cache.set("k", payload)
        charged = cache.bytes
        payload.update({str(index): index for index in range(100)})
        await cache.delete("k")
        assert (charged, cache.bytes) == (charged, 0)

    async def test_multi_set_evicts_within_the_batch(self) -> None:
        cache = BoundedMemoryCache(max_size=2)
        await cache.multi_set([("a", 1), ("b", 2), ("c", 3)])
        assert await cache.multi_get(["a", "b", "c"]) == [None, 2, 3]

    async def test_add_is_bounded_and_still_refuses_an_existing_key(self) -> None:
        cache = BoundedMemoryCache(max_size=1)
        await cache.add("a", 1)
        await cache.add("b", 2)
        assert await cache.get("a") is None
        with pytest.raises(ValueError, match="already exists"):
            await cache.add("b", 3)

    async def test_clear_with_a_namespace_keeps_the_ledger_and_the_rest(self) -> None:
        cache = BoundedMemoryCache(max_bytes=100)
        await cache.set("ns:a", _payload(10))
        await cache.set("other", _payload(10))
        await cache.clear(namespace="ns")
        assert cache.bytes == 10
        await cache.set("ns:b", _payload(10))
        assert cache.bytes == 20

    async def test_expire_then_eviction_leaves_no_timer_behind(self) -> None:
        cache = BoundedMemoryCache(max_size=1)
        await cache.set("a", 1)
        await cache.expire("a", 10)
        await cache.set("b", 2)
        assert "a" not in cache._handlers

    async def test_a_ttl_expiry_releases_bytes_and_an_eviction_cancels_the_timer(self) -> None:
        cache = BoundedMemoryCache(max_bytes=15)
        await cache.set("a", _payload(10), ttl=0.01)
        await cache.set("b", _payload(10), ttl=10)  # evicts a, whose timer must go
        assert "a" not in cache._handlers
        await asyncio.sleep(0.03)
        assert cache.bytes == 10
        await cache.set("c", _payload(5), ttl=0.01)
        await asyncio.sleep(0.03)
        assert cache.bytes == 10
        assert await cache.get("c") is None


class TestEntryBound:
    async def test_entries_past_max_size_are_evicted_lru(self) -> None:
        cache = BoundedMemoryCache(max_size=2)
        await cache.set("a", 1)
        await cache.set("b", 2)
        await cache.get("a")
        await cache.set("c", 3)
        assert await cache.get("b") is None
        assert await cache.get("a") == 1
        assert await cache.get("c") == 3

    async def test_increment_is_bounded_too(self) -> None:
        cache = BoundedMemoryCache(max_size=1)
        await cache.increment("first")
        await cache.increment("second")
        assert await cache.get("first") is None
        assert await cache.get("second") == 1

    async def test_bounds_below_one_are_refused(self) -> None:
        with pytest.raises(ValueError, match="max_size"):
            BoundedMemoryCache(max_size=0)
        with pytest.raises(ValueError, match="max_bytes"):
            BoundedMemoryCache(max_bytes=-1)


class TestApplyConfig:
    def teardown_method(self) -> None:
        caches._caches.clear()
        caches._config = {"default": {"cache": "aiocache.SimpleMemoryCache"}}

    async def test_a_memory_alias_with_a_bound_builds_and_evicts(self) -> None:
        # Regression: loom 2.1.3 injected ``max_size`` into ``SimpleMemoryCache``,
        # which does not accept it, so ``caches.get`` raised ``TypeError``.
        config = CacheConfig(
            aiocache_alias="data",
            max_size=2,
            max_bytes=1024,
            aiocache_config=RedactedMap(
                {
                    "data": {
                        "cache": "aiocache.SimpleMemoryCache",
                        "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
                    }
                }
            ),
        )
        CacheGateway.apply_config(config)
        gateway = CacheGateway(alias="data")
        backend = gateway._cache
        assert isinstance(backend, BoundedMemoryCache)
        assert isinstance(backend.serializer, MsgspecSerializer)
        assert (backend.max_size, backend.max_bytes) == (2, 1024)
        for index in range(3):
            await gateway.set_value(f"k{index}", {"index": index})
        assert await gateway.get_value("k0") is None
        assert await gateway.get_value("k2") == {"index": 2}

    @pytest.mark.parametrize("global_bound", [None, 100])
    def test_an_alias_declaring_its_own_bound_keeps_it(self, global_bound: int | None) -> None:
        config = CacheConfig(
            aiocache_alias="data",
            max_size=global_bound,
            aiocache_config=RedactedMap(
                {"data": {"cache": "aiocache.SimpleMemoryCache", "max_size": 5}}
            ),
        )
        CacheGateway.apply_config(config)
        backend = CacheGateway(alias="data")._cache
        assert isinstance(backend, BoundedMemoryCache)
        assert backend.max_size == 5

    def test_without_a_bound_the_plain_class_is_kept(self) -> None:
        config = CacheConfig(
            aiocache_alias="data",
            aiocache_config=RedactedMap({"data": {"cache": "aiocache.SimpleMemoryCache"}}),
        )
        CacheGateway.apply_config(config)
        assert type(CacheGateway(alias="data")._cache).__name__ == "SimpleMemoryCache"

    def test_a_redis_alias_is_forwarded_unchanged(self) -> None:
        redis = {"cache": "aiocache.RedisCache", "endpoint": "redis", "port": 6379}
        config = CacheConfig(
            aiocache_alias="data",
            max_bytes=1024,
            aiocache_config=RedactedMap({"data": dict(redis)}),
        )
        CacheGateway.apply_config(config)
        assert caches.get_config()["data"] == redis

    def test_from_mapping_reads_max_bytes(self) -> None:
        config = CacheConfig.from_mapping({"max_bytes": "2048"})
        assert config.max_bytes == 2048

    def test_config_refuses_bounds_below_one(self) -> None:
        with pytest.raises(ValueError, match="max_bytes"):
            CacheConfig(max_bytes=0)
