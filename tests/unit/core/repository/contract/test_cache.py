"""Cache layer parity (T205): ``CachedRepository`` over every backend."""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypeVar

from loom.core.cache import CacheConfig, CachedRepository, GenerationalDependencyResolver
from loom.core.stores.repository import BulkCreatable, Listable, PageParams
from loom.testing import make

from .conftest import SEED, BackendCase, CreateOrder, OrderStatus, UpdateOrder, require

T = TypeVar("T")

_FIRST_PAGE = make(PageParams, page=1, limit=20)


class _MemoryCache:
    """Dict-backed :class:`~loom.core.cache.abc.backend.CacheBackend`.

    Values are stored as given, so ``type`` is not needed to rebuild them.
    """

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        return self.data.get(key)

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        self.data[key] = value

    async def multi_get_values(
        self, keys: list[str], *, type: type[T] | None = None
    ) -> list[T | Any | None]:
        return [self.data.get(key) for key in keys]

    async def multi_set_values(self, pairs: list[tuple[str, Any]], ttl: int | None = None) -> None:
        self.data.update(pairs)

    async def exists(self, key: str) -> bool:
        return key in self.data

    async def delete(self, key: str) -> int:
        return 0 if self.data.pop(key, None) is None else 1

    async def delete_many(self, keys: list[str]) -> int:
        return sum([await self.delete(key) for key in keys])

    async def incr(self, key: str, delta: int = 1) -> int:
        self.data[key] = int(self.data.get(key) or 0) + delta
        return self.data[key]

    async def close(self) -> None:
        return None


def _cached(repository: Any) -> tuple[CachedRepository[Any, Any, Any, Any], _MemoryCache]:
    cache = _MemoryCache()
    wrapped = CachedRepository(
        repository,
        config=CacheConfig(enabled=True, default_ttl=100, default_list_ttl=50),
        cache=cache,
        dependency_resolver=GenerationalDependencyResolver(cache),
    )
    return wrapped, cache


class TestContractCache:
    async def test_cache_hit_returns_the_typed_output_struct(
        self, case: BackendCase, seeded: Any
    ) -> None:
        cached, _ = _cached(seeded)
        first = await cached.get_by_id(SEED[0].id)
        # A write behind the cache is not observed: the second read is a hit.
        await seeded.update(SEED[0].id, UpdateOrder(amount=999))

        second = await cached.get_by_id(SEED[0].id)

        assert type(second) is case.model
        assert second == first
        assert second is not None
        assert second.amount == SEED[0].amount

    async def test_entity_keys_are_namespaced_by_the_model_table(
        self, case: BackendCase, seeded: Any
    ) -> None:
        cached, cache = _cached(seeded)

        await cached.get_by_id(SEED[0].id)

        assert cached.entity_name == case.model.__tablename__
        assert cached.entity_name != type(seeded).__name__.lower()
        entity_keys = [key for key in cache.data if f":{SEED[0].id}:" in key]
        assert entity_keys
        assert all(key.startswith(f"{case.model.__tablename__}:") for key in entity_keys)

    async def test_create_many_invalidates_a_cached_first_page(
        self, case: BackendCase, seeded: Any
    ) -> None:
        require(case, BulkCreatable, Listable)
        cached, _ = _cached(seeded)
        before = await cached.list_paginated(_FIRST_PAGE)
        assert len(before.items) == len(SEED)
        stamp = datetime(2026, 2, 1, 12, 0)
        extra = (
            CreateOrder(id=9, customer="gus", amount=5, status=OrderStatus.PAID, created_at=stamp),
            CreateOrder(id=10, customer="hal", amount=5, status=OrderStatus.PAID, created_at=stamp),
        )

        await cached.create_many(extra)

        after = await cached.list_paginated(_FIRST_PAGE)
        assert {row.id for row in after.items} == {row.id for row in SEED} | {9, 10}
        assert after.total_count == len(SEED) + 2
