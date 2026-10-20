from __future__ import annotations

import importlib
from collections.abc import AsyncGenerator, Sequence
from datetime import date, timedelta
from typing import Any, NamedTuple, cast
from unittest.mock import AsyncMock, patch

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.cache import (
    CacheBackend,
    CacheConfig,
    CachedRepository,
    CacheGateway,
    GenerationalDependencyResolver,
)
from loom.core.cache.keys import entity_key
from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField
from loom.core.repository import Repository
from loom.core.repository.abc import CursorCodec, PageParams
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import transactional
from loom.testing import RepositoryIntegrationHarness, ScenarioDict
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.review.schemas import CreateProductReview
from tests.integration.fake_repo.product.schemas import CreateProduct, UpdateProduct


class _Gateways(NamedTuple):
    # ``CacheGateway`` overloads ``get_value`` while ``CacheBackend`` declares it
    # once, so a type checker does not see the gateway as a structural match.
    # It is one at runtime; the cast keeps the fixture honest about that.
    data: CacheBackend
    counters: CacheBackend
    config: CacheConfig


class DatedEvent(BaseModel):
    """Model whose primary key msgpack renders as a string."""

    __tablename__ = "dated_events"

    id: date = ColumnField(primary_key=True)
    label: str = ColumnField(length=32)


class CreateDatedEvent(Command, frozen=True):
    """Creation payload for :class:`DatedEvent`."""

    id: date
    label: str


class UpdateDatedEvent(Command, frozen=True):
    """Update payload for :class:`DatedEvent`."""

    label: str


@pytest.fixture
async def cache_gateways(
    cache_backend_kind: str,
) -> AsyncGenerator[_Gateways, None]:
    namespace = "integration_cache"
    counter_namespace = "integration_cache_counters"

    # "test_cache"    — data backend with MsgspecSerializer (entity / list storage)
    # "test_counters" — counter backend without serializer (native atomic increment)
    aiocache_config: dict[str, object] = {
        "default": {
            "cache": "aiocache.SimpleMemoryCache",
            "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
            "namespace": "integration_default_cache",
        },
        "test_cache": {
            "cache": "aiocache.SimpleMemoryCache",
            "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
            "namespace": namespace,
        },
        "test_counters": {
            "cache": "aiocache.SimpleMemoryCache",
            # No serializer — raw integer storage, asyncio-safe increment
            "namespace": counter_namespace,
        },
    }

    if cache_backend_kind == "redis-fake":
        fakeredis_async = importlib.import_module("fakeredis.aioredis")
        fake_connection = fakeredis_async.FakeConnection
        aiocache_config["test_cache"] = {
            "cache": "aiocache.RedisCache",
            "serializer": {"class": "loom.core.cache.serializer.MsgspecSerializer"},
            "endpoint": "127.0.0.1",
            "port": 6379,
            "db": 0,
            "namespace": namespace,
            "connection_pool_kwargs": {"connection_class": fake_connection},
        }
        aiocache_config["test_counters"] = {
            "cache": "aiocache.RedisCache",
            # No serializer — atomic Redis INCR
            "endpoint": "127.0.0.1",
            "port": 6379,
            "db": 0,
            "namespace": counter_namespace,
            "connection_pool_kwargs": {"connection_class": fake_connection},
        }

    CacheGateway.configure(aiocache_config)

    data_gateway = CacheGateway(alias="test_cache")
    counter_gateway = CacheGateway(alias="test_counters")
    cache_config = CacheConfig(
        enabled=True,
        aiocache_alias="test_cache",
        counter_alias="test_counters",
        default_ttl=120,
        default_list_ttl=60,
    )
    try:
        yield _Gateways(
            cast(CacheBackend, data_gateway),
            cast(CacheBackend, counter_gateway),
            cache_config,
        )
    finally:
        await data_gateway.clear()
        await counter_gateway.clear()
        await data_gateway.close()
        await counter_gateway.close()


@pytest.fixture
def cached_integration_repo(
    integration_context: RepositoryIntegrationHarness,
    cache_gateways: _Gateways,
) -> CachedRepository[Product, CreateProduct, UpdateProduct, int]:
    return CachedRepository(
        repository=cast(
            Repository[Product, CreateProduct, UpdateProduct, int],
            integration_context.product.repository,
        ),
        config=cache_gateways.config,
        cache=cache_gateways.data,
        dependency_resolver=GenerationalDependencyResolver(cache_gateways.counters),
    )


@pytest.fixture
async def cached_dated_repo(
    integration_context: RepositoryIntegrationHarness,
    cache_gateways: _Gateways,
) -> CachedRepository[DatedEvent, CreateDatedEvent, UpdateDatedEvent, date]:
    compile_all(DatedEvent)
    compiled = get_compiled(DatedEvent)
    assert compiled is not None
    async with integration_context.session_manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    repository: RepositorySQLAlchemy[DatedEvent, date] = RepositorySQLAlchemy(
        session_manager=integration_context.session_manager,
        model=DatedEvent,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    return CachedRepository(
        repository=cast(
            Repository[DatedEvent, CreateDatedEvent, UpdateDatedEvent, date], repository
        ),
        config=cache_gateways.config,
        cache=cache_gateways.data,
        dependency_resolver=GenerationalDependencyResolver(cache_gateways.counters),
    )


class ProductService:
    """``@transactional`` owner whose writes go through the cached wrapper.

    The wrapper is an instance attribute because ``@transactional`` finds the
    post-commit hooks in ``vars(owner)``; the owner is not a repository, and
    it shares the session manager of the product repository so the writes
    join the transaction it opens.
    """

    def __init__(
        self,
        session_manager: SessionManager,
        products: CachedRepository[Product, CreateProduct, UpdateProduct, int],
    ) -> None:
        self.session_manager = session_manager
        self.products = products

    @transactional
    async def create(self, data: CreateProduct) -> Product:
        return await self.products.create(data)

    @transactional
    async def rename(self, product_id: int, name: str) -> Product | None:
        return await self.products.update(product_id, UpdateProduct(name=name))

    @transactional
    async def remove(self, product_id: int) -> bool:
        return await self.products.delete(product_id)


@pytest.fixture
def product_service(
    integration_context: RepositoryIntegrationHarness,
    cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
) -> ProductService:
    return ProductService(integration_context.session_manager, cached_integration_repo)


@pytest.fixture(params=["memory", "redis-fake"])
def cache_backend_kind(request: Any) -> str:
    return cast(str, request.param)


def _spy_base_repo_method(
    cached_repo: CachedRepository[Any, Any, Any, Any],
    method_name: str,
) -> Any:
    """Patch *method_name* on the wrapped repository with a call-counting spy."""
    base_repo = cached_repo._repository
    original = getattr(base_repo, method_name)
    mocked = AsyncMock(wraps=original)
    return patch.object(base_repo, method_name, mocked)


class TestCacheIntegration:
    @pytest.mark.asyncio
    async def test_get_by_id_cached_and_invalidated_on_update(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product: ScenarioDict,
    ) -> None:
        # Arrange
        await integration_context.load(scenario_one_product)
        product_id = 1

        # Act / Assert
        with _spy_base_repo_method(cached_integration_repo, "get_by_id") as get_by_id_spy:
            first = await cached_integration_repo.get_by_id(product_id)
            second = await cached_integration_repo.get_by_id(product_id)

            assert first is not None
            assert second is not None
            assert first.name == "seed"
            assert second.name == "seed"
            assert get_by_id_spy.await_count == 1

            updated = await cached_integration_repo.update(
                product_id, UpdateProduct(name="seed-updated")
            )
            assert updated is not None

            after_update = await cached_integration_repo.get_by_id(product_id)
            assert after_update is not None
            assert after_update.name == "seed-updated"
            assert get_by_id_spy.await_count == 2

    @pytest.mark.asyncio
    async def test_list_paginated_cache_hit_and_invalidation_on_create(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        # Arrange
        await integration_context.load(scenario_catalog_with_price_20)
        page = PageParams(page=1, limit=2)

        # Act / Assert
        with _spy_base_repo_method(cached_integration_repo, "list_paginated") as list_spy:
            first = await cached_integration_repo.list_paginated(page)
            second = await cached_integration_repo.list_paginated(page)

            assert first.total_count == 3
            assert second.total_count == 3
            assert list_spy.await_count == 1

            _ = await cached_integration_repo.create(CreateProduct(name="d", price=40.0))
            third = await cached_integration_repo.list_paginated(page)

            assert third.total_count == 4
            assert list_spy.await_count == 2

    @pytest.mark.asyncio
    async def test_delete_invalidates_cached_entity(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product: ScenarioDict,
    ) -> None:
        # Arrange
        await integration_context.load(scenario_one_product)
        product_id = 1

        # Act / Assert
        with _spy_base_repo_method(cached_integration_repo, "get_by_id") as get_by_id_spy:
            _ = await cached_integration_repo.get_by_id(product_id)
            _ = await cached_integration_repo.get_by_id(product_id)

            deleted = await cached_integration_repo.delete(product_id)
            after_delete = await cached_integration_repo.get_by_id(product_id)

            assert deleted is True
            assert after_delete is None
            assert get_by_id_spy.await_count == 2


class TestCachedIndexRefillIntegration:
    """A warm index that lost entity entries is refilled with one query."""

    @staticmethod
    async def _evict_entities(
        cached_repo: CachedRepository[Any, Any, Any, Any],
        ids: Sequence[object],
    ) -> None:
        resolver = cached_repo._resolver
        cache = cached_repo._cache
        for entity_id in ids:
            tags = resolver.entity_tags(cached_repo.entity_name, entity_id)
            tags.extend(cached_repo._entity_dependency_tags(entity_id))
            fingerprint = await resolver.fingerprint(tags)
            await cache.delete(
                entity_key(cached_repo.entity_name, entity_id, "default", fingerprint, scope="")
            )

    @pytest.mark.asyncio
    async def test_partially_warm_index_reloads_the_missing_ids_with_one_query(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)
        page = PageParams(page=1, limit=3)
        warm = await cached_integration_repo.list_paginated(page)
        assert [item.id for item in warm.items] == [1, 2, 3]
        await self._evict_entities(cached_integration_repo, (1, 3))

        with (
            _spy_base_repo_method(cached_integration_repo, "list_with_query") as query_spy,
            _spy_base_repo_method(cached_integration_repo, "list_paginated") as list_spy,
            _spy_base_repo_method(cached_integration_repo, "get_by_id") as get_spy,
        ):
            refilled = await cached_integration_repo.list_paginated(page)

            assert query_spy.await_count == 1
            assert list_spy.await_count == 0
            assert get_spy.await_count == 0

        assert [(item.id, item.name) for item in refilled.items] == [
            (item.id, item.name) for item in warm.items
        ]

    @pytest.mark.asyncio
    async def test_refills_a_date_primary_key_that_msgpack_degraded_to_a_string(
        self,
        cached_dated_repo: CachedRepository[DatedEvent, CreateDatedEvent, UpdateDatedEvent, date],
    ) -> None:
        days = [date(2020, 1, 1) + timedelta(days=offset) for offset in range(3)]
        for offset, day in enumerate(days):
            await cached_dated_repo.create(CreateDatedEvent(id=day, label=f"e{offset}"))
        page = PageParams(page=1, limit=3)
        warm = await cached_dated_repo.list_paginated(page)
        assert [item.id for item in warm.items] == days
        await self._evict_entities(cached_dated_repo, days[:2])

        with (
            _spy_base_repo_method(cached_dated_repo, "list_with_query") as query_spy,
            _spy_base_repo_method(cached_dated_repo, "list_paginated") as list_spy,
        ):
            refilled = await cached_dated_repo.list_paginated(page)

            assert query_spy.await_count == 1
            assert list_spy.await_count == 0

        assert [(item.id, item.label) for item in refilled.items] == [
            (item.id, item.label) for item in warm.items
        ]


class TestRelatedInvalidationIntegration:
    @pytest.mark.asyncio
    async def test_with_details_profile_is_invalidated_from_review_repository(
        self,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_one_product)
        product_id = 1

        first = await cached_integration_repo.get_by_id(product_id, profile="with_details")
        assert first is not None
        assert first.count_reviews == 0
        assert first.has_reviews is False

        await integration_context.review.repository.create(
            CreateProductReview(product_id=product_id, rating=5, comment="awesome")
        )
        await cached_integration_repo.on_transaction_committed(
            (
                MutationEvent(
                    entity="product_reviews",
                    op="create",
                    ids=(1,),
                    tags=frozenset(
                        {
                            f"product_reviews:product_id:{product_id}",
                            "product_reviews:list",
                        }
                    ),
                ),
            )
        )
        second = await cached_integration_repo.get_by_id(product_id, profile="with_details")
        assert second is not None
        assert second.count_reviews == 1
        assert second.has_reviews is True
        assert {item["comment"] for item in second.review_snippets} == {"awesome"}


class TestTransactionalGranularityIntegration:
    """The mixins' post-commit events evict one row, not the whole entity."""

    @pytest.mark.asyncio
    async def test_update_inside_a_transaction_evicts_only_its_row(
        self,
        product_service: ProductService,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        integration_context: RepositoryIntegrationHarness,
        scenario_two_products: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_two_products)

        with _spy_base_repo_method(cached_integration_repo, "get_by_id") as get_by_id_spy:
            _ = await cached_integration_repo.get_by_id(1)
            _ = await cached_integration_repo.get_by_id(2)
            assert get_by_id_spy.await_count == 2

            await product_service.rename(1, "one-renamed")

            other = await cached_integration_repo.get_by_id(2)
            assert other is not None
            assert other.name == "two"
            assert get_by_id_spy.await_count == 2

            renamed = await cached_integration_repo.get_by_id(1)
            assert renamed is not None
            assert renamed.name == "one-renamed"
            assert get_by_id_spy.await_count == 3

    @pytest.mark.asyncio
    async def test_a_full_cycle_never_bumps_the_bare_entity_tag(
        self,
        product_service: ProductService,
        cached_integration_repo: CachedRepository[Product, CreateProduct, UpdateProduct, int],
        cache_gateways: _Gateways,
    ) -> None:
        created = await product_service.create(CreateProduct(name="cycle", price=1.0))
        renamed = await product_service.rename(created.id, "cycle-renamed")
        removed = await product_service.remove(created.id)
        assert renamed is not None
        assert removed is True

        entity = cached_integration_repo.entity_name
        counters = cache_gateways.counters
        assert await counters.exists(f"tag:{entity}") is False
        assert await counters.exists(f"tag:{entity}:list") is True
        assert await counters.exists(f"tag:{entity}:id:{created.id}") is True
