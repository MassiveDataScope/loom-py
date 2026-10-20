from __future__ import annotations

import inspect
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any, TypeVar, cast
from uuid import uuid4

import msgspec
import pytest

from loom.core.cache import (
    CacheBackend,
    CacheConfig,
    CachedRepository,
    GenerationalDependencyResolver,
    cache_query,
    cached,
)
from loom.core.cache.keys import entity_key
from loom.core.cache.repository import _DELEGATED_METHODS
from loom.core.engine.post_commit import (
    PostCommitChannel,
    PostCommitError,
    active_channel,
    bind_channel,
    reset_channel,
)
from loom.core.model import BaseModel, Cardinality, ColumnField, ProjectionField, RelationField
from loom.core.repository import FilterParams, PageParams, PageResult, Repository
from loom.core.repository.abc.query import (
    CursorResult,
    PaginationMode,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction

from ._doubles import (
    CachedEnv,
    CodeWidget,
    CountingCacheBackend,
    CountingRepository,
    DateWidget,
    UuidWidget,
    Widget,
    WidgetCreate,
    WidgetUpdate,
    WritableRepository,
    open_transaction_with_channel,
    wrap_with_cache,
)

T = TypeVar("T")


class _MemoryCacheBackend:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def get(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        value = self.data.get(key)
        if value is None or type is None:
            return value
        if isinstance(value, dict):
            return type(**value)
        return value

    async def get_value(self, key: str, *, type: type[T] | None = None) -> T | Any | None:
        return await self.get(key, type=type)

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        _ = ttl
        self.data[key] = value

    async def set_value(self, key: str, value: Any, ttl: int | None = None) -> None:
        await self.set(key, value, ttl=ttl)

    async def multi_get(
        self, keys: list[str], *, type: type[T] | None = None
    ) -> list[T | Any | None]:
        result: list[T | Any | None] = []
        for key in keys:
            value = self.data.get(key)
            if value is None or type is None:
                result.append(value)
            elif isinstance(value, dict):
                result.append(type(**value))
            else:
                result.append(value)
        return result

    async def multi_get_values(
        self,
        keys: list[str],
        *,
        type: type[T] | None = None,
    ) -> list[T | Any | None]:
        return await self.multi_get(keys, type=type)

    async def multi_set(self, pairs: list[tuple[str, Any]], ttl: int | None = None) -> None:
        _ = ttl
        for key, value in pairs:
            self.data[key] = value

    async def multi_set_values(self, pairs: list[tuple[str, Any]], ttl: int | None = None) -> None:
        await self.multi_set(pairs, ttl=ttl)

    async def exists(self, key: str) -> bool:
        return key in self.data

    async def delete(self, key: str) -> int:
        if key not in self.data:
            return 0
        del self.data[key]
        return 1

    async def delete_many(self, keys: list[str]) -> int:
        deleted = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                deleted += 1
        return deleted

    async def incr(self, key: str, delta: int = 1) -> int:
        current = int(self.data.get(key) or 0)
        current += delta
        self.data[key] = current
        return current

    async def close(self) -> None:
        return None


class _FakeSession:
    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeSessionManager:
    async def __aenter__(self) -> _FakeSession:
        return _FakeSession()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        _ = exc_type
        _ = exc
        _ = tb

    def session(self) -> _FakeSessionManager:
        return self


class _EntityOut(msgspec.Struct):
    id: int
    name: str


class _Create(msgspec.Struct):
    name: str


class _Update(msgspec.Struct, kw_only=True):
    name: str | msgspec.UnsetType = msgspec.UNSET


class _EntityModel:
    __name__ = "EntityModel"


class _ModelWithDependsOn(BaseModel):
    __tablename__ = "products"
    id: int = ColumnField(primary_key=True)
    reviews: list[dict[str, Any]] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        depends_on=("product_reviews:product_id",),
    )
    note_count: int = ProjectionField(
        loader=None,
        depends_on=("product_notes:product_id",),
        default=0,
    )


@cached
class _FakeRepository(Repository[_EntityOut, _Create, _Update, int]):
    model: type[Any] = _EntityModel
    output_type = _EntityOut

    def __init__(self) -> None:
        self.storage: dict[int, _EntityOut] = {}
        self.get_calls = 0
        self.list_calls = 0
        self.query_calls = 0
        self.custom_calls = 0
        self.session_manager = _FakeSessionManager()

    def to_output_from_payload(self, payload: dict[str, Any]) -> _EntityOut:
        return _EntityOut(**payload)

    async def get_by_id(self, obj_id: int, profile: str = "default") -> _EntityOut | None:
        _ = profile
        self.get_calls += 1
        return self.storage.get(obj_id)

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> _EntityOut | None:
        _ = profile
        if field == "id":
            candidate = self.storage.get(int(value))
            self.get_calls += 1
            return candidate
        if field == "name":
            self.get_calls += 1
            for item in self.storage.values():
                if item.name == value:
                    return item
            return None
        raise ValueError(f"unsupported field: {field}")

    async def exists_by(self, field: str, value: Any) -> bool:
        return await self.get_by(field, value) is not None

    async def count(self) -> int:
        return len(self.storage)

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[_EntityOut]:
        _ = filter_params
        _ = profile
        self.list_calls += 1
        values = list(self.storage.values())
        start = page_params.offset
        end = start + page_params.limit
        items = values[start:end]
        return build_page_result(items, len(values), page_params)

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[_EntityOut] | CursorResult[_EntityOut]:
        _ = profile
        self.query_calls += 1
        if query.pagination == PaginationMode.CURSOR:
            values = tuple(list(self.storage.values())[: query.limit])
            return CursorResult(items=values, next_cursor=None, has_next=False)
        page = PageParams(page=query.page, limit=query.limit)
        return await self.list_paginated(page, profile=profile)

    async def create(self, data: _Create) -> _EntityOut:
        new_id = len(self.storage) + 1
        out = _EntityOut(id=new_id, name=data.name)
        self.storage[new_id] = out
        return out

    async def update(self, obj_id: int, data: _Update) -> _EntityOut | None:
        existing = self.storage.get(obj_id)
        if existing is None:
            return None
        fields = msgspec.to_builtins(data)
        next_name = fields.get("name", existing.name)
        updated = _EntityOut(id=obj_id, name=next_name)
        self.storage[obj_id] = updated
        return updated

    async def delete(self, obj_id: int) -> bool:
        if obj_id not in self.storage:
            return False
        del self.storage[obj_id]
        return True

    @cache_query(scope="list")
    async def find_names(self, prefix: str) -> list[str]:
        self.custom_calls += 1
        return [item.name for item in self.storage.values() if item.name.startswith(prefix)]

    @cache_query(scope="entity")
    async def count_related_notes(self, entity_id: int) -> int:
        _ = entity_id
        self.custom_calls += 1
        return len(self.storage)


class _RepoWithModelDependsOn(_FakeRepository):
    model = _ModelWithDependsOn
    depends_on = ("products:id",)


class _RepoWithInvalidDependsOn(_FakeRepository):
    depends_on = ("invalid",)


# ---------------------------------------------------------------------------
# Auto-inferred depends_on fixtures (no explicit declaration)
# ---------------------------------------------------------------------------


class _AutoReview(BaseModel):
    """Child model — used to resolve ONE_TO_MANY auto-inference."""

    __tablename__ = "auto_reviews"
    id: int = ColumnField(primary_key=True)
    product_id: int = ColumnField(foreign_key="auto_products.id")


class _FakeLoaderWithModel:
    """Minimal loader stub that exposes a ``model`` attribute."""

    def __init__(self, model: type) -> None:
        self.model = model


class _AutoProduct(BaseModel):
    """Parent model — no explicit ``depends_on`` on any field."""

    __tablename__ = "auto_products"
    id: int = ColumnField(primary_key=True)
    reviews: list[_AutoReview] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        # depends_on intentionally omitted → should be auto-inferred
    )
    review_count: int = ProjectionField(
        loader=_FakeLoaderWithModel(model=_AutoReview),
        default=0,
        # depends_on intentionally omitted → should be auto-inferred
    )


class _AutoProductRepo(_FakeRepository):
    model = _AutoProduct


class _FqAutoChild(BaseModel):
    """Child model with fully-qualified FK string on the parent relation."""

    __tablename__ = "fq_auto_children"
    id: int = ColumnField(primary_key=True)
    parent_id: int = ColumnField(foreign_key="fq_auto_parents.id")


class _FqAutoParent(BaseModel):
    """Parent model using fully-qualified FK format (``table.column``)."""

    __tablename__ = "fq_auto_parents"
    id: int = ColumnField(primary_key=True)
    children: list[_FqAutoChild] = RelationField(
        foreign_key="fq_auto_children.parent_id",  # fully-qualified
        cardinality=Cardinality.ONE_TO_MANY,
        # depends_on intentionally omitted
    )


class _FqAutoParentRepo(_FakeRepository):
    model = _FqAutoParent


@pytest.fixture
def wrapped_repository(
    cache_config: CacheConfig,
) -> CachedRepository[_EntityOut, _Create, _Update, int]:
    repository = _FakeRepository()
    cache = _MemoryCacheBackend()
    resolver = GenerationalDependencyResolver(cache)
    return CachedRepository(
        repository, config=cache_config, cache=cache, dependency_resolver=resolver
    )


class TestCachedDecorator:
    def test_marks_repository_class(self) -> None:
        assert getattr(_FakeRepository, "__cache_policy__", False) is True


class TestCachedRepository:
    @pytest.mark.asyncio
    async def test_get_by_id_uses_cache_aside(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        created = await wrapped_repository.create(_Create(name="entity-1"))

        loaded_1 = await wrapped_repository.get_by_id(created.id)
        loaded_2 = await wrapped_repository.get_by_id(created.id)

        assert loaded_1 is not None
        assert loaded_2 is not None
        assert loaded_1.id == created.id
        assert loaded_2.id == created.id
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.get_calls == 1

    @pytest.mark.asyncio
    async def test_custom_method_cache_for_developer_function(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="alpha"))
        await wrapped_repository.create(_Create(name="beta"))

        result_1 = await wrapped_repository.find_names("a")
        result_2 = await wrapped_repository.find_names("a")

        assert result_1 == ["alpha"]
        assert result_2 == ["alpha"]
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.custom_calls == 1

    @pytest.mark.asyncio
    async def test_delete_invalidates_entity_cache(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        created = await wrapped_repository.create(_Create(name="entity-1"))
        _ = await wrapped_repository.get_by_id(created.id)
        _ = await wrapped_repository.get_by_id(created.id)

        deleted = await wrapped_repository.delete(created.id)
        loaded_after_delete = await wrapped_repository.get_by_id(created.id)

        assert deleted is True
        assert loaded_after_delete is None
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.get_calls == 2

    @pytest.mark.asyncio
    async def test_custom_method_cache_is_invalidated_by_external_event(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        _ = await wrapped_repository.create(_Create(name="entity-1"))

        first = await wrapped_repository.count_related_notes(1)
        second = await wrapped_repository.count_related_notes(1)
        assert first == second

        await wrapped_repository.on_transaction_committed(
            (
                MutationEvent(
                    entity=wrapped_repository.entity_name,
                    op="update",
                    ids=(1,),
                ),
            )
        )

        third = await wrapped_repository.count_related_notes(1)
        assert third == first
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.custom_calls == 2

    @pytest.mark.asyncio
    async def test_custom_method_cache_is_invalidated_by_a_create_event_for_its_id(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        _ = await wrapped_repository.create(_Create(name="entity-1"))
        first = await wrapped_repository.count_related_notes(1)

        await wrapped_repository.on_transaction_committed(
            (
                MutationEvent(
                    entity=wrapped_repository.entity_name,
                    op="create",
                    ids=(1,),
                ),
            )
        )

        second = await wrapped_repository.count_related_notes(1)
        assert second == first
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.custom_calls == 2

    @pytest.mark.asyncio
    async def test_list_with_query_offset_uses_cache_aside(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="a"))
        await wrapped_repository.create(_Create(name="b"))
        query = QuerySpec(page=1, limit=1, pagination=PaginationMode.OFFSET)

        first = await wrapped_repository.list_with_query(query)
        second = await wrapped_repository.list_with_query(query)

        assert len(first.items) == 1
        assert len(second.items) == 1
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.query_calls == 1

    @pytest.mark.asyncio
    async def test_list_with_query_cursor_caches_only_first_page(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        await wrapped_repository.create(_Create(name="a"))
        await wrapped_repository.create(_Create(name="b"))
        first_page = QuerySpec(limit=1, pagination=PaginationMode.CURSOR)
        next_page = QuerySpec(limit=1, pagination=PaginationMode.CURSOR, cursor="cursor-2")

        _ = await wrapped_repository.list_with_query(first_page)
        _ = await wrapped_repository.list_with_query(first_page)
        _ = await wrapped_repository.list_with_query(next_page)
        _ = await wrapped_repository.list_with_query(next_page)

        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.query_calls == 3


class TestDependencySpecs:
    def test_collects_depends_on_from_repository_relation_and_projection(
        self, cache_config: CacheConfig
    ) -> None:
        repository = _RepoWithModelDependsOn()
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)
        wrapped = CachedRepository(
            repository, config=cache_config, cache=cache, dependency_resolver=resolver
        )

        specs = {(item.entity, item.fk_field) for item in wrapped._depends_on}
        assert specs == {
            ("products", "id"),
            ("product_reviews", "product_id"),
            ("product_notes", "product_id"),
        }

    def test_invalid_dependency_spec_fails_fast(self, cache_config: CacheConfig) -> None:
        repository = _RepoWithInvalidDependsOn()
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)

        with pytest.raises(ValueError, match="Invalid dependency spec"):
            CachedRepository(
                repository, config=cache_config, cache=cache, dependency_resolver=resolver
            )

    def test_otm_relation_dep_auto_inferred_when_depends_on_empty(
        self, cache_config: CacheConfig
    ) -> None:
        """ONE_TO_MANY relation without explicit depends_on → spec auto-inferred."""
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)
        wrapped = CachedRepository(
            _AutoProductRepo(), config=cache_config, cache=cache, dependency_resolver=resolver
        )

        specs = {(d.entity, d.fk_field) for d in wrapped._depends_on}
        assert ("auto_reviews", "product_id") in specs

    def test_projection_dep_auto_inferred_via_loader_model(self, cache_config: CacheConfig) -> None:
        """Projection with loader.model and no depends_on → auto-inferred from sibling relation."""
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)
        wrapped = CachedRepository(
            _AutoProductRepo(), config=cache_config, cache=cache, dependency_resolver=resolver
        )

        specs = {(d.entity, d.fk_field) for d in wrapped._depends_on}
        # projection and relation share the same spec — deduplication keeps one entry
        assert ("auto_reviews", "product_id") in specs

    def test_fully_qualified_fk_normalised_in_auto_inferred_spec(
        self, cache_config: CacheConfig
    ) -> None:
        """Fully-qualified FK ``table.column`` is normalised to bare column name."""
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)
        wrapped = CachedRepository(
            _FqAutoParentRepo(), config=cache_config, cache=cache, dependency_resolver=resolver
        )

        specs = {(d.entity, d.fk_field) for d in wrapped._depends_on}
        assert ("fq_auto_children", "parent_id") in specs

    def test_explicit_depends_on_wins_over_auto_inference(self, cache_config: CacheConfig) -> None:
        """Explicit depends_on is used as-is; auto-inference is skipped for that field."""
        cache = _MemoryCacheBackend()
        resolver = GenerationalDependencyResolver(cache)
        wrapped = CachedRepository(
            _RepoWithModelDependsOn(),
            config=cache_config,
            cache=cache,
            dependency_resolver=resolver,
        )

        # explicit specs from _ModelWithDependsOn and the repo-level depends_on
        specs = {(d.entity, d.fk_field) for d in wrapped._depends_on}
        assert ("product_reviews", "product_id") in specs
        assert ("product_notes", "product_id") in specs
        assert ("products", "id") in specs


class _CreateWithNote(msgspec.Struct):
    name: str
    note: str


class _BulkFakeRepository(_FakeRepository):
    async def create_many(self, data: Sequence[msgspec.Struct]) -> tuple[_EntityOut, ...]:
        names = [str(msgspec.to_builtins(item)["name"]) for item in data]
        return tuple([await self.create(_Create(name=name)) for name in names])


class _RecordingResolver(GenerationalDependencyResolver):
    def __init__(self, cache: CacheBackend) -> None:
        super().__init__(cache)
        self.events: list[MutationEvent] = []

    async def bump_from_events(self, events: tuple[MutationEvent, ...]) -> None:
        self.events.extend(events)
        await super().bump_from_events(events)


def _wrap(
    repository: _FakeRepository, cache_config: CacheConfig
) -> tuple[CachedRepository[_EntityOut, _Create, _Update, int], _RecordingResolver]:
    cache = _MemoryCacheBackend()
    resolver = _RecordingResolver(cache)
    wrapped = CachedRepository(
        repository, config=cache_config, cache=cache, dependency_resolver=resolver
    )
    return wrapped, resolver


class TestCreateMany:
    @pytest.mark.asyncio
    async def test_emits_one_create_event_with_ids_and_union_of_fields(
        self, cache_config: CacheConfig
    ) -> None:
        wrapped, resolver = _wrap(_BulkFakeRepository(), cache_config)

        created = await wrapped.create_many(
            [_Create(name="a"), _CreateWithNote(name="b", note="n")]
        )

        assert [row.name for row in created] == ["a", "b"]
        assert resolver.events == [
            MutationEvent(
                entity=wrapped.entity_name,
                op="create",
                ids=(1, 2),
                changed_fields=frozenset({"name", "note"}),
            )
        ]

    @pytest.mark.asyncio
    async def test_empty_batch_bumps_no_generation(self, cache_config: CacheConfig) -> None:
        wrapped, resolver = _wrap(_BulkFakeRepository(), cache_config)

        assert await wrapped.create_many([]) == ()
        assert resolver.events == []

    def test_access_fails_when_the_wrapped_repository_has_no_create_many(
        self, cache_config: CacheConfig
    ) -> None:
        inner = _FakeRepository()
        wrapped, _ = _wrap(inner, cache_config)

        with pytest.raises(AttributeError):
            _ = wrapped.create_many
        assert hasattr(wrapped, "create_many") is hasattr(inner, "create_many")


def _generation(
    wrapped: CachedRepository[_EntityOut, _Create, _Update, int],
    resolver: GenerationalDependencyResolver,
) -> int:
    """Read the raw generation counter of the wrapped entity's ``:list`` tag.

    That tag is the one every mutation event bumps.  Reserved for the one
    test that must observe a value *mid-drain*, from inside another queued
    action: everywhere else, ``resolver.events`` is already the public,
    equivalent observation of whether the bump ran.
    """
    cache = wrapped._cache
    assert isinstance(cache, _MemoryCacheBackend)
    return int(cache.data.get(resolver._tag_key(f"{wrapped.entity_name}:list")) or 0)


class TestPostCommitDeferral:
    """F03: the generation bump must wait for the transaction to commit.

    Under an atomic transaction (a unit of work or an owned ``@transactional``
    session), the wrapped write is not durable yet when ``create``/``update``/
    ``delete``/``create_many`` return, so bumping the generation counter right
    there would let a concurrent reader repopulate the cache from state that
    might still roll back, or invalidate a write that never lands.
    """

    @pytest.mark.asyncio
    async def test_create_inside_a_transaction_defers_the_bump_past_commit(
        self, cache_config: CacheConfig
    ) -> None:
        """The regression: a concurrent reader must not see the bump before commit."""
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        with open_transaction_with_channel() as channel:
            created = await wrapped.create(_Create(name="entity-1"))
            # A second reader, mid-transaction: no bump has happened, exactly
            # as if the write had not occurred.
            assert resolver.events == []

        # The transaction commits: draining the channel is what a real commit
        # does through the executor or ``@transactional``.
        await channel.drain(committed=True)

        assert resolver.events == [
            MutationEvent(
                entity=wrapped.entity_name,
                op="create",
                ids=(created.id,),
                changed_fields=frozenset({"name"}),
            )
        ]

    @pytest.mark.asyncio
    async def test_create_after_a_queued_dispatch_still_bumps_before_it(
        self, cache_config: CacheConfig
    ) -> None:
        """F2: a use case that dispatches a job, then writes, must not read stale cache.

        ``add_pending_dispatch`` queues on the same channel with a plain
        ``enqueue``; in inline job mode the dispatch body runs during the
        drain and may read the cache this write is about to invalidate. The
        bump this wrapper queues must run first regardless of enqueue order.
        """
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        # Simulates inline job mode: the dispatch body reads the cache while
        # the channel drains it, exactly as a real job handler would.
        generation_seen_by_dispatch: list[int] = []
        with open_transaction_with_channel() as channel:
            # The use case body dispatches a job first...
            channel.enqueue(
                lambda: generation_seen_by_dispatch.append(_generation(wrapped, resolver))
            )
            # ...then writes. The bump this call queues must still run first.
            await wrapped.create(_Create(name="entity-1"))

        await channel.drain(committed=True)

        # The dispatch, though queued first, ran after the invalidation.
        assert generation_seen_by_dispatch == [1]

    @pytest.mark.asyncio
    async def test_rollback_publishes_no_bump_at_all(self, cache_config: CacheConfig) -> None:
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        with open_transaction_with_channel() as channel:
            await wrapped.create(_Create(name="entity-1"))

        # The transaction rolled back: the channel is discarded, not drained.
        channel.discard()

        assert resolver.events == []

    @pytest.mark.asyncio
    async def test_create_with_no_transaction_bumps_inline(self, cache_config: CacheConfig) -> None:
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)

        await wrapped.create(_Create(name="entity-1"))

        assert resolver.events != []

    @pytest.mark.asyncio
    async def test_a_bound_channel_with_no_transaction_still_bumps_inline(
        self, cache_config: CacheConfig
    ) -> None:
        """The executor binds a channel even when it owns no unit of work.

        A read-only execution (or a Mongo/DynamoDB backend without a
        transaction) still has an ``active_channel()``, but nothing commits
        it later: it is drained with ``committed=False`` or discarded on
        failure. Deferring on channel presence alone would drop the bump
        silently on that path; ``active_channel() is not None`` is not the
        right predicate.
        """
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        channel = PostCommitChannel()
        channel_token = bind_channel(channel)
        try:
            await wrapped.create(_Create(name="entity-1"))
            # Bumped immediately, not queued on the bound-but-uncommitted channel.
            assert resolver.events != []
        finally:
            reset_channel(channel_token)

    @pytest.mark.asyncio
    async def test_a_bare_unit_of_work_with_no_channel_bound_still_bumps_inline(
        self, cache_config: CacheConfig
    ) -> None:
        """F1: a directly entered unit of work opens the signal but binds no channel.

        ``async with SQLAlchemyUnitOfWork(session_manager): await repo.create(...)``
        — the usage both units of work document — is real: only the executor
        and ``@transactional`` bind a post-commit channel. With a transaction
        open but nothing to defer to, bumping inline here is the deliberate
        fallback: deferring would queue an action nothing ever drains,
        losing the invalidation for good. This reopens the F03 race for that
        one path, which is the lesser fault of the two.
        """
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        assert active_channel() is None
        transaction_token = open_atomic_transaction()
        try:
            await wrapped.create(_Create(name="entity-1"))
            # Bumped immediately: no channel exists to defer to.
            assert resolver.events != []
        finally:
            close_atomic_transaction(transaction_token)

    @pytest.mark.asyncio
    async def test_update_with_no_transaction_bumps_inline(self, cache_config: CacheConfig) -> None:
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        created = await wrapped.create(_Create(name="entity-1"))
        resolver.events.clear()

        await wrapped.update(created.id, _Update(name="entity-1-renamed"))

        assert resolver.events == [
            MutationEvent(
                entity=wrapped.entity_name,
                op="update",
                ids=(created.id,),
                changed_fields=frozenset({"name"}),
            )
        ]

    @pytest.mark.asyncio
    async def test_delete_with_no_transaction_bumps_inline(self, cache_config: CacheConfig) -> None:
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)
        created = await wrapped.create(_Create(name="entity-1"))
        resolver.events.clear()

        await wrapped.delete(created.id)

        assert resolver.events == [
            MutationEvent(entity=wrapped.entity_name, op="delete", ids=(created.id,))
        ]

    @pytest.mark.asyncio
    async def test_create_many_with_no_transaction_bumps_inline(
        self, cache_config: CacheConfig
    ) -> None:
        wrapped, resolver = _wrap(_BulkFakeRepository(), cache_config)

        await wrapped.create_many([_Create(name="a")])

        assert resolver.events == [
            MutationEvent(
                entity=wrapped.entity_name,
                op="create",
                ids=(1,),
                changed_fields=frozenset({"name"}),
            )
        ]

    @pytest.mark.asyncio
    async def test_create_many_inside_a_transaction_defers_the_bump(
        self, cache_config: CacheConfig
    ) -> None:
        wrapped, resolver = _wrap(_BulkFakeRepository(), cache_config)
        with open_transaction_with_channel() as channel:
            await wrapped.create_many([_Create(name="a")])
            assert resolver.events == []

        await channel.drain(committed=True)

        assert resolver.events != []

    @pytest.mark.asyncio
    async def test_a_failing_deferred_bump_logs_the_entity_and_ids_and_still_raises(
        self,
        cache_config: CacheConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        wrapped, resolver = _wrap(_FakeRepository(), cache_config)

        async def _boom(events: tuple[MutationEvent, ...]) -> None:
            raise RuntimeError("cache backend unreachable")

        resolver.bump_from_events = _boom  # type: ignore[method-assign]
        with open_transaction_with_channel() as channel:
            created = await wrapped.create(_Create(name="entity-1"))

        with caplog.at_level("ERROR"), pytest.raises(PostCommitError) as excinfo:
            await channel.drain(committed=True)

        assert isinstance(excinfo.value.failures[0], RuntimeError)
        assert "CachePostCommitBumpFailed" in caplog.text
        assert wrapped.entity_name in caplog.text
        assert str(created.id) in caplog.text


class TestPrimaryKeyByName:
    """The wrapper reads the primary key by the attribute the model declares."""

    @pytest.mark.asyncio
    async def test_create_event_ids_carry_the_declared_key(self, cache_config: CacheConfig) -> None:
        repository = WritableRepository([CodeWidget(code=1, name="a")], CodeWidget)
        backend = CountingCacheBackend()
        resolver = _RecordingResolver(backend)
        wrapped = CachedRepository(
            repository, config=cache_config, cache=backend, dependency_resolver=resolver
        )

        created = await wrapped.create(WidgetCreate(name="b"))

        assert created.code == 2
        assert resolver.events == [
            MutationEvent(
                entity=wrapped.entity_name,
                op="create",
                ids=(2,),
                changed_fields=frozenset({"name"}),
            )
        ]

    @pytest.mark.asyncio
    async def test_batch_caches_only_the_items_with_a_key(self, cache_config: CacheConfig) -> None:
        keyed = CodeWidget(code=1, name="a")
        keyless = CodeWidget(code=cast(int, None), name="b")
        env = wrap_with_cache(CountingRepository([keyless, keyed], CodeWidget), cache_config)

        page = await env.wrapper.list_paginated(PageParams(page=1, limit=2))

        assert list(page.items) == [keyless, keyed]
        fingerprint = await env.resolver.fingerprint(
            env.resolver.entity_tags(env.wrapper.entity_name, 1)
        )
        expected_key = entity_key(env.wrapper.entity_name, 1, "default", fingerprint, scope="")
        entity_batches = [batch for batch in env.backend.multi_set_batches if expected_key in batch]
        assert entity_batches == [[expected_key]]
        assert await env.backend.get_value(expected_key, type=CodeWidget) == keyed

    @pytest.mark.asyncio
    async def test_repository_without_model_falls_back_to_id(
        self, cache_config: CacheConfig
    ) -> None:
        repository = WritableRepository([Widget(id=1, name="a")], Widget)
        del repository.model
        backend = CountingCacheBackend()
        resolver = _RecordingResolver(backend)
        wrapped = CachedRepository(
            repository, config=cache_config, cache=backend, dependency_resolver=resolver
        )

        created = await wrapped.create(WidgetCreate(name="b"))

        assert created.id == 2
        assert [event.ids for event in resolver.events] == [(2,)]


class TestEntityScopeValidation:
    """A ``scope="entity"`` read is keyed by the primary key, of its exact type."""

    @staticmethod
    def _assert_backend_untouched(env: CachedEnv[Any]) -> None:
        assert env.backend.multi_get_batches == []
        assert env.backend.multi_set_batches == []
        assert env.backend.data == {}
        assert env.repository.note_count_calls == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("env", "wrong_id", "received", "expected"),
        [
            (
                wrap_with_cache(
                    CountingRepository([Widget(id=1, name="a")], Widget), CacheConfig()
                ),
                "1",
                "str",
                "int",
            ),
            (
                wrap_with_cache(
                    CountingRepository([Widget(id=1, name="a")], Widget), CacheConfig()
                ),
                True,
                "bool",
                "int",
            ),
            (
                wrap_with_cache(
                    CountingRepository([DateWidget(id=date(2026, 1, 1), name="a")], DateWidget),
                    CacheConfig(),
                ),
                datetime(2026, 1, 1),
                "datetime",
                "date",
            ),
            (
                wrap_with_cache(
                    CountingRepository([UuidWidget(id=uuid4(), name="a")], UuidWidget),
                    CacheConfig(),
                ),
                str(uuid4()),
                "str",
                "UUID",
            ),
        ],
        ids=["str-for-int", "bool-for-int", "datetime-for-date", "str-for-uuid"],
    )
    async def test_wrong_exact_type_raises_before_touching_the_backend(
        self,
        env: CachedEnv[Any],
        wrong_id: object,
        received: str,
        expected: str,
    ) -> None:
        with pytest.raises(TypeError) as excinfo:
            await env.wrapper.note_count(wrong_id)

        message = str(excinfo.value)
        assert "note_count" in message
        assert received in message
        assert expected in message
        self._assert_backend_untouched(env)

    @pytest.mark.asyncio
    async def test_key_passed_by_keyword_raises(self) -> None:
        env = wrap_with_cache(CountingRepository([Widget(id=1, name="a")], Widget), CacheConfig())

        with pytest.raises(TypeError, match="note_count.*first positional argument"):
            await env.wrapper.note_count(obj_id=1)

        self._assert_backend_untouched(env)

    @pytest.mark.asyncio
    async def test_none_raises_even_when_the_key_type_is_unresolvable(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        unresolvable_id = cast(int, None)

        with pytest.raises(TypeError, match="count_related_notes.*first positional argument"):
            await wrapped_repository.count_related_notes(unresolvable_id)

        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)
        assert repo.custom_calls == 0

    @pytest.mark.asyncio
    async def test_model_without_primary_key_accepts_any_id(
        self,
        wrapped_repository: CachedRepository[_EntityOut, _Create, _Update, int],
    ) -> None:
        repo = wrapped_repository._repository
        assert isinstance(repo, _FakeRepository)

        first = await wrapped_repository.count_related_notes(cast(int, "x"))
        second = await wrapped_repository.count_related_notes(cast(int, "x"))

        assert first == second
        assert repo.custom_calls == 1

    @pytest.mark.asyncio
    async def test_exact_type_caches_until_its_row_is_updated(
        self, cache_config: CacheConfig
    ) -> None:
        rows = [Widget(id=1, name="a"), Widget(id=2, name="bb")]
        env = wrap_with_cache(WritableRepository(rows, Widget), cache_config)

        assert await env.wrapper.note_count(1) == 1
        assert await env.wrapper.note_count(1) == 1
        assert env.repository.note_count_calls == 1

        await env.wrapper.update(1, WidgetUpdate(name="abc"))

        assert await env.wrapper.note_count(1) == 3
        assert env.repository.note_count_calls == 2
        # Granularity (update(2) keeps note_count(1) warm) is covered by
        # test_invalidation_granularity.py.


@cached
class _ReadOnlyRepository:
    """Marked double that only reads, so the wrapper would advertise writes it lacks."""

    async def get_by_id(self, obj_id: int, profile: str = "default") -> _EntityOut | None:
        return None


_OPTIONAL_OVERRIDES = frozenset({"on_transaction_committed", "cache_scope"})
"""Public overrides outside the precondition: the wrapper adapts them when present."""


class TestDelegationPrecondition:
    """The wrapper refuses what it cannot delegate to, and knows what that is."""

    def test_refuses_a_repository_missing_delegated_methods(
        self, cache_config: CacheConfig
    ) -> None:
        cache = _MemoryCacheBackend()
        repository = cast(Any, _ReadOnlyRepository())
        dependency_resolver = GenerationalDependencyResolver(cache)

        with pytest.raises(RuntimeError, match="_ReadOnlyRepository") as info:
            CachedRepository(
                repository,
                config=cache_config,
                cache=cache,
                dependency_resolver=dependency_resolver,
            )

        message = str(info.value)
        assert "create" in message
        assert "list_paginated" in message

    def test_the_derived_set_covers_every_public_override(self) -> None:
        """Drift guard: a new public override must join the set or the allow-list."""
        overridden = {
            name
            for name, attr in vars(CachedRepository).items()
            if not name.startswith("_") and inspect.isfunction(attr)
        }

        assert overridden - _OPTIONAL_OVERRIDES == set(_DELEGATED_METHODS)

    def test_the_derived_set_is_the_repository_surface(self) -> None:
        assert all(hasattr(Repository, name) for name in _DELEGATED_METHODS)
        assert not any(hasattr(Repository, name) for name in _OPTIONAL_OVERRIDES)
