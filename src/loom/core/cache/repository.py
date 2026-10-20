from __future__ import annotations

import asyncio
import inspect
import random
import warnings
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial, wraps
from types import GenericAlias
from typing import Any, Generic, NamedTuple, TypeVar, cast

import msgspec

from loom.core.cache._batching import REFILL_BATCH_SIZE, batched
from loom.core.cache._single_flight import SingleFlight
from loom.core.cache.abc.backend import CacheBackend
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.abc.dependency import BatchFingerprintResolver, DependencyResolver
from loom.core.cache.decorators import declares_cache_policy
from loom.core.cache.keys import entity_key, list_index_key, stable_hash
from loom.core.cache.result_codec import (
    PassthroughResultCodec,
    ResultCodec,
    build_result_codec,
    to_payload,
)
from loom.core.engine.post_commit import active_channel
from loom.core.logger import get_logger
from loom.core.model import BoundaryValidationError, LoomType, is_pydantic_model, loom_type_of
from loom.core.model.convert import to_struct
from loom.core.model.enums import Cardinality
from loom.core.model.introspection import (
    get_id_attributes,
    get_projections,
    get_relations,
    list_element_type,
    resolve_type_hints,
)
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.projection.loaders import resolve_model_reference
from loom.core.repository import FilterParams, MutationEvent, PageParams, PageResult, Repository
from loom.core.repository.abc.query import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterSpec,
    PaginationMode,
    QuerySpec,
)
from loom.core.repository.abc.repo_for import BulkCreatable
from loom.core.repository.abc.repository import CreateT, IdT, OutputT, UpdateT
from loom.core.repository.abc.session_scope import SupportsCallerScopedSession
from loom.core.transaction import in_atomic_transaction

LoadT = TypeVar("LoadT")


def _infer_otm_cache_dep(model: type, attr_name: str, rel: Relation) -> str | None:
    """Auto-infer ``entity:fk_col`` cache spec for a ONE_TO_MANY relation.

    Resolves the child model type from the field's type annotation and
    combines it with the FK column name from ``rel.foreign_key``.

    Returns ``None`` when cardinality is not ONE_TO_MANY, when the child
    type cannot be resolved, or when it has no ``__tablename__``.
    """
    if rel.cardinality is not Cardinality.ONE_TO_MANY:
        return None
    hints = resolve_type_hints(model)
    child_type = list_element_type(hints.get(attr_name))
    if child_type is None or not hasattr(child_type, "__tablename__"):
        return None
    fk_col = rel.foreign_key.rsplit(".", 1)[-1]
    return f"{child_type.__tablename__}:{fk_col}"


def _infer_projection_cache_dep(model: type, proj: Projection) -> str | None:
    """Auto-infer ``entity:fk_col`` cache spec for a projection loader.

    Looks up the ONE_TO_MANY relation on ``model`` whose child type matches
    the loader's ``model`` attribute, then reuses that relation's FK column.

    Returns ``None`` when the loader has no ``model``, when no matching
    ONE_TO_MANY relation is found, or when types are unresolvable.
    """
    loader_model = resolve_model_reference(getattr(proj.loader, "model", None))
    if loader_model is None or not hasattr(loader_model, "__tablename__"):
        return None
    hints = resolve_type_hints(model)
    for attr_name, rel in get_relations(model).items():
        if rel.cardinality is not Cardinality.ONE_TO_MANY:
            continue
        if list_element_type(hints.get(attr_name)) is loader_model:
            fk_col = rel.foreign_key.rsplit(".", 1)[-1]
            return f"{loader_model.__tablename__}:{fk_col}"
    return None


_UNDECODABLE = object()
"""Sentinel for a cached payload that no longer fits its declared type."""


def _is_cached_read(attr: object) -> bool:
    """Whether *attr* is a coroutine function marked with ``@cache_query``."""
    return getattr(attr, "__cache_query__", None) is not None and inspect.iscoroutinefunction(attr)


class _ListIndexPayload(msgspec.Struct):
    ids: list[Any]
    total_count: int


class _QueryIndexPayload(msgspec.Struct):
    ids: list[Any]
    total_count: int | None = None
    next_cursor: str | None = None
    has_next: bool = False


@dataclass(frozen=True, slots=True)
class _DependencySpec:
    entity: str
    fk_field: str


class _PrimaryKey(NamedTuple):
    """Primary key of the cached model as the wrapper reads it.

    A single-column key is its value; a composite key is the tuple of every
    key value in declaration order, the form the SQLAlchemy repository's
    ``get_by_id`` takes.

    Attributes:
        attributes: Names of the primary-key attributes in declaration order;
            ``("id",)`` when the wrapped repository declares no model or the
            model declares no primary key.
        python_types: Type of each key attribute, ``None`` where it is
            unresolvable or not a plain class (``int | None``, for instance).
    """

    attributes: tuple[str, ...]
    python_types: tuple[type | None, ...]


_FALLBACK_PRIMARY_KEY = _PrimaryKey(attributes=("id",), python_types=(None,))


def _resolve_primary_key(repository: Repository[Any, Any, Any, Any]) -> _PrimaryKey:
    """Resolve the primary key of the wrapped repository's model.

    Falls back to the attribute ``id`` with no type for a repository without a
    model or whose model declares no primary key; the type of one attribute
    alone is ``None`` when its annotation is not a plain class.
    """
    model = getattr(repository, "model", None)
    if model is None:
        return _FALLBACK_PRIMARY_KEY
    try:
        attributes = get_id_attributes(model)
    except (TypeError, ValueError):
        return _FALLBACK_PRIMARY_KEY
    hints = resolve_type_hints(model)
    types = tuple(hint if isinstance(hint, type) else None for hint in map(hints.get, attributes))
    return _PrimaryKey(attributes, types)


class CachedRepository(
    Repository[OutputT, CreateT, UpdateT, IdT],
    Generic[OutputT, CreateT, UpdateT, IdT],
):
    """Cache-aside wrapper with generational invalidation.

    Attributes the wrapper does not define pass through to the wrapped
    repository, so its capability set (``create_many``, ``count``, custom
    queries) is exactly the wrapped one.

    The cached list path reloads the entities missing from a warm index with a
    single ``<primary key> IN (...)`` query, so the wrapped repository should
    support :attr:`~loom.core.repository.abc.query.FilterOp.IN` on the primary
    key; a repository whose ``allowed_filter_fields`` excludes the primary key
    is detected and served with per-id reads instead, and so is a model with a
    composite primary key, which a filter on one field cannot match.  The key
    is read by the attribute names the model declares (``id`` when there is no
    model or the model declares no primary key); a composite key is the tuple
    of its values in declaration order.

    Concurrent misses of the same key on the entity read and on a
    ``@cache_query`` read are coalesced inside the process: the first caller
    loads and the rest await that result, so a burst on a hot key costs one
    repository call.  Two conditions bound that:

    * The wrapper must be application-scoped.  A wrapper built per request has
      a coalescing group of one, so it never coalesces anything and only pays
      the bookkeeping.
    * Coalescing is skipped while the wrapped repository reports a
      caller-scoped session (``has_caller_scoped_session()``), which is what a
      ``@transactional`` scope or a unit of work binds.  A coalesced load runs
      in its own task and outlives the caller that started it, so inside a
      transaction it would query a session already closed by that caller's
      teardown, and would serve another caller the uncommitted writes of the
      first.  Inside a transaction the read runs inline.

    Callers that miss together are served the *same object* by the coalesced
    load, on the entity read as on a ``@cache_query`` read, while a caller
    served from the cache gets a freshly decoded one; treat a cached result as
    immutable, since a ``BaseModel`` struct is mutable unless declared frozen.

    The cached list and query reads are deliberately left out of the
    coalescing: their miss path is not a pure loader — it caches the page's
    entities as a side effect — and its result feeds ``_load_items_from_index``,
    which re-enters the coalesced entity read, so the herd is already collapsed
    one level down.

    Every TTL is spread inside
    :attr:`~loom.core.cache.abc.config.CacheConfig.ttl_jitter` as it reaches
    the backend, so two write calls do not choose the same expiry.  The spread
    is per write call: a batch write (a cached page, an index refill) carries
    one TTL for the whole batch because
    :class:`~loom.core.cache.abc.backend.CacheBackend` takes one TTL per call,
    so the rows of a single page still expire together.
    """

    def __init__(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
        *,
        config: CacheConfig,
        cache: CacheBackend,
        dependency_resolver: DependencyResolver,
    ) -> None:
        _require_full_repository(repository)
        self._repository = repository
        self._config = config
        self._cache = cache
        self._resolver = dependency_resolver
        fallback_name = repository.__class__.__name__.lower()
        self._entity_name = getattr(repository, "entity_name", fallback_name)
        self._depends_on = self._parse_dependency_specs(self._collect_dependency_specs(repository))
        self._primary_key = _resolve_primary_key(repository)
        self._single_flight = SingleFlight(self._log_abandoned_load)
        self._rng = random.Random()
        self._log = get_logger(__name__).bind(repository=repository.__class__.__name__)
        self._passthrough_codec = PassthroughResultCodec()
        self._cached_method_wrappers: dict[str, Callable[..., Awaitable[Any]]] = {}
        self._result_codecs = self._build_result_codecs(repository)
        self._pydantic_output_type: LoomType | None = self._resolve_pydantic_output_type(repository)

    @staticmethod
    def _resolve_pydantic_output_type(
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
    ) -> LoomType | None:
        """Compile the repository's pydantic ``model``, or ``None`` when it is not one."""
        model = getattr(repository, "model", None)
        if not isinstance(model, type) or not is_pydantic_model(model):
            return None
        return loom_type_of(model)

    @property
    def entity_name(self) -> str:
        """Normalized name of the cached entity."""
        return self._entity_name

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        tags = self._resolver.entity_tags(self.entity_name, obj_id)
        tags.extend(self._entity_dependency_tags(obj_id))
        fingerprint = await self._resolver.fingerprint(tags)
        key = entity_key(self.entity_name, obj_id, profile, fingerprint)

        cached_payload = await self._cache.get_value(key)
        if cached_payload is not None:
            self._log.debug("CacheHitEntity", key=key)
            return self._to_output_from_cache(cached_payload)

        self._log.debug("CacheMissEntity", key=key)
        return await self._coalesced(
            key,
            lambda: self._load_and_store_entity(key, obj_id, profile),
        )

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by arbitrary field.

        This path intentionally delegates to the wrapped repository without
        cache-aside behavior for now. Field-based lookups can target mutable
        columns and the cache invalidation surface is broader than id-based
        access; keeping it uncached preserves correctness while the lookup
        cache policy is designed explicitly.
        """
        return await self._repository.get_by(field, value, profile=profile)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check existence by arbitrary field.

        Existence checks are delegated directly to the wrapped repository to
        avoid stale negative/positive cache entries on mutable fields.
        """
        return await self._repository.exists_by(field, value)

    async def count(self) -> int:
        """Count every entity, forwarded uncached to the wrapped repository.

        Counts are not cached: a total changes on every write and a stale
        value is worse than the single round trip.
        """
        return await self._repository.count()

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        filters_payload = self._serialize_filters(filter_params)
        filter_fingerprint = stable_hash(filters_payload)
        tags = self._resolver.list_tags(self.entity_name, filter_fingerprint)
        tags.extend(self._list_dependency_tags())
        fingerprint = await self._resolver.fingerprint(tags)
        index_key = list_index_key(
            self.entity_name,
            filter_fingerprint,
            page=page_params.page,
            limit=page_params.limit,
            profile=profile,
            deps_fingerprint=fingerprint,
        )
        cached_index = await self._cache.get_value(index_key, type=_ListIndexPayload)
        entity_ids = None if cached_index is None else self._index_ids(cached_index.ids)
        if cached_index is not None and entity_ids is not None:
            total_count = cached_index.total_count
            items = await self._load_items_from_index(entity_ids, profile=profile)
            if len(items) == len(entity_ids):
                self._log.debug("CacheHitList", key=index_key)
                return PageResult(
                    items=tuple(items),
                    total_count=total_count,
                    page=page_params.page,
                    limit=page_params.limit,
                    has_next=(page_params.offset + len(items)) < total_count,
                )

        self._log.debug("CacheMissList", key=index_key)
        page = await self._repository.list_paginated(
            page_params,
            filter_params=filter_params,
            profile=profile,
        )

        ids = [
            cast(IdT, entity_id)
            for item in page.items
            for entity_id in [self._extract_entity_id(item)]
            if entity_id is not None
        ]
        index_to_store = _ListIndexPayload(ids=ids, total_count=page.total_count)
        ttl = self._write_ttl(self._config.ttl_for_list(self.entity_name))
        await self._cache.set_value(index_key, index_to_store, ttl=ttl)

        await self._cache_entity_batch(page.items, profile=profile)
        return page

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        is_cursor = query.pagination == PaginationMode.CURSOR
        # For cursor pagination, cache only the first page to avoid
        # unbounded key growth and low-hit deep-page caches.
        should_cache = not is_cursor or query.cursor is None
        if not should_cache:
            return await self._repository.list_with_query(query, profile=profile)

        query_payload = self._serialize_query(query)
        query_fingerprint = stable_hash(repr(query_payload))
        tags = self._resolver.list_tags(self.entity_name, query_fingerprint)
        tags.extend(self._list_dependency_tags())
        deps_fingerprint = await self._resolver.fingerprint(tags)
        query_key = (
            f"{self.entity_name}:query:{query_fingerprint}:"
            f"profile={profile}:deps={deps_fingerprint}"
        )

        cached_index = await self._cache.get_value(query_key, type=_QueryIndexPayload)
        entity_ids = None if cached_index is None else self._index_ids(cached_index.ids)
        if cached_index is not None and entity_ids is not None:
            items = await self._load_items_from_index(entity_ids, profile=profile)
            if len(items) == len(entity_ids):
                self._log.debug("CacheHitQuery", key=query_key)
                if is_cursor:
                    return CursorResult(
                        items=tuple(items),
                        next_cursor=cached_index.next_cursor,
                        has_next=cached_index.has_next,
                    )
                return PageResult(
                    items=tuple(items),
                    total_count=0 if cached_index.total_count is None else cached_index.total_count,
                    page=query.page,
                    limit=query.limit,
                    has_next=cached_index.has_next,
                )

        self._log.debug("CacheMissQuery", key=query_key)
        loaded = await self._repository.list_with_query(query, profile=profile)
        ids = [
            cast(IdT, entity_id)
            for item in loaded.items
            for entity_id in [self._extract_entity_id(item)]
            if entity_id is not None
        ]
        ttl = self._write_ttl(self._config.ttl_for_list(self.entity_name))
        if isinstance(loaded, CursorResult):
            await self._cache.set_value(
                query_key,
                _QueryIndexPayload(
                    ids=ids,
                    next_cursor=loaded.next_cursor,
                    has_next=loaded.has_next,
                ),
                ttl=ttl,
            )
            await self._cache_entity_batch(loaded.items, profile=profile)
            return loaded

        await self._cache.set_value(
            query_key,
            _QueryIndexPayload(
                ids=ids,
                total_count=loaded.total_count,
                has_next=loaded.has_next,
            ),
            ttl=ttl,
        )
        await self._cache_entity_batch(loaded.items, profile=profile)
        return loaded

    async def create(self, data: CreateT) -> OutputT:
        created = await self._repository.create(data)
        entity_id = self._extract_entity_id(created)
        await self._publish_invalidation(
            MutationEvent(
                entity=self.entity_name,
                op="create",
                ids=() if entity_id is None else (entity_id,),
                changed_fields=frozenset(self._struct_keys(data)),
            )
        )
        return created

    async def update(self, obj_id: IdT, data: UpdateT) -> OutputT | None:
        updated = await self._repository.update(obj_id, data)
        if updated is None:
            return None
        await self._publish_invalidation(
            MutationEvent(
                entity=self.entity_name,
                op="update",
                ids=(obj_id,),
                changed_fields=frozenset(self._struct_keys(data)),
            )
        )
        return updated

    async def delete(self, obj_id: IdT) -> bool:
        deleted = await self._repository.delete(obj_id)
        if deleted:
            await self._publish_invalidation(
                MutationEvent(
                    entity=self.entity_name,
                    op="delete",
                    ids=(obj_id,),
                )
            )
        return deleted

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        """Bump the tags of the mixins' events and forward them downstream.

        Under ``@transactional`` the mixins' tagged events arrive here in the
        plain post-commit lane, while the wrapper's own event runs in the
        shielded priority lane queued by ``create``/``update``/``delete``.
        Both bumps are wanted: they are idempotent, and only this lane carries
        the relation/projection tags the wrapper cannot compute.

        Args:
            events: Mutation events committed by the wrapped repository.
        """
        await self._resolver.bump_from_events(events)
        post_commit = getattr(self._repository, "on_transaction_committed", None)
        if inspect.iscoroutinefunction(post_commit):
            handler = cast(Callable[[tuple[MutationEvent, ...]], Awaitable[None]], post_commit)
            await handler(events)

    def __getattr__(self, name: str) -> Any:
        """Resolve *name* on the wrapped repository, then adapt it.

        The lookup happens on the wrapped repository first, so a missing
        attribute raises there and ``hasattr(wrapper, "create_many") is
        hasattr(inner, "create_many")`` holds. ``create_many`` is intercepted
        to emit a mutation event after the bulk write; coroutines marked
        with ``@cache_query`` are wrapped with cache-aside behaviour; anything
        else is returned as-is.
        """
        attr = getattr(self._repository, name)
        if name == "create_many":
            return self._create_many
        if not callable(attr):
            return attr

        metadata = getattr(attr, "__cache_query__", None)
        if metadata is None:
            return attr
        if inspect.iscoroutinefunction(attr):
            return self._cached_method(
                name,
                cast(Callable[..., Awaitable[Any]], attr),
                metadata,
            )
        return attr

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_result_codecs(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
    ) -> dict[str, ResultCodec]:
        """Build one result codec per cached read, at construction time.

        The *methods* are collected from the class dictionaries of the wrapped
        repository's MRO, never by ``getattr`` on the instance: a repository is
        free to expose a property that opens a connection, and walking the
        instance would evaluate it while the application boots. Only the model
        is read off the instance, as the rest of the wrapper already does.

        Shadowing is tracked apart from selection: a subclass that overrides a
        cached read *without* the marker takes the name out of the cache, so
        the base class's decorated version must not be registered — the
        wrapper re-reads the marker on the resolved attribute and would never
        use that codec, but it would still deprecate the base annotation.
        """
        owner = type(repository)
        model = getattr(repository, "model", None)
        codecs: dict[str, ResultCodec] = {}
        declared: set[str] = set()
        for klass in owner.__mro__:
            for name, attr in vars(klass).items():
                if name in declared:
                    continue
                declared.add(name)
                if _is_cached_read(attr):
                    codecs[name] = self._codec_for(owner.__name__, name, attr, model)
        return codecs

    def _codec_for(
        self,
        owner_name: str,
        method_name: str,
        method: Callable[..., Any],
        model: object | None,
    ) -> ResultCodec:
        """Codec declared by *method*, or the deprecated pass-through."""
        codec = build_result_codec(method, model=model)
        if codec is not None:
            return codec
        warnings.warn(
            f"{owner_name}.{method_name} is decorated with @cache_query but its return "
            "annotation is missing, unresolvable, or outside the supported grammar "
            "(a struct, a scalar, or a list, tuple or optional of those). The cached "
            "call keeps returning the decoded payload, which may not be the type the "
            "first call returns; annotate the method to get one type on both paths. "
            "The pass-through will be removed in a future release.",
            DeprecationWarning,
            stacklevel=4,
        )
        self._log.warning(
            "CacheQueryReturnTypeUnusable",
            repository=owner_name,
            method=method_name,
        )
        return self._passthrough_codec

    async def _coalesced(self, key: str, loader: Callable[[], Awaitable[LoadT]]) -> LoadT:
        """Run *loader* once per key, unless the caller owns the session it uses.

        Coalescing detaches the load into its own task so it survives the
        cancellation of the caller that started it.  Inside a caller-scoped
        transaction that is exactly wrong: the task would inherit a session
        that is closed when the caller unwinds, and a waiter from another
        request would read the caller's uncommitted writes.  The load then runs
        inline, as it did before coalescing existed.
        """
        if self._runs_in_caller_scoped_transaction():
            return await loader()
        return await self._single_flight.run(key, loader)

    def _runs_in_caller_scoped_transaction(self) -> bool:
        """Whether the wrapped repository reports a session owned by the caller.

        The capability is optional: a repository that does not declare
        :class:`~loom.core.repository.abc.session_scope.SupportsCallerScopedSession`
        is taken to own the scope of its own reads.

        This wrapper asks "am I in a transaction?" two ways for two different
        reasons: this one guards coalescing (may a read safely detach into
        its own task?), while :func:`loom.core.transaction.in_atomic_transaction`
        in :meth:`_publish_invalidation` guards the bump (must it wait for a
        commit?). They read different signals and answer different questions;
        neither substitutes for the other.
        """
        repository = self._repository
        if not isinstance(repository, SupportsCallerScopedSession):
            return False
        return repository.has_caller_scoped_session()

    def _log_abandoned_load(self, key: str, error: BaseException) -> None:
        self._log.warning("CacheLoadAbandoned", key=key, error=repr(error))

    async def _publish_invalidation(self, event: MutationEvent) -> None:
        """Defer the bump past the commit, or bump inline right away.

        Bumps inline for either of two distinct reasons:

        * No transaction is open: every write without one commits on its
          own — SQLAlchemy's own session on exit, Mongo's no-op unit of work
          and DynamoDB on every call — so there is nothing left to wait for.
        * A transaction *is* open but no post-commit channel is bound: a
          unit of work entered directly — ``async with
          SQLAlchemyUnitOfWork(session_manager): await repo.create(...)``,
          the style both units of work document — opens the transaction
          signal without binding a channel, since only the executor and
          ``@transactional`` do that. Deferring here would queue an action
          nothing ever drains, losing the invalidation permanently; bumping
          early and risking the F03 race this method otherwise closes is the
          lesser fault of the two.

        Otherwise deferred through
        :meth:`~loom.core.engine.post_commit.PostCommitChannel.enqueue_priority`
        so the bump always precedes any job dispatch queued during the same
        pipeline (:mod:`loom.core.job.context`), which might otherwise read
        the cache before this invalidation reaches it. That lane runs
        shielded inside :meth:`~loom.core.engine.post_commit.PostCommitChannel.drain`,
        so a cancellation reaching the drain cannot interrupt this bump —
        only a process death between the commit and the drain can still lose
        it, nothing else; the affected entries' TTL is the backstop for
        that one remaining gap.
        """
        channel = active_channel() if in_atomic_transaction() else None
        if channel is None:
            await self._resolver.bump_from_events((event,))
            return
        channel.enqueue_priority(partial(self._bump_logging_failure, (event,)))

    async def _bump_logging_failure(self, events: tuple[MutationEvent, ...]) -> None:
        """Bump ``events``, logging entity and ids first: the write already committed."""
        try:
            await self._resolver.bump_from_events(events)
        except Exception:
            self._log.exception(
                "CachePostCommitBumpFailed",
                entity=self.entity_name,
                ids=tuple(event.ids for event in events),
            )
            raise

    async def _load_and_store_entity(self, key: str, obj_id: IdT, profile: str) -> OutputT | None:
        """Read one entity from the wrapped repository and cache it."""
        loaded = await self._repository.get_by_id(obj_id, profile=profile)
        if loaded is None:
            return None
        ttl = self._write_ttl(self._config.ttl_for_single(self.entity_name))
        await self._cache.set_value(key, self._entity_payload(loaded), ttl=ttl)
        return loaded

    def _entity_payload(self, item: Any) -> Any:
        """Render *item* for storage, through the declared pydantic type when strict.

        A subclass instance carrying an extra field is rendered in the
        declared shape, so the strict hit-path validation still accepts it.
        """
        if self._pydantic_output_type is not None:
            return self._pydantic_output_type.to_builtins(item)
        return to_payload(item)

    def _write_ttl(self, ttl: int) -> int:
        """Spread *ttl* inside the configured jitter band.

        The resolved TTL is deterministic; the spread is applied only where the
        value reaches the backend, so a burst of writes does not expire at the
        same instant.  The result is never below one second, and with
        ``ttl_jitter`` at zero it is the resolved value itself.  The generator
        is owned by this wrapper, so seeding the process-wide ``random`` module
        does not freeze every TTL in the process.
        """
        jitter = self._config.ttl_jitter
        if jitter <= 0.0:
            return ttl
        spread = ttl * jitter
        return max(1, round(ttl + self._rng.uniform(-spread, spread)))

    def _extract_entity_id(self, item: Any) -> object | None:
        """Read the primary key of *item* by the attributes the model declares.

        Returns the value of a single-column key and the tuple of every value
        of a composite one, or ``None`` when a key value is missing.
        """
        values = tuple(getattr(item, name, None) for name in self._primary_key.attributes)
        if None in values:
            return None
        return values[0] if len(values) == 1 else values

    async def _create_many(self, data: Sequence[msgspec.Struct]) -> tuple[OutputT, ...]:
        if not data:
            return ()
        bulk = cast(BulkCreatable[OutputT], self._repository)
        created = await bulk.create_many(data)
        ids = tuple(
            entity_id
            for item in created
            for entity_id in [self._extract_entity_id(item)]
            if entity_id is not None
        )
        changed_fields = frozenset(key for item in data for key in self._struct_keys(item))
        await self._publish_invalidation(
            MutationEvent(
                entity=self.entity_name,
                op="create",
                ids=ids,
                changed_fields=changed_fields,
            )
        )
        return created

    def _cached_method(
        self,
        method_name: str,
        method: Callable[..., Awaitable[Any]],
        metadata: dict[str, object],
    ) -> Callable[..., Awaitable[Any]]:
        """Return the delegator of a cached read, built once per method name.

        ``__getattr__`` runs on every access to a method the wrapper does not
        define, so rebuilding the delegator there would allocate a closure per
        call and make ``wrapper.method is wrapper.method`` false.
        """
        wrapper = self._cached_method_wrappers.get(method_name)
        if wrapper is None:
            wrapper = self._wrap_custom_cached_method(method_name, method, metadata)
            self._cached_method_wrappers[method_name] = wrapper
        return wrapper

    def _wrap_custom_cached_method(
        self,
        method_name: str,
        method: Callable[..., Awaitable[Any]],
        metadata: dict[str, object],
    ) -> Callable[..., Awaitable[Any]]:
        codec = self._result_codecs.get(method_name, self._passthrough_codec)

        @wraps(method)
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            key, ttl = await self._custom_cache_entry(method_name, metadata, args, kwargs)
            cached_payload = await self._cache.get_value(key)
            if cached_payload is not None:
                decoded = self._decode_cached_result(codec, method_name, key, cached_payload)
                if decoded is not _UNDECODABLE:
                    self._log.debug("CacheHitCustomMethod", key=key, method=method_name)
                    return decoded

            self._log.debug("CacheMissCustomMethod", key=key, method=method_name)

            async def load() -> Any:
                result = await method(*args, **kwargs)
                if result is None:
                    return None
                encoded = codec.encode(result)
                await self._cache.set_value(key, encoded.payload, ttl=self._write_ttl(ttl))
                return encoded.value

            return await self._coalesced(key, load)

        return wrapped

    def _decode_cached_result(
        self,
        codec: ResultCodec,
        method_name: str,
        key: str,
        payload: Any,
    ) -> Any:
        """Decode a cached payload, or report it unusable.

        A payload written by an earlier version of the code no longer fits a
        return type that has since gained a field or narrowed one, and the
        cache key hashes the call arguments only, so nothing about the entry
        changes when the type does. Raising here would fail every caller of a
        warm key until its TTL ran out — a timed outage on a rolling deploy,
        where both versions are live. The entry is treated as a miss instead:
        the load that follows overwrites it, so the two paths still agree on
        the type.

        Returns:
            The decoded value, or the ``_UNDECODABLE`` sentinel, which is not
            ``None`` because ``None`` is a value the pass-through codec can
            legitimately return.
        """
        try:
            return codec.decode(payload)
        except BoundaryValidationError as error:
            self._log.warning(
                "CacheCustomPayloadMismatch",
                key=key,
                method=method_name,
                error=repr(error),
            )
            return _UNDECODABLE

    async def _custom_cache_entry(
        self,
        method_name: str,
        metadata: dict[str, object],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> tuple[str, int]:
        """Resolve the cache key and the TTL of one call to a cached read."""
        scope = cast(str, metadata.get("scope") or "list")
        ttl_key = cast(str | None, metadata.get("ttl_key"))
        raw = {"args": to_payload(args), "kwargs": to_payload(kwargs)}
        raw_hash = stable_hash(repr(raw))

        if scope == "entity":
            entity_id = self._entity_scope_id(method_name, args)
            tags = self._resolver.entity_tags(self.entity_name, entity_id)
            tags.extend(self._entity_dependency_tags(entity_id))
            ttl = self._config.ttl_for_single(ttl_key or self.entity_name)
        else:
            tags = self._resolver.list_tags(self.entity_name, raw_hash)
            tags.extend(self._list_dependency_tags())
            ttl = self._config.ttl_for_list(ttl_key or self.entity_name)

        fingerprint = await self._resolver.fingerprint(tags)
        return f"{self.entity_name}:custom:{method_name}:{raw_hash}:deps={fingerprint}", ttl

    def _entity_scope_id(self, method_name: str, args: tuple[Any, ...]) -> object:
        """Return the primary key a ``scope="entity"`` read is keyed by, or raise.

        The key must be the first positional argument (a keyword does not
        count) and must not be ``None``; a composite key must be a tuple of
        one value per key attribute.  Where a key attribute's type resolved
        to a plain class, its value must be of that exact type: a
        ``datetime`` for a ``date`` key or a ``bool`` for an ``int`` key
        would render a tag no mutation event ever bumps, so both are
        rejected even though ``isinstance`` would accept them.  An attribute
        declared ``int | None`` resolves no class and gets no type check.
        Only this entry point is guarded: ``get_by_id``, ``update`` and
        ``delete`` are typed ``IdT`` and stay unchecked.

        Raises:
            TypeError: When the key is missing, ``None``, or of another type.
        """
        if not args or args[0] is None:
            raise TypeError(
                f"{method_name}: @cache_query(scope='entity') needs the primary key"
                " as first positional argument"
            )
        types = self._primary_key.python_types
        composite = len(types) > 1
        values = args[0] if composite and isinstance(args[0], tuple) else (args[0],)
        if len(values) != len(types) or any(
            expected is not None and type(value) is not expected
            for value, expected in zip(values, types, strict=True)
        ):
            names = ", ".join("Any" if t is None else t.__name__ for t in types)
            raise TypeError(
                f"{method_name}: @cache_query(scope='entity') received a"
                f" {type(args[0]).__name__} as primary key,"
                f" expected {f'tuple[{names}]' if composite else names}"
            )
        return args[0]

    async def _load_items_from_index(self, ids: list[IdT], profile: str) -> list[OutputT]:
        tags_by_id = [
            self._resolver.entity_tags(self.entity_name, eid) + self._entity_dependency_tags(eid)
            for eid in ids
        ]
        fingerprints = await self._resolve_fingerprints(tags_by_id)
        entity_keys = [
            entity_key(self.entity_name, eid, profile, fp)
            for eid, fp in zip(ids, fingerprints, strict=True)
        ]
        cached_values = await self._cache.multi_get_values(entity_keys)
        restored = self._restore_cached_page(cached_values)
        missing = [
            (position, ids[position]) for position, item in enumerate(restored) if item is None
        ]
        if not missing:
            return cast(list[OutputT], restored)

        loaded_by_id = await self._fetch_missing_by_ids(
            [missing_id for _, missing_id in missing],
            profile=profile,
        )
        refill: list[tuple[str, OutputT]] = []
        for position, missing_id in missing:
            loaded = loaded_by_id.get(missing_id)
            if loaded is None:
                # The index points at an id the backend no longer returns:
                # let the caller fall back to its own repository query.
                return []
            restored[position] = loaded
            refill.append((entity_keys[position], loaded))

        await self._refill_cache(refill)
        return cast(list[OutputT], restored)

    def _restore_cached_page(self, cached_values: list[Any]) -> list[OutputT | None]:
        """Rebuild each cached payload, leaving ``None`` where the entry is unusable."""
        return [
            None if value is None else self._to_output_from_cache(value) for value in cached_values
        ]

    async def _refill_cache(self, entries: list[tuple[str, OutputT]]) -> None:
        """Write back only the entities that were missing, leaving warm TTLs alone."""
        ttl = self._write_ttl(self._config.ttl_for_single(self.entity_name))
        pairs = [(key, self._entity_payload(item)) for key, item in entries]
        await self._cache.multi_set_values(pairs, ttl=ttl)

    async def _fetch_missing_by_ids(
        self,
        missing_ids: list[IdT],
        profile: str,
    ) -> dict[object, OutputT]:
        """Reload the entities missing from the cache, keyed by their id."""
        if len(missing_ids) == 1 or not self._supports_id_filter():
            return await self._fetch_missing_one_by_one(missing_ids, profile)
        return await self._fetch_missing_with_query(missing_ids, profile)

    def _supports_id_filter(self) -> bool:
        """Whether the wrapped repository accepts a filter on the primary key.

        A repository may restrict the filterable fields (``allowed_filter_fields``).
        When the primary-key attribute is not among them the batched refill would
        be rejected, so the wrapper degrades to per-id reads rather than failing
        the page; the repository's declared policy is left untouched.  A
        composite key is always read per id, since a filter names one field.
        """
        attributes = self._primary_key.attributes
        allowed = getattr(self._repository, "allowed_filter_fields", None)
        return len(attributes) == 1 and (not allowed or attributes[0] in allowed)

    async def _fetch_missing_one_by_one(
        self,
        missing_ids: Sequence[IdT],
        profile: str,
    ) -> dict[object, OutputT]:
        loaded: dict[object, OutputT] = {}
        for missing_id in missing_ids:
            item = await self._repository.get_by_id(missing_id, profile=profile)
            if item is not None:
                loaded[missing_id] = item
        return loaded

    async def _fetch_missing_with_query(
        self,
        missing_ids: Sequence[IdT],
        profile: str,
    ) -> dict[object, OutputT]:
        loaded: dict[object, OutputT] = {}
        for batch in batched(missing_ids, REFILL_BATCH_SIZE):
            # Sequential on purpose: the wrapped repository may hold a single
            # session, which is not safe to drive concurrently.
            page = await self._repository.list_with_query(self._id_in_query(batch), profile=profile)
            for item in page.items:
                entity_id = self._extract_entity_id(item)
                if entity_id is not None:
                    loaded[entity_id] = item
        return loaded

    def _id_in_query(self, ids: Sequence[IdT]) -> QuerySpec:
        return QuerySpec(
            filters=FilterGroup(
                filters=(
                    FilterSpec(field=self._primary_key.attributes[0], op=FilterOp.IN, value=ids),
                )
            ),
            limit=len(ids),
        )

    async def _resolve_fingerprints(self, tags_by_id: list[list[str]]) -> list[str]:
        """Resolve one fingerprint per tag group, batched when the resolver allows it."""
        resolver = self._resolver
        if isinstance(resolver, BatchFingerprintResolver):
            return await resolver.fingerprint_many(tags_by_id)
        return list(await asyncio.gather(*(resolver.fingerprint(tags) for tags in tags_by_id)))

    async def _cache_entity_batch(self, items: Sequence[OutputT], profile: str) -> None:
        keyed = self._items_with_key(items)
        if not keyed:
            return
        ttl = self._write_ttl(self._config.ttl_for_single(self.entity_name))
        tags_by_id = [
            self._resolver.entity_tags(self.entity_name, entity_id)
            + self._entity_dependency_tags(entity_id)
            for _item, entity_id in keyed
        ]
        fingerprints = await self._resolve_fingerprints(tags_by_id)

        pairs: list[tuple[str, Any]] = []
        for (item, entity_id), fingerprint in zip(keyed, fingerprints, strict=True):
            key = entity_key(self.entity_name, entity_id, profile, fingerprint)
            pairs.append((key, self._entity_payload(item)))
        await self._cache.multi_set_values(pairs, ttl=ttl)

    def _items_with_key(self, items: Sequence[OutputT]) -> list[tuple[OutputT, object]]:
        """Pair each item with its primary key, leaving out the ones without one.

        An item whose key resolves to ``None`` would be cached under a key that
        no per-id bump ever reaches, so it is not cached at all.
        """
        keyed: list[tuple[OutputT, object]] = []
        for item in items:
            entity_id = self._extract_entity_id(item)
            if entity_id is not None:
                keyed.append((item, entity_id))
        return keyed

    def _to_output_from_cache(self, payload: Any) -> OutputT | None:
        if payload is None:
            return None
        if isinstance(payload, msgspec.Struct):
            return cast(OutputT, payload)
        if isinstance(payload, Mapping):
            builder = getattr(self._repository, "to_output_from_payload", None)
            if callable(builder):
                return cast(OutputT, builder(payload))
            if self._pydantic_output_type is not None:
                return cast(OutputT, self._pydantic_output_type.from_builtins(payload))
            model = getattr(self._repository, "model", None)
            if isinstance(model, type) and issubclass(model, msgspec.Struct):
                return cast(OutputT, to_struct(model, payload))
        return cast(OutputT, payload)

    def _index_ids(self, raw_ids: list[Any]) -> list[IdT] | None:
        """Restore the ids of a cached index to the model's primary-key type.

        The index round-trips through msgpack, which renders a ``datetime``, a
        ``date`` or a ``UUID`` as a string and a tuple as a list, while the
        repository binds the declared type: SQLAlchemy returns no rows for a
        ``datetime`` column filtered with strings, and the SQLAlchemy
        repository takes a composite key only as a tuple.  Converting here
        keeps every later use of the id — cache key, filter value, identity
        match — in the declared type.

        Returns:
            The converted ids, or ``None`` when they do not fit the current
            primary-key type, which makes the index unusable.
        """
        types = self._primary_key.python_types
        id_type: Any = types[0]
        if len(types) > 1:
            id_type = GenericAlias(tuple, tuple(Any if t is None else t for t in types))
        if id_type is None:
            return cast(list[IdT], raw_ids)
        try:
            return [cast(IdT, msgspec.convert(value, id_type)) for value in raw_ids]
        except msgspec.ValidationError:
            self._log.debug("CacheIndexIdTypeMismatch", entity=self.entity_name)
            return None

    def _list_dependency_tags(self) -> list[str]:
        tags: list[str] = []
        for dependency in self._depends_on:
            tags.append(dependency.entity)
            tags.append(f"{dependency.entity}:list")
        return tags

    def _entity_dependency_tags(self, obj_id: object) -> list[str]:
        tags: list[str] = []
        for dependency in self._depends_on:
            tags.extend(
                (
                    dependency.entity,
                    f"{dependency.entity}:list",
                    f"{dependency.entity}:{dependency.fk_field}:{obj_id}",
                )
            )
        return tags

    def _parse_dependency_specs(self, specs: tuple[str, ...]) -> list[_DependencySpec]:
        deps: list[_DependencySpec] = []
        for spec in specs:
            if ":" not in spec:
                msg = f"Invalid dependency spec '{spec}'. Expected '<entity>:<fk_field>'"
                raise ValueError(msg)
            entity_name, fk_field = spec.split(":", 1)
            entity = entity_name.strip()
            field = fk_field.strip()
            if not entity or not field:
                raise ValueError(f"Invalid dependency spec '{spec}'. Empty entity or fk field")
            deps.append(_DependencySpec(entity=entity, fk_field=field))
        return deps

    def _collect_dependency_specs(
        self,
        repository: Repository[OutputT, CreateT, UpdateT, IdT],
    ) -> tuple[str, ...]:
        specs: list[str] = list(getattr(repository, "depends_on", ()))
        model = getattr(repository, "model", None)
        if model is None:
            return tuple(specs)

        for attr_name, rel in get_relations(model).items():
            if rel.depends_on:
                specs.extend(rel.depends_on)
            else:
                inferred = _infer_otm_cache_dep(model, attr_name, rel)
                if inferred is not None:
                    specs.append(inferred)

        for proj in get_projections(model).values():
            if proj.depends_on:
                specs.extend(proj.depends_on)
            else:
                inferred = _infer_projection_cache_dep(model, proj)
                if inferred is not None:
                    specs.append(inferred)

        return tuple(dict.fromkeys(specs))

    def _serialize_filters(self, filter_params: FilterParams | None) -> str:
        if filter_params is None:
            return "{}"
        return repr(to_payload(filter_params.filters))

    def _serialize_query(self, query: QuerySpec) -> dict[str, Any]:
        return {
            "pagination": query.pagination.value,
            "limit": query.limit,
            "page": query.page,
            "cursor": query.cursor,
            "sort": [{"field": sort.field, "direction": sort.direction} for sort in query.sort],
            "filters": self._serialize_filter_group(query.filters),
        }

    def _serialize_filter_group(self, group: FilterGroup | None) -> dict[str, Any] | None:
        if group is None:
            return None
        return {
            "op": group.op,
            "filters": [
                {
                    "field": item.field,
                    "op": item.op.value,
                    "value": to_payload(item.value),
                }
                for item in group.filters
            ],
        }

    def _struct_keys(self, payload: msgspec.Struct) -> list[str]:
        data = msgspec.to_builtins(payload)
        if isinstance(data, dict):
            return list(data.keys())
        return []


def _protocol_surface(protocol: type) -> frozenset[str]:
    """Return the public method names *protocol* and its bases declare."""
    return frozenset(
        name
        for klass in protocol.__mro__
        for name, attr in vars(klass).items()
        if not name.startswith("_") and inspect.isfunction(attr)
    )


def _derive_delegated_methods() -> tuple[str, ...]:
    """Return the ``Repository`` methods :class:`CachedRepository` overrides.

    Derived from the class itself rather than transcribed, so an override added
    later joins the precondition with no list to update.  Declaration order is
    preserved, so a refusal names the missing methods in the order the wrapper
    declares them.
    """
    surface = _protocol_surface(Repository)
    return tuple(
        name
        for name, attr in vars(CachedRepository).items()
        if name in surface and inspect.isfunction(attr)
    )


_DELEGATED_METHODS: tuple[str, ...] = _derive_delegated_methods()
"""Methods the wrapper overrides, and therefore requires of what it wraps."""


def _require_full_repository(repository: object) -> None:
    """Check that a ``@cached`` *repository* provides every delegated method.

    :class:`CachedRepository` satisfies the whole ``Repository`` protocol, so
    wrapping a read-only object would advertise write methods that fail with an
    opaque ``AttributeError`` on the first call.  Refusing at construction names
    the class and what it lacks instead, at boot rather than in traffic.

    Only a class that declared itself cacheable is held to the whole protocol:
    a partial repository that never asked for the wrapper —
    :class:`~loom.core.repository.dynamodb.repository.RepositoryDynamoDB`, which
    serves no list and no count — may still be wrapped by hand for the reads it
    does support.

    Args:
        repository: The instance about to be wrapped.

    Raises:
        RuntimeError: If the class is marked ``@cached`` and a delegated method
            is missing.
    """
    if not declares_cache_policy(type(repository)):
        return
    missing = [name for name in _DELEGATED_METHODS if not callable(getattr(repository, name, None))]
    if missing:
        raise RuntimeError(
            f"{type(repository).__qualname__} cannot be wrapped in CachedRepository: "
            f"it does not implement {', '.join(missing)}. A @cached repository must "
            f"implement the full Repository protocol."
        )
