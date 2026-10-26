"""The cache module wraps every ``@cached`` repository the registration module builds.

AC2, AC3 and AC4 of spec 009: the decorator hook fires once per model
whichever module is loaded first, the wrapped instance is shared by every key
of the model, invalidation lands in the counter alias, not the data one, and a
deployment without the section learns at boot which marked classes run uncached.
AC14 of spec 010 rides along: the same module binds the ``CachedCalls`` a
``kind: python`` factory resolves.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import ExitStack
from typing import Any

import msgspec
import pytest
from aiocache import caches

from loom.core.cache import (
    CacheConfig,
    CachedCalls,
    CachedRepository,
    cache_call,
    cached_calls,
)
from loom.core.cache.calls import AsyncCallable, _ConfiguredCalls, _UnconfiguredCalls
from loom.core.cache.decorators import cached
from loom.core.cache.keys import call_key
from loom.core.cache.wiring import CacheGateways, cache_module, cache_module_for
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.model.redaction import RedactedMap
from loom.core.repository import (
    RepositoryBuildContext,
    RepositoryDecorator,
    RepositoryRegistration,
    build_repository_registration_module,
)
from loom.core.repository.abc.repo_for import Listable, Readable

from ._doubles import (
    MEMORY_BACKEND,
    SERIALIZED_BACKEND,
    CachedWidgetRepository,
    CodeWidget,
    Widget,
    WidgetUpdate,
    WritableRepository,
    open_transaction_with_channel,
    registered_repository,
)


def _backend(alias: str) -> Any:
    """Return the aiocache instance behind ``alias`` (typed ``object`` by aiocache's stubs)."""
    return caches.get(alias)


Module = Callable[[LoomContainer], None]


class PlainCodeWidgetRepository(
    WritableRepository[CodeWidget], Readable[CodeWidget], Listable[CodeWidget]
):
    """Unmarked double with the same capability keys as the cached one."""

    def __init__(self, rows: Sequence[CodeWidget] = ()) -> None:
        super().__init__(rows, CodeWidget)


class _SpyBuilder:
    """Repository builder that keeps every instance it built."""

    def __init__(self) -> None:
        self.built: list[Any] = []

    def __call__(
        self, context: RepositoryBuildContext, registration: RepositoryRegistration
    ) -> Any:
        rows = [Widget(id=1, name="one")] if registration.model is Widget else []
        repository = registration.repository_type(rows)
        self.built.append(repository)
        return repository


class _Tagged:
    """What a user-registered decorator returns."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository


def _user_decorator(repository: Any) -> Any:
    return _Tagged(repository)


class _TaggingDecorator:
    """User-registered decorator whose class name the warning must report."""

    def __call__(self, repository: Any) -> Any:
        return _Tagged(repository)


@cached
class ReadOnlyWidgetRepository:
    """Marked double that only reads, so the wrapper would advertise writes it lacks."""

    def __init__(self, rows: Sequence[Widget] = ()) -> None:
        self.rows = list(rows)

    async def get_by_id(self, obj_id: int, profile: str = "default") -> Widget | None:
        return next((row for row in self.rows if row.id == obj_id), None)


def _single_alias() -> CacheConfig:
    return CacheConfig(aiocache_config=RedactedMap({"default": dict(SERIALIZED_BACKEND)}))


def _two_aliases() -> CacheConfig:
    return CacheConfig(
        aiocache_alias="data",
        counter_alias="counters",
        aiocache_config=RedactedMap(
            {"data": dict(SERIALIZED_BACKEND), "counters": dict(MEMORY_BACKEND)}
        ),
    )


@pytest.fixture
def builder() -> _SpyBuilder:
    return _SpyBuilder()


@pytest.fixture
def repository_module(builder: _SpyBuilder) -> Iterator[Module]:
    with ExitStack() as stack:
        stack.enter_context(registered_repository(Widget, CachedWidgetRepository))
        stack.enter_context(registered_repository(CodeWidget, PlainCodeWidgetRepository))
        yield build_repository_registration_module(
            models=(),
            explicit_models=(Widget, CodeWidget),
            build_registered_repository=builder,
        )


def _container(*modules: Module) -> LoomContainer:
    container = LoomContainer()
    for module in modules:
        module(container)
    return container


def _in_either_order(first: Module, second: Module, cache_first: bool) -> LoomContainer:
    return _container(second, first) if cache_first else _container(first, second)


class TestDecoration:
    @pytest.mark.parametrize("cache_first", [False, True])
    def test_a_marked_repository_is_served_wrapped(
        self, repository_module: Module, cache_first: bool
    ) -> None:
        container = _in_either_order(repository_module, cache_module(_single_alias()), cache_first)

        assert isinstance(container.resolve_repo(Widget), CachedRepository)

    def test_every_key_of_the_model_resolves_the_same_instance(
        self, repository_module: Module
    ) -> None:
        container = _container(repository_module, cache_module(_single_alias()))

        primary = container.resolve_repo(Widget)

        assert container.resolve(Readable[Widget]) is primary
        assert container.resolve(Listable[Widget]) is primary

    @pytest.mark.parametrize("cache_first", [False, True])
    def test_an_unmarked_repository_is_served_bare(
        self, repository_module: Module, cache_first: bool
    ) -> None:
        container = _in_either_order(repository_module, cache_module(_single_alias()), cache_first)

        primary = container.resolve_repo(CodeWidget)

        assert isinstance(primary, PlainCodeWidgetRepository)
        assert container.resolve(Readable[CodeWidget]) is primary
        assert container.resolve(Listable[CodeWidget]) is primary

    def test_a_decorator_registered_before_the_cache_module_is_kept(
        self, repository_module: Module
    ) -> None:
        container = LoomContainer()
        container.register_instance(RepositoryDecorator, _user_decorator)
        cache_module(_single_alias())(container)
        repository_module(container)

        assert container.resolve(RepositoryDecorator) is _user_decorator
        assert isinstance(container.resolve_repo(Widget), _Tagged)

    def test_names_the_decorator_that_already_held_the_slot(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = LoomContainer()
        container.register_instance(RepositoryDecorator, _TaggingDecorator())

        with caplog.at_level(logging.WARNING, logger="loom.core.cache.wiring"):
            cache_module(_single_alias())(container)

        warnings = [
            record
            for record in caplog.records
            if "CacheDecoratorAlreadyRegistered" in record.getMessage()
        ]
        assert len(warnings) == 1
        assert "_TaggingDecorator" in warnings[0].getMessage()

    def test_two_aliases_open_two_gateways(self) -> None:
        container = _container(cache_module(_two_aliases()))

        gateways = container.resolve(CacheGateways)

        assert gateways.counters is not gateways.data
        assert list(gateways.distinct()) == [gateways.data, gateways.counters]

    def test_one_alias_opens_a_single_gateway(self) -> None:
        container = _container(cache_module(_single_alias()))

        gateways = container.resolve(CacheGateways)

        assert gateways.counters is gateways.data
        assert list(gateways.distinct()) == [gateways.data]


class TestIncompleteRepository:
    def test_refuses_a_marked_repository_that_cannot_be_delegated_to(self) -> None:
        with registered_repository(Widget, ReadOnlyWidgetRepository):
            module = build_repository_registration_module(
                models=(),
                explicit_models=(Widget,),
                build_registered_repository=_SpyBuilder(),
            )
            container = _container(module, cache_module(_single_alias()))

            with pytest.raises(RuntimeError, match="ReadOnlyWidgetRepository") as info:
                container.resolve_repo(Widget)

        assert "create" in str(info.value)
        assert "list_paginated" in str(info.value)

    def test_boot_validation_names_the_offending_repository(self) -> None:
        """``validate()`` wraps the refusal, so the aborted boot still names the class."""
        with registered_repository(Widget, ReadOnlyWidgetRepository):
            module = build_repository_registration_module(
                models=(),
                explicit_models=(Widget,),
                build_registered_repository=_SpyBuilder(),
            )
            container = _container(module, cache_module(_single_alias()))

            with pytest.raises(ResolutionError, match="ReadOnlyWidgetRepository") as info:
                container.validate()

        assert "create" in str(info.value)
        assert "list_paginated" in str(info.value)


class TestInvalidationThroughTheContainer:
    async def test_the_second_read_is_served_from_the_cache(
        self, repository_module: Module, builder: _SpyBuilder
    ) -> None:
        container = _container(repository_module, cache_module(_two_aliases()))
        wrapped = container.resolve_repo(Widget)
        inner = builder.built[0]
        assert isinstance(inner, CachedWidgetRepository)

        await wrapped.get_by_id(1)
        await wrapped.get_by_id(1)

        assert inner.get_by_id_calls == 1

    async def test_a_committed_write_sends_the_next_read_through(
        self, repository_module: Module, builder: _SpyBuilder
    ) -> None:
        container = _container(repository_module, cache_module(_two_aliases()))
        wrapped = container.resolve_repo(Widget)
        inner = builder.built[0]
        await wrapped.get_by_id(1)

        with open_transaction_with_channel() as channel:
            await wrapped.update(1, WidgetUpdate(name="uno"))
        await channel.drain(committed=True)
        await wrapped.get_by_id(1)

        assert inner.get_by_id_calls == 2
        assert await _backend("counters").exists("tag:widget:id:1")
        assert await _backend("counters").exists("tag:widget:list")
        assert not await _backend("data").exists("tag:widget:id:1")
        assert not await _backend("data").exists("tag:widget:list")


def _not_configured_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING and "CacheNotConfigured" in record.getMessage()
    ]


class TestCacheNotConfigured:
    @pytest.fixture(autouse=True)
    def _capture_wiring_warnings(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.WARNING, logger="loom.core.cache.wiring")

    @pytest.mark.parametrize(
        "raw",
        [pytest.param({}, id="absent"), pytest.param({"cache": {"enabled": False}}, id="disabled")],
    )
    def test_boot_warns_once_for_a_marked_class_without_the_section(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture, raw: dict[str, Any]
    ) -> None:
        container = _container(repository_module, cache_module_for(ConfigContext.from_dict(raw)))

        container.validate()

        warnings = _not_configured_warnings(caplog)
        assert len(warnings) == 1
        assert "CachedWidgetRepository" in warnings[0].getMessage()
        assert isinstance(container.resolve_repo(Widget), CachedWidgetRepository)

    def test_resolving_after_validate_warns_no_further(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = _container(repository_module, cache_module_for(ConfigContext.from_dict({})))
        container.validate()
        caplog.clear()

        container.resolve_repo(Widget)
        container.resolve(Readable[Widget])

        assert _not_configured_warnings(caplog) == []

    def test_an_unmarked_class_is_never_announced(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = _container(repository_module, cache_module_for(ConfigContext.from_dict({})))
        container.validate()

        assert all(
            "PlainCodeWidgetRepository" not in record.getMessage()
            for record in _not_configured_warnings(caplog)
        )

    def test_an_enabled_section_wraps_and_stays_quiet(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture
    ) -> None:
        ctx = ConfigContext.from_dict(
            {"cache": {"aiocache_config": {"default": dict(SERIALIZED_BACKEND)}}}
        )
        container = _container(repository_module, cache_module_for(ctx))

        container.validate()

        assert isinstance(container.resolve_repo(Widget), CachedRepository)
        assert _not_configured_warnings(caplog) == []


class Answer(msgspec.Struct):
    """Return type inside the msgspec grammar, so the call really is cached."""

    text: str


@cache_call(ttl_key="web_search")
async def fetch_answer(query: str) -> Answer:
    """Marked coroutine the container's binder is asked to cache.

    Args:
        query: The only argument the key sees.

    Returns:
        An answer built from *query*.
    """
    return Answer(text=query)


class _UserCalls:
    """Binder a user module registered before loom's, which loom must keep."""

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        return func

    def bind(self, obj: object) -> list[AsyncCallable]:
        del obj
        return []


def _already_registered_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record for record in caplog.records if "CacheCallsAlreadyRegistered" in record.getMessage()
    ]


class TestCachedCallsBinding:
    """AC14: which ``CachedCalls`` a container hands to a ``kind: python`` factory."""

    def test_a_section_binds_the_configured_binder(self) -> None:
        container = _container(cache_module(_single_alias()))

        assert isinstance(cached_calls(container), _ConfiguredCalls)

    @pytest.mark.parametrize(
        "raw",
        [pytest.param({}, id="absent"), pytest.param({"cache": {"enabled": False}}, id="disabled")],
    )
    def test_no_usable_section_binds_the_pass_through(self, raw: dict[str, Any]) -> None:
        """Bound, not merely returned: the announcing module must claim the slot.

        ``cached_calls`` builds a fresh pass-through for a container that has
        no binding at all, so an ``isinstance`` check alone would pass with no
        registration whatsoever. The registration is what makes every factory
        in one container share a binder — and with it the once-set that keeps
        ``CacheCallNotConfigured`` to one record per marked coroutine.
        """
        container = _container(cache_module_for(ConfigContext.from_dict(raw)))

        assert container.is_registered(CachedCalls)
        assert isinstance(cached_calls(container), _UnconfiguredCalls)
        assert cached_calls(container) is cached_calls(container)

    def test_a_binder_registered_before_the_cache_module_is_kept(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = LoomContainer()
        user_calls = _UserCalls()
        container.register_instance(CachedCalls, user_calls)

        with caplog.at_level(logging.WARNING, logger="loom.core.cache.wiring"):
            cache_module(_single_alias())(container)

        assert cached_calls(container) is user_calls
        warnings = _already_registered_warnings(caplog)
        assert len(warnings) == 1
        assert "_UserCalls" in warnings[0].getMessage()

    def test_a_container_holding_a_decorator_still_gets_the_configured_binder(self) -> None:
        """The decorator block returns early; the binder must be bound before it."""
        container = LoomContainer()
        container.register_instance(RepositoryDecorator, _user_decorator)

        cache_module(_single_alias())(container)

        assert container.resolve(RepositoryDecorator) is _user_decorator
        assert isinstance(cached_calls(container), _ConfiguredCalls)

    async def test_the_configured_binder_writes_to_the_data_alias(self) -> None:
        """The binder is built over the data gateway, never the counter one.

        The counter alias carries generation numbers and is configured without
        the entity serializer; a payload written there is spec 009's separation
        broken, and an ``isinstance`` check cannot see which gateway went in.
        """
        container = _container(cache_module(_two_aliases()))

        answer = await cached_calls(container).wrap(fetch_answer)("q")

        assert answer == Answer(text="q")
        # The rendering the binder passes: every value labelled with its
        # qualified type, and the bound arguments as a list of pairs.
        key = call_key(
            module=fetch_answer.__module__,
            qualname=fetch_answer.__qualname__,
            version=1,
            arguments=["builtins.dict", [[["builtins.str", "query"], ["builtins.str", "q"]]]],
        )
        assert await _backend("data").exists(key)
        assert not await _backend("counters").exists(key)


async def _until(condition: Callable[[], bool]) -> None:
    """Yield the loop until *condition* holds, so a callback can run."""
    for _ in range(200):
        if condition():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("the condition never held")


class TestTheBinderReportsAnAbandonedLoad:
    """The body of a cached call is application code; its failure is not lost.

    The load is detached into its own task, so a caller that goes away leaves
    nobody to receive an exception. Without a reporter the single flight
    retrieves it and drops it, and a third-party body that fails every time is
    completely silent — the repository's binder is wired with one, and this one
    must be too.
    """

    async def test_a_failure_with_no_caller_left_is_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = _container(cache_module(_single_alias()))
        started = asyncio.Event()
        gate = asyncio.Event()

        @cache_call()
        async def fail_later(query: str) -> Answer:
            started.set()
            await gate.wait()
            raise RuntimeError("upstream down")

        wrapped = cached_calls(container).wrap(fail_later)

        with caplog.at_level(logging.WARNING, logger="loom.core.cache.calls"):
            # ensure_future, not create_task: wrap() is typed Awaitable, not Coroutine.
            caller = asyncio.ensure_future(wrapped("q"))
            await _until(started.is_set)
            caller.cancel()
            gate.set()
            await asyncio.gather(caller, return_exceptions=True)
            await _until(lambda: bool(_abandoned_warnings(caplog)))

        records = _abandoned_warnings(caplog)
        assert len(records) == 1
        assert "upstream down" in records[0].getMessage()


def _abandoned_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [record for record in caplog.records if "CacheCallLoadAbandoned" in record.getMessage()]
