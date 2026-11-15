"""Composition of the ``cache:`` section into a container module.

The decorator declares (``@cached``), the composition root binds: the module
built here registers the gateways and the
:class:`~loom.core.repository.registration.RepositoryDecorator` that wraps a
marked repository in :class:`~loom.core.cache.repository.CachedRepository`
when the registration module builds it.  The same module binds the
:class:`~loom.core.cache.calls.CachedCalls` that turns a ``@cache_call``
coroutine into a cached one, for code that is not a repository.  Without the
section both only announce, at boot, what runs uncached.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from loom.core.cache._single_flight import SingleFlight
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.calls import (
    CachedCalls,
    _ConfiguredCalls,
    _log_abandoned_call,
    _UnconfiguredCalls,
)
from loom.core.cache.decorators import declares_cache_policy
from loom.core.cache.dependency import GenerationalDependencyResolver
from loom.core.cache.gateway import CacheGateway
from loom.core.cache.repository import CachedRepository
from loom.core.config import ConfigContext, ConfigKey
from loom.core.di.container import LoomContainer
from loom.core.logger import get_logger
from loom.core.repository.registration import RepositoryDecorator


@dataclass(frozen=True)
class CacheGateways:
    """The data and counter gateways a deployment opened.

    Shutdown is the only consumer: closing the composed application (the REST
    lifespan, a worker's shutdown signals) closes each member of
    :meth:`distinct`.

    Attributes:
        data: Gateway over ``config.aiocache_alias``.
        counters: Gateway over ``config.effective_counter_alias``; the very
            same object as ``data`` when the aliases coincide.
    """

    data: CacheGateway
    counters: CacheGateway

    def named(self) -> Iterator[tuple[str, CacheGateway]]:
        """Yield each gateway once with its name, ``data`` then ``counters``."""
        yield "data", self.data
        if self.counters is not self.data:
            yield "counters", self.counters

    def distinct(self) -> Iterator[CacheGateway]:
        """Yield each gateway once, so a shutdown closes an alias only once."""
        for _, gateway in self.named():
            yield gateway


@dataclass(frozen=True)
class _CacheSlot:
    """A container key the cache module binds unless a deployment bound its own.

    Attributes:
        key: The key claimed.
        kept: The event logged when a binding is already there.
        log_field: The log field naming the kept binding's type.
    """

    key: object
    kept: str
    log_field: str

    def claim(self, container: LoomContainer, instance: object) -> None:
        """Bind *instance* under :attr:`key`, or log :attr:`kept` naming the binding kept."""
        existing = container.resolve_or_none(self.key)
        if existing is not None:
            get_logger(__name__).warning(self.kept, **{self.log_field: type(existing).__qualname__})
            return
        container.register_instance(self.key, instance)


_CALLS_SLOT = _CacheSlot(CachedCalls, "CacheCallsAlreadyRegistered", "calls")
_DECORATOR_SLOT = _CacheSlot(RepositoryDecorator, "CacheDecoratorAlreadyRegistered", "decorator")


def cache_module(config: CacheConfig) -> Callable[[LoomContainer], None]:
    """Build the container module for an enabled ``cache:`` section.

    The returned module applies *config* to aiocache before it builds any
    gateway, opens the data and counter gateways, and registers them, the
    resolver, the :class:`~loom.core.cache.calls.CachedCalls` binder that
    caches ``@cache_call`` coroutines, and — unless one is already registered,
    in which case it logs ``CacheDecoratorAlreadyRegistered`` and leaves the
    slot alone — a
    :class:`~loom.core.repository.registration.RepositoryDecorator` that wraps
    every ``@cached`` repository the registration module builds.  All of it
    happens when the module runs against a container, so a boot that fails
    earlier has not touched the global aiocache registry.

    Args:
        config: The decoded ``cache:`` section.

    Returns:
        A module to hand to the kernel or apply to a container.
    """

    def register(container: LoomContainer) -> None:
        CacheGateway.apply_config(config)
        data = CacheGateway(alias=config.aiocache_alias)
        counters = (
            data
            if config.effective_counter_alias == config.aiocache_alias
            else CacheGateway(alias=config.effective_counter_alias)
        )
        resolver = GenerationalDependencyResolver(counters)

        def decorate(repository: Any) -> Any:
            already_wrapped = isinstance(repository, CachedRepository)
            if already_wrapped or not declares_cache_policy(type(repository)):
                return repository
            return CachedRepository(
                repository,
                config=config,
                cache=data,
                dependency_resolver=resolver,
            )

        container.register_instance(CacheConfig, config)
        container.register_instance(CacheGateway, data)
        container.register_instance(GenerationalDependencyResolver, resolver)
        container.register_instance(CacheGateways, CacheGateways(data=data, counters=counters))
        flight = SingleFlight(_log_abandoned_call)
        _CALLS_SLOT.claim(container, _ConfiguredCalls(config, data, flight, random.Random()))
        _DECORATOR_SLOT.claim(container, decorate)

    return register


def _announce_uncached(repository: Any) -> Any:
    """Return *repository* unchanged, warning once when its class is marked ``@cached``.

    Once per model follows from the registration module, which runs the
    decorator a single time per model per container.
    """
    if declares_cache_policy(type(repository)):
        get_logger(__name__).warning("CacheNotConfigured", repository=type(repository).__qualname__)
    return repository


def _announcing_module(container: LoomContainer) -> None:
    """Claim the decorator and binder slots with the pass-throughs that announce."""
    _CALLS_SLOT.claim(container, _UnconfiguredCalls())
    _DECORATOR_SLOT.claim(container, _announce_uncached)


def cache_module_for(ctx: ConfigContext) -> Callable[[LoomContainer], None]:
    """Build the cache module a deployment's configuration asks for.

    Args:
        ctx: Loaded configuration; ``cache:`` is optional.

    Returns:
        :func:`cache_module` over the section when it is present and enabled;
        otherwise a module whose decorator leaves every repository bare and
        logs ``CacheNotConfigured`` for each marked class.
    """
    config = ctx.section_optional(ConfigKey.CACHE, CacheConfig)
    if config is not None and config.enabled:
        return cache_module(config)
    return _announcing_module
