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

    def distinct(self) -> Iterator[CacheGateway]:
        """Yield each gateway once, so a shutdown closes an alias only once."""
        yield self.data
        if self.counters is not self.data:
            yield self.counters


def _register_calls(container: LoomContainer, calls: CachedCalls) -> None:
    """Claim the ``CachedCalls`` slot with *calls*, unless it is already taken.

    The same courtesy the decorator slot gets: a deployment that registered its
    own binder keeps it, and loom says whose it kept.

    Args:
        container: Container the cache module is being applied to.
        calls: Binder loom would bind when the slot is free.
    """
    if container.is_registered(CachedCalls):
        existing = container.resolve(CachedCalls)
        get_logger(__name__).warning(
            "CacheCallsAlreadyRegistered", calls=type(existing).__qualname__
        )
        return
    container.register_instance(CachedCalls, calls)


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
        # The single flight reports a load abandoned by its last caller: the
        # body of a cached call is application code, and a failure nobody is
        # left to await would otherwise be lost, as it is for a repository.
        flight = SingleFlight(_log_abandoned_call)
        # Before the decorator block, which returns early when its slot is
        # taken: registering after it would leave a deployment that supplies
        # its own decorator with no binder at all.
        _register_calls(container, _ConfiguredCalls(config, data, flight, random.Random()))
        if container.is_registered(RepositoryDecorator):
            existing = container.resolve(RepositoryDecorator)
            get_logger(__name__).warning(
                "CacheDecoratorAlreadyRegistered", decorator=type(existing).__qualname__
            )
            return
        container.register_instance(RepositoryDecorator, decorate)

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
    """Claim the decorator and binder slots, when free, with the pass-throughs that announce."""
    _register_calls(container, _UnconfiguredCalls())
    if not container.is_registered(RepositoryDecorator):
        container.register_instance(RepositoryDecorator, _announce_uncached)


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
