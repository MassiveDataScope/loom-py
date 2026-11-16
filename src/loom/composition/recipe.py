"""What an entry point declares, and the contract every adapter fulfils."""

from __future__ import annotations

import warnings
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeVar

from loom.core.bootstrap import BootstrapError
from loom.core.di.graph import (
    LENIENT_REFUSALS,
    REFUSALS,
    ClassNode,
    DependencyGraph,
    DependencyGraphError,
    GraphAnalysis,
    refuse_unmapped_repository,
)
from loom.core.stores.catalog import NONE_BACKEND, StoreCatalog
from loom.core.stores.repository import CursorCodec

if TYPE_CHECKING:
    from loom.composition.application import Application
    from loom.composition.source import CompositionSource
    from loom.core.config import ConfigContext
    from loom.core.di.container import LoomContainer
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.stores.profile import PoolMetricsSink, RuntimeProfile

T_co = TypeVar("T_co", covariant=True)


@dataclass(frozen=True, kw_only=True)
class Recipe:
    """How an entry point wants its application composed.

    Args:
        profile: Pool defaults of the entry point (server, worker, ...).
        discovered: Use cases, models, interfaces and agent specs, final:
            with whatever the adapter added or pruned.
        default_store_backend: Backend of the default store when ``stores.default``
            is not declared.
        skip_checks: Deploy-check areas this entry point does not serve,
            such as :data:`~loom.core.checks.REST_CHECKS` for a worker.
        agents: Whether this entry point hosts the agent runtime; without it
            an ``ai`` section is ignored and any compiled use case declaring
            an ``Agent()`` or ``Mcp()`` marker is refused at start-up.
        agent_endpoints: Whether this entry point serves its agents over
            HTTP endpoints too, so it builds every declared agent. Without
            them, agents are reached only through ``Agent()`` and ``Mcp()``
            markers.
        metrics: Use-case metrics, or ``None``.
        pool_metrics: Connection-pool metrics, only for an entry point that
            serves a scrape.
        connection_only: Ask the backend for its connection only, no
            repository; for an entry point with no model (see
            :class:`~loom.core.stores.StoreRuntime`).
        callbacks: Job callback classes the entry point builds; the
            dependency graph's roots with the use cases and jobs, never
            compiled.
        overrides: DI modules run right after the caller's modules, under the
            same reserved-key check; a test double of a repository binds under
            the key its store would bind it. Set by the testing harness; no
            production entry point sets them.
        lenient: Skip the checks a test double cannot pass: the deploy
            checks, the use-case factory's verification, the refusal of a
            store a component cannot write to, and the refusal of an agent
            marker on an entry point that hosts no agents. A missing binding
            an ``APPLICATION`` singleton needs is still refused. Set by the
            testing harness; no production entry point sets it.
    """

    profile: RuntimeProfile
    discovered: DiscoveryResult
    default_store_backend: str = NONE_BACKEND
    skip_checks: frozenset[str] = frozenset()
    agents: bool = False
    agent_endpoints: bool = True
    metrics: MetricsAdapter | None = None
    pool_metrics: PoolMetricsSink | None = None
    connection_only: bool = False
    callbacks: tuple[type, ...] = ()
    overrides: tuple[Callable[[LoomContainer], None], ...] = ()
    lenient: bool = False

    def analysed(self, graph: DependencyGraph) -> GraphAnalysis:
        """*graph* analysed from the classes loom builds: use cases, jobs, then callbacks.

        Each :class:`~loom.core.di.errors.DependencyUnresolvable` is emitted as a warning.

        Args:
            graph: The use-case factory's dependency graph.

        Returns:
            The analysis.

        Raises:
            BootstrapError: Wrapping the first structural refusal, cause chained.
        """
        discovered = self.discovered
        callbacks = tuple(map(ClassNode, self.callbacks))
        roots = (*map(ClassNode, (*discovered.use_cases, *discovered.jobs)), *callbacks)
        refusals = (
            LENIENT_REFUSALS
            if self.lenient
            else (*REFUSALS, refuse_unmapped_repository(frozenset(callbacks)))
        )
        try:
            analysis = graph.analyse(roots, refusals)
        except DependencyGraphError as exc:
            raise BootstrapError(f"Bootstrap failed: {exc}") from exc
        for warning in analysis.warnings:
            warnings.warn(warning, stacklevel=2)
        return analysis

    def cursor_codec(self, ctx: ConfigContext, catalog: StoreCatalog) -> CursorCodec | None:
        """The one cursor codec every store shares; ``None`` when no store pages with cursors.

        Args:
            ctx: The configuration holding ``stores.settings.cursors``.
            catalog: The stores the application builds.

        Returns:
            The codec, or ``None`` for a connection-only recipe or when no
            store of *catalog* pages with cursors.

        Raises:
            ConfigError: When a store pages with cursors and
                ``stores.settings.cursors`` is invalid.
        """
        if self.connection_only or not catalog.pages_with_cursors:
            return None
        return CursorCodec.from_context(ctx)


class Adapter(Protocol[T_co]):
    """A transport mounted on a composed application.

    :meth:`recipe` declares what to compose and opens nothing. :meth:`bind`
    mounts the transport on the composed application and returns what the
    entry point serves (an ASGI app, a Celery app, a handler); it opens no
    resource either, since :meth:`Application.running` owns them and the
    platform's lifespan hook enters it. The exception is a platform with no
    lifespan hook, such as a Lambda function's Init: there :meth:`bind` opens
    the application through a :class:`~loom.composition.LoopHost` and returns
    an object that owns closing it.
    """

    def recipe(self, source: CompositionSource) -> Recipe:
        """Declare how to compose the application from *source*."""
        ...

    def bind(self, application: Application) -> T_co:
        """Mount the transport on *application*."""
        ...
