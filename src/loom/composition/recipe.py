"""What an entry point declares, and the contract every adapter fulfils."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeVar

if TYPE_CHECKING:
    from loom.composition.application import Application
    from loom.composition.source import CompositionSource
    from loom.core.discovery.base import DiscoveryResult
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.persistence.abc import PersistenceBackend
    from loom.core.persistence.profile import PoolMetricsSink, RuntimeProfile

T_co = TypeVar("T_co", covariant=True)


@dataclass(frozen=True, kw_only=True)
class Recipe:
    """How an entry point wants its application composed.

    Args:
        profile: Pool defaults of the entry point (server, worker, ...).
        discovered: Use cases, models, interfaces and agent specs, final:
            with whatever the adapter added or pruned.
        backend: Persistence backend serving the models.
        skip_checks: Deploy-check areas this entry point does not serve,
            such as :data:`~loom.core.checks.REST_CHECKS` for a worker.
        agents: Whether this entry point hosts the agent runtime; without it
            an ``ai`` section is ignored.
        metrics: Use-case metrics, or ``None``.
        pool_metrics: Connection-pool metrics, only for an entry point that
            serves a scrape.
        connection_only: Ask the backend for its connection only, no
            repository; for an entry point with no model (see
            :class:`~loom.core.persistence.BackendRuntime`).
    """

    profile: RuntimeProfile
    discovered: DiscoveryResult
    backend: PersistenceBackend
    skip_checks: frozenset[str] = frozenset()
    agents: bool = False
    metrics: MetricsAdapter | None = None
    pool_metrics: PoolMetricsSink | None = None
    connection_only: bool = False


class Adapter(Protocol[T_co]):
    """A transport mounted on a composed application.

    :meth:`recipe` declares what to compose and opens nothing. :meth:`bind`
    mounts the transport on the composed application and returns what the
    entry point serves (an ASGI app, a Celery app, a handler); it opens no
    resource either, since :meth:`Application.running` owns them.
    """

    def recipe(self, source: CompositionSource) -> Recipe:
        """Declare how to compose the application from *source*."""
        ...

    def bind(self, application: Application) -> T_co:
        """Mount the transport on *application*."""
        ...
