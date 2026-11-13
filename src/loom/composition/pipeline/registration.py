"""Register repositories, kernel, jobs, SQL, and application collaborators."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from loom.composition._reserved import refusing_reserved
from loom.composition._sql import SqlWiring, register_sql_collaborators, resolve_sql
from loom.composition.pipeline.persistence import Persisted
from loom.composition.recipe import Recipe
from loom.composition.resources import Resource
from loom.composition.source import CompositionSource
from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.cache.wiring import cache_module_for
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.job.config import JobSettings
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.elevation import ScopedElevationSink
from loom.core.persistence.stores import StoreCatalog, StoreKey
from loom.core.uow.coordinator import ROLLBACK_TIMEOUT_S, OrderedCommits


@dataclass(frozen=True, slots=True, kw_only=True)
class Registered:
    kernel: KernelRuntime
    job_resources: tuple[Resource, ...]
    sql: SqlWiring


def register(
    source: CompositionSource,
    recipe: Recipe,
    persisted: Persisted,
    job_settings: JobSettings,
    *,
    modules: Sequence[Callable[[LoomContainer], None]],
    observability_provider: Callable[[], ObservabilityRuntime],
    catalog_provider: Callable[[], StoreCatalog],
) -> Registered:
    from loom.composition import build

    ctx, observability = source.config, source.observability
    discovered = recipe.discovered
    catalog, wirings, factories = source.catalog, persisted.wirings, persisted.factories
    # Caller modules first, then repositories, then the framework's own. A
    # caller module binding a framework-owned key fails right after it runs.
    kernel = create_kernel(
        config=source.app,
        use_cases=discovered.use_cases,
        modules=[
            *refusing_reserved(modules, tuple(catalog.specs)),
            *(wiring.repo_registration_module for wiring in wirings.values()),
            cache_module_for(ctx),
            *_elevation_modules(wirings),
        ],
        coordinator=(
            OrderedCommits(factories, rollback_timeout=_rollback_timeout(ctx))
            if factories
            else None
        ),
        metrics=recipe.metrics,
        environment=source.environment,
    )
    job_resources = build.configure_job_service(
        ctx,
        kernel,
        observability,
        backend=source.app.job_service,
        jobs=discovered.jobs,
        settings=job_settings,
    )
    sql = resolve_sql(ctx)
    register_sql_collaborators(kernel.container, sql, observability)
    # Registered, not merely passed: capability spans resolve it from the
    # container, and an unregistered runtime makes every Scope.TOOL span a
    # silent no-op in production while passing every test that injects one.
    kernel.container.register(ObservabilityRuntime, observability_provider, scope=Scope.APPLICATION)
    kernel.container.register(StoreCatalog, catalog_provider, scope=Scope.APPLICATION)
    return Registered(kernel=kernel, job_resources=tuple(job_resources), sql=sql)


def _elevation_modules(
    wirings: Mapping[StoreKey, PersistenceWiring],
) -> list[Callable[[LoomContainer], None]]:
    """Register the one elevation sink of the stores whose sessions carry elevation flags.

    Run once every store's models are prepared: the sink knows each store's
    elevable scopes, and the product's elevations are checked against all of
    them. Without such a store nothing is registered.

    Raises:
        ConfigError: See :func:`~loom.core.persistence.elevation.validate_elevations`.
    """
    stores = {key: w.elevation for key, w in wirings.items() if w.elevation is not None}
    if not stores:
        return []
    sink = ScopedElevationSink(stores)
    return [lambda container: container.register_instance(ElevationSink, sink)]


def _rollback_timeout(ctx: ConfigContext) -> float:
    """``persistence.rollback_timeout``: seconds the rollbacks of one invocation may take together.

    Raises:
        ConfigError: When it is not above zero.
    """
    timeout = ctx.section_or_default("persistence.rollback_timeout", float, ROLLBACK_TIMEOUT_S)
    if timeout <= 0:
        raise ConfigError(f"persistence.rollback_timeout must be above zero, got {timeout}")
    return timeout
