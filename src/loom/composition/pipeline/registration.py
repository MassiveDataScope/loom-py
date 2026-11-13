"""Register every binding of the application in one fixed order, and build its kernel.

The order: the seed keys (the application's ``AppConfig``, ``Environment``,
``ObservabilityRuntime`` and ``StoreCatalog``); the caller's modules; the
recipe's overrides; each store's repositories; the framework's keys, among
them the kernel, whose construction compiles the discovered use cases.
Nothing registers after this phase, which ends by analysing the dependency graph.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from loom.composition._jobs import configure_job_service
from loom.composition._reserved import refusing_reserved
from loom.composition._sql import SqlWiring, register_sql_collaborators, resolve_sql
from loom.composition.pipeline.analysis import analysed
from loom.composition.pipeline.persistence import Persisted
from loom.composition.resources import Resource
from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap import KernelRuntime
from loom.core.bootstrap.kernel import bootstrapping, install_kernel
from loom.core.cache.wiring import cache_module_for
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.graph import GraphAnalysis
from loom.core.environment import Environment
from loom.core.logger import LoggerPort
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.elevation import ScopedElevationSink
from loom.core.persistence.stores import StoreCatalog, StoreKey
from loom.core.uow.coordinator import ROLLBACK_TIMEOUT_S, OrderedCommits


@dataclass(frozen=True, slots=True, kw_only=True)
class Registered:
    """The persisted composition, its kernel, its analysed graph and the collaborators around it."""

    persisted: Persisted
    kernel: KernelRuntime
    analysis: GraphAnalysis
    job_resources: tuple[Resource, ...]
    sql: SqlWiring


def register(
    persisted: Persisted,
    *,
    modules: Sequence[Callable[[LoomContainer], None]],
    logger: LoggerPort,
) -> Registered:
    """Register every binding and build the kernel; the job service, SQL and the rest follow it.

    Raises:
        ConfigError: When the ``cache`` section, an elevation,
            ``persistence.rollback_timeout`` or the ``sql`` section is
            invalid. Raised as is.
        ReservedBindingError: When a caller module or an override binds or
            replaces a framework-owned key.
        BootstrapError: Wrapping any other failure of a module, an override,
            a repository or cache module or a use case's compilation.
        JobBackendError: When the job backend is unknown or its extra is
            missing. Raised as is.
        SQSQueueConfigError: When SQS serves a job it cannot route. Raised as is.
        BootstrapError: Wrapping the first structural refusal of the
            dependency graph (:func:`~loom.composition.pipeline.analysis.analysed`).
    """
    source = persisted.planned.source
    ctx, observability = source.config, source.observability
    kernel = _bind_application(persisted, modules, logger)
    job_resources = configure_job_service(
        ctx,
        kernel,
        observability,
        backend=source.app.job_service,
        jobs=persisted.planned.recipe.discovered.jobs,
        settings=persisted.job_settings,
    )
    sql = resolve_sql(ctx)
    register_sql_collaborators(kernel.container, sql, observability)
    return Registered(
        persisted=persisted,
        kernel=kernel,
        analysis=analysed(kernel.container, persisted.planned.recipe),
        job_resources=tuple(job_resources),
        sql=sql,
    )


def _bind_application(
    persisted: Persisted, modules: Sequence[Callable[[LoomContainer], None]], logger: LoggerPort
) -> KernelRuntime:
    """Seed a container, run every module into it and build the kernel on it.

    The caller's modules and the overrides run first, so the framework's
    defaults leave what they bound alone; either one binding a key the
    framework owns fails right after it runs. The container is validated
    when the application is sealed.

    Raises:
        ConfigError: When the ``cache`` section, an elevation or
            ``persistence.rollback_timeout`` is invalid; raised before any
            module runs.
        ReservedBindingError: When a caller module or an override binds or
            replaces a framework-owned key.
        BootstrapError: Wrapping any other failure of a module or of a use
            case's compilation.
    """
    source, recipe = persisted.planned.source, persisted.planned.recipe
    ctx, observability, catalog = source.config, source.observability, source.catalog
    stores = tuple(catalog.specs)
    cache = cache_module_for(ctx)
    sink = _elevation_sink(persisted.wirings)
    factories = persisted.factories
    coordinator = (
        OrderedCommits(factories, rollback_timeout=_rollback_timeout(ctx)) if factories else None
    )

    ordered = (
        *refusing_reserved(modules, stores),
        *refusing_reserved(recipe.overrides, stores),
        *(wiring.repo_registration_module for wiring in persisted.wirings.values()),
        cache,
    )
    seeds = {
        type(source.app): source.app,
        Environment: source.environment,
        # Bound, not merely passed: capability spans resolve the observability
        # runtime from the container, and an unregistered runtime makes every
        # Scope.TOOL span a silent no-op in production while passing every test
        # that injects one.
        ObservabilityRuntime: observability,
        StoreCatalog: catalog,
    }
    with bootstrapping(seeds, modules=ordered, logger=logger) as container:
        if sink is not None:
            container.register_instance(ElevationSink, sink)
        return install_kernel(
            container,
            use_cases=recipe.discovered.use_cases,
            logger=logger,
            coordinator=coordinator,
            metrics=recipe.metrics,
        )


def _elevation_sink(wirings: Mapping[StoreKey, PersistenceWiring]) -> ElevationSink | None:
    """The one elevation sink of the stores whose sessions carry elevation flags, if any.

    Built once every store's models are prepared: the sink knows each store's
    elevable scopes, and the product's elevations are checked against all of
    them.

    Raises:
        ConfigError: See :func:`~loom.core.persistence.elevation.validate_elevations`.
    """
    stores = {key: w.elevation for key, w in wirings.items() if w.elevation is not None}
    return ScopedElevationSink(stores) if stores else None


def _rollback_timeout(ctx: ConfigContext) -> float:
    """``persistence.rollback_timeout``: seconds the rollbacks of one invocation may take together.

    Raises:
        ConfigError: When it is not above zero.
    """
    timeout = ctx.section_or_default("persistence.rollback_timeout", float, ROLLBACK_TIMEOUT_S)
    if timeout <= 0:
        raise ConfigError(f"persistence.rollback_timeout must be above zero, got {timeout}")
    return timeout
