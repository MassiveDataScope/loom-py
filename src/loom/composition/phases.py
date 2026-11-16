"""The ordered phases that :meth:`~loom.composition.project.Project.compose` runs.

The order: :func:`plan` takes the adapter's recipe; :func:`build_stores` builds
every store before any registration; :func:`register` fills the container and
builds the kernel; :meth:`Recipe.analysed` analyses the dependency graph;
:func:`wire_ai` builds the optional agent runtime; :func:`seal` seals the
container. Nothing registers after :func:`register`.
"""

from __future__ import annotations

import time
from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar, cast

from loom.composition._ai import AiWiring, MarkerDeclarations
from loom.composition._jobs import configure_job_service
from loom.composition._reserved import reserved_keys
from loom.composition._sql import SqlWiring, register_sql_collaborators, resolve_sql
from loom.composition.errors import ReservedBindingError
from loom.composition.recipe import Adapter, Recipe
from loom.composition.resources import Resource, ResourceKind
from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap import BootstrapError, KernelRuntime
from loom.core.bootstrap.kernel import KernelRecipe, seeded_container
from loom.core.cache.wiring import cache_module_for
from loom.core.checks import enforce_deploy_checks
from loom.core.config import ConfigError, ConfigKey
from loom.core.di.container import ResolutionError
from loom.core.di.graph import GraphAnalysis
from loom.core.engine.compilable import Compilable
from loom.core.environment import Environment
from loom.core.identity import DEFAULT_MAX_DELEGATION_SECONDS, configure_delegation
from loom.core.job.config import JobSettings
from loom.core.logger import LoggerPort
from loom.core.model import BaseModel
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores import StoreRuntime, StoreWiring
from loom.core.stores.catalog import (
    DEFAULT_STORE,
    DEFAULT_STORE_PATH,
    StoreCatalog,
    StoreKey,
    StoreSetting,
)
from loom.core.stores.elevation import ScopedElevationSink
from loom.core.stores.repository import RepositoryCatalog
from loom.core.stores.uow.abc import UnitOfWorkFactory
from loom.core.stores.uow.coordinator import ROLLBACK_TIMEOUT_S, OrderedCommits
from loom.core.use_case.factory import ModelRead
from loom.core.use_case.kinds import AGENT, MCP

if TYPE_CHECKING:
    from loom.composition.project import Project

T = TypeVar("T")


class PhaseClock:
    """The milliseconds of each composition phase, each from the end of the one before."""

    def __init__(self) -> None:
        self.started = self._mark = time.perf_counter()
        self.ms: dict[str, float] = {}

    def lap(self, phase: str, result: T) -> T:
        """Record *phase* as ending now and hand its *result* back.

        Args:
            phase: The phase's name.
            result: What the phase returned.

        Returns:
            *result*.
        """
        now = time.perf_counter()
        self.ms[phase] = (now - self._mark) * 1000
        self._mark = now
        return result

    @property
    def total_ms(self) -> float:
        """The milliseconds since the clock started."""
        return (time.perf_counter() - self.started) * 1000


@dataclass(frozen=True, slots=True, kw_only=True)
class Stores:
    """The job settings, and one wiring per store the discovered models use.

    Args:
        job_settings: Each discovered job's settings.
        store_backends: The name of the backend each store's wiring was built by.
        models: The models each store serves.
        wirings: Each store's wiring, its models prepared.
    """

    job_settings: JobSettings
    store_backends: dict[StoreKey, str]
    models: dict[StoreKey, tuple[type[BaseModel], ...]]
    wirings: dict[StoreKey, StoreWiring]

    @property
    def factories(self) -> dict[StoreKey, UnitOfWorkFactory]:
        """The unit-of-work factory of every store that has one."""
        return {key: w.uow_factory for key, w in self.wirings.items() if w.uow_factory is not None}

    @property
    def resources(self) -> tuple[Resource, ...]:
        """One resource per store; the default store's readiness is keyed by its backend's name."""
        return tuple(
            Resource(
                kind=ResourceKind.STORE,
                name=key,
                manager=wiring.lifespan_init,
                check=wiring.startup_checks,
                readiness=wiring.readiness,
                readiness_key=self.store_backends[key] if key == DEFAULT_STORE else key,
            )
            for key, wiring in self.wirings.items()
        )


def plan(project: Project, adapter: Adapter[object]) -> Recipe:
    """Ask *adapter* for its recipe, then refuse a failing deploy check.

    A lenient recipe runs no deploy check.

    Args:
        project: What the application composes from.
        adapter: Whose recipe to take.

    Returns:
        The recipe.

    Raises:
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
    """
    recipe = adapter.recipe(project)
    if not recipe.lenient:
        enforce_deploy_checks(project.config, project.environment, skip=recipe.skip_checks)
    return recipe


def build_stores(project: Project, recipe: Recipe) -> Stores:
    """Read each job's settings, then build and prepare every store the discovered models use.

    The stores share one cursor codec, built only when
    ``stores.settings.cursors`` is configured.

    Args:
        project: What the application composes from.
        recipe: What it composes.

    Returns:
        The job settings and the stores.

    Raises:
        ConfigError: When a job's settings, a store section or, for a store
            ``stores.settings.cursors`` is invalid, or a
            store is named after the default store's backend.
    """
    ctx, discovered, catalog = project.config, recipe.discovered, project.catalog
    job_settings = JobSettings.from_config(ctx, discovered.jobs)
    stores = catalog.models_by_store(discovered.models)
    backends = {
        key: project.resolve_store_backend(key, default=recipe.default_store_backend)
        for key in stores
    }
    default_label = backends[DEFAULT_STORE].name
    if default_label in stores:
        raise ConfigError(
            f"{ConfigKey.STORES}.{default_label} would report its readiness under the "
            f"key of {DEFAULT_STORE_PATH}, named after its backend; rename the store"
        )
    cursor_codec = recipe.build_cursor_codec(ctx)
    wirings: dict[StoreKey, StoreWiring] = {}
    for key, models in stores.items():
        runtime = StoreRuntime(
            recipe.profile,
            pool_metrics=recipe.pool_metrics,
            connection_only=recipe.connection_only,
            catalog=catalog,
            store=key,
            cursor_codec=cursor_codec,
        )
        wiring = wirings[key] = backends[key].build(ctx, models, runtime)
        wiring.prepare_models(models)
    return Stores(
        job_settings=job_settings,
        store_backends={key: backends[key].name for key in stores},
        models=stores,
        wirings=wirings,
    )


def register(
    project: Project,
    recipe: Recipe,
    stores: Stores,
    *,
    logger: LoggerPort,
) -> tuple[KernelRuntime, SqlWiring, tuple[Resource, ...]]:
    """Fill a container and build the kernel on it, then register the job service and SQL.

    The order: the seed keys (the application's ``AppConfig``,
    ``Environment``, ``ObservabilityRuntime`` and ``StoreCatalog``); the
    discovered installers; the recipe's overrides; each store's repositories and the
    ``RepositoryCatalog`` over them; the cache module; the framework's keys,
    among them the kernel, whose construction compiles the discovered use
    cases; the job service; the SQL collaborators. The installers and
    the overrides run first, so the framework's defaults leave what they bound
    alone; either one binding a key the framework owns fails right after it
    runs. The container is validated when the application is sealed.

    Args:
        project: What the application composes from.
        recipe: What it composes.
        stores: The stores, whose repositories and units of work it registers.
        logger: Where the container and the compilation log.

    Returns:
        The kernel, the SQL wiring and the job backends' resources.

    Raises:
        ConfigError: When the ``cache`` section, an elevation,
            ``stores.settings.rollback_timeout``,
            ``identity.max_delegation_seconds`` or the ``sql`` section is
            invalid. Raised as is.
        ReservedBindingError: When a caller module or an override binds or
            replaces a framework-owned key.
        BootstrapError: Wrapping any other failure of a module or of a use
            case's compilation.
        JobBackendError: When the job backend is unknown or its extra is
            missing. Raised as is.
        SQSQueueConfigError: When SQS serves a job it cannot route. Raised as is.
    """
    ctx, catalog, observability = project.config, project.catalog, project.observability
    cache = cache_module_for(ctx)
    configure_delegation(
        ctx.positive_seconds("identity.max_delegation_seconds", DEFAULT_MAX_DELEGATION_SECONDS)
    )
    sink = ScopedElevationSink.from_wirings(stores.wirings)
    factories = stores.factories
    coordinator = (
        OrderedCommits(
            factories,
            rollback_timeout=ctx.positive_seconds(
                StoreSetting.ROLLBACK_TIMEOUT.path, ROLLBACK_TIMEOUT_S
            ),
        )
        if factories
        else None
    )
    seeds = {
        type(project.app): project.app,
        Environment: project.environment,
        ObservabilityRuntime: observability,
        StoreCatalog: catalog,
    }
    installers = tuple(installer().install for installer in recipe.discovered.installers)
    with seeded_container(
        KernelRecipe.container(),
        seeds,
        modules=(*installers, *recipe.overrides),
        logger=logger,
        reserved=reserved_keys(catalog.specs),
        error=ReservedBindingError.for_key,
    ) as container:
        for wiring in stores.wirings.values():
            wiring.repo_registration_module(container)
        container.register_instance(RepositoryCatalog, RepositoryCatalog(container, stores.models))
        cache(container)
        if sink is not None:
            container.register_instance(ElevationSink, sink)
        kernel = KernelRecipe(
            recipe.discovered.use_cases,
            logger,
            metrics=recipe.metrics,
            coordinator=coordinator,
        ).install(container)
    job_resources = configure_job_service(
        ctx,
        kernel,
        observability,
        backend=project.app.job_service,
        jobs=recipe.discovered.jobs,
        callbacks=recipe.discovered.callbacks,
        settings=stores.job_settings,
    )
    sql = resolve_sql(ctx)
    register_sql_collaborators(kernel.container, sql, observability)
    return kernel, sql, job_resources


def wire_ai(
    project: Project,
    recipe: Recipe,
    kernel: KernelRuntime,
    analysis: GraphAnalysis,
    sql: SqlWiring,
) -> AiWiring:
    """Build the agent runtime when the entry point hosts it, else nothing.

    Every ``Agent()`` and ``Mcp()`` declaration the analysed graph reaches
    counts, in a use case's ``execute()`` or on an adapter it is built
    with; the graph is walked once, here. An entry point that does not host
    agents still refuses, at start-up, any such declaration. One that
    serves no agent endpoints builds only the agents a declaration names. A
    lenient recipe that hosts no agents refuses none and walks nothing.

    Args:
        project: What the application composes from.
        recipe: What it composes.
        kernel: The kernel whose executor resolves the markers.
        analysis: The analysed dependency graph.
        sql: The SQL wiring, whose configuration the runtime reads.

    Returns:
        The wiring, :attr:`AiWiring.ABSENT` when no runtime is built.

    Raises:
        ConfigError: When ``app.agents`` is set and there is no
            ``ai:`` section.
        AgentCompilationError: When a declaration cannot be satisfied.
    """
    if not recipe.agents and recipe.lenient:
        return AiWiring.ABSENT
    declared = MarkerDeclarations.of(analysis, kernel.compiler, kernel.registry)
    if not recipe.agents:
        declared.mcp_grants({})
        declared.verify({})
        return AiWiring.ABSENT
    ai = AiWiring.build(
        project.config,
        kernel=kernel,
        sql_cfg=sql.config,
        code_path=project.code_path,
        agent_specs=recipe.discovered.agent_specs,
        declared=declared,
        names=(
            None
            if recipe.agent_endpoints
            else frozenset(found.binding.declaration.name for _, found in declared.agents)
        ),
    )
    declared.verify({agent.name: agent for agent in ai.plans})
    ai.bind_resolvers(kernel)
    return ai


def seal(
    kernel: KernelRuntime,
    analysis: GraphAnalysis,
    recipe: Recipe,
    *,
    catalog: StoreCatalog,
    factories: Collection[StoreKey],
) -> None:
    """Seal the container and build its singletons; refuse an unservable store; verify use cases.

    The container refuses a singleton the analysed graph reaches that
    declares a kind no resolver is bound for. A lenient recipe defers the
    agent and MCP kinds (such a singleton is not built and fails at its first
    resolve) and stops after building the singletons. The model
    reads verified are those of the use cases; a job's are not.

    Args:
        kernel: The kernel to seal.
        analysis: The analysed dependency graph.
        recipe: What the application composes.
        catalog: The stores the application declares.
        factories: The stores that have a unit of work.

    Raises:
        BootstrapError: Wrapping the refusal of such a singleton, with its
            path, or the failure of an ``APPLICATION`` binding's
            instantiation, cause chained.
        CompilationError: When a use case or job does not compile.
        UncomposedWriteStore: See :meth:`~loom.core.engine.plan.ExecutionPlan.refuse_unservable`.
        JoinedAutocommitStore: See :meth:`~loom.core.engine.plan.ExecutionPlan.refuse_unservable`.
        ResolutionError: When a use case's dependency or marker read cannot be served.
    """
    try:
        kernel.container.seal(
            analysis, deferred=(AGENT, MCP) if recipe.lenient else (), strict=not recipe.lenient
        )
    except ResolutionError as exc:
        raise BootstrapError(f"Bootstrap failed: {exc}") from exc
    if recipe.lenient:
        return
    discovered = recipe.discovered
    use_cases = frozenset(discovered.use_cases)
    reads: list[ModelRead] = []
    for component in (*discovered.use_cases, *discovered.jobs):
        compiled = kernel.compiler.compile(cast(type[Compilable], component))
        compiled.refuse_unservable(catalog, factories)
        if component in use_cases:
            reads.extend(compiled.model_reads())
    kernel.factory.verify(reads=reads)
