"""The ordered phases that :meth:`~loom.composition.project.Project.compose` runs.

The order: :func:`plan` takes the runtime's recipe; :func:`build_stores` builds
every store before any registration; :func:`register` fills the container and
builds the kernel; :meth:`Recipe.analysed` analyses the dependency graph;
:func:`wire_ai` builds the optional agent host; :func:`seal` seals the
container. Nothing registers after :func:`register`.
"""

from __future__ import annotations

import time
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar, cast

from loom.composition._ai import AiWiring, MarkerDeclarations
from loom.composition._jobs import configure_job_service
from loom.composition._reserved import reserved_keys
from loom.composition._sql import SqlWiring, register_sql_collaborators, resolve_sql
from loom.composition.errors import ReservedBindingError
from loom.composition.recipe import Recipe, Runtime
from loom.connections.aws import AwsClients
from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap import BootstrapError, Kernel
from loom.core.bootstrap.kernel import KernelRecipe, seeded_container
from loom.core.cache.wiring import cache_module_for
from loom.core.checks import enforce_deploy_checks
from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.di.container import ResolutionError
from loom.core.di.graph import GraphAnalysis
from loom.core.di.installer import Installer
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.declared_observers import build_execution_observers
from loom.core.engine.execution_observer import FanOutExecutionObserver
from loom.core.environment import Environment
from loom.core.identity import DEFAULT_MAX_DELEGATION_SECONDS, configure_delegation
from loom.core.job.config import JobSettings
from loom.core.model import BaseModel
from loom.core.observability.observability import Observability
from loom.core.resources import Resource, ResourceKind
from loom.core.stores import StorePluginContext, StoreWiring
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
        store_plugins: The name of the store plugin each store's wiring was built by.
        models: The models each store serves.
        wirings: Each store's wiring, its models prepared.
    """

    job_settings: JobSettings
    store_plugins: dict[StoreKey, str]
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
                readiness_key=self.store_plugins[key] if key == DEFAULT_STORE else key,
            )
            for key, wiring in self.wirings.items()
        )


def plan(project: Project, runtime: Runtime[object]) -> Recipe:
    """Ask *runtime* for its recipe, then refuse a failing deploy check.

    A lenient recipe runs no deploy check.

    Args:
        project: What the application composes from.
        runtime: Whose recipe to take.

    Returns:
        The recipe.

    Raises:
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
    """
    recipe = runtime.recipe(project)
    if not recipe.lenient:
        enforce_deploy_checks(project.config, project.environment, scope=recipe.check_scope)
    return recipe


def build_stores(project: Project, recipe: Recipe, aws: AwsClients) -> Stores:
    """Read each job's settings, then build and prepare every store the discovered models use.

    The stores share one cursor codec, built only when
    ``stores.settings.cursors`` is configured.

    Args:
        project: What the application composes from.
        recipe: What it composes.
        aws: The application's AWS clients, which the store backends take their clients from.

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
        key: project.resolve_store_plugin(key, default=recipe.default_store_plugin)
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
        context = StorePluginContext(
            recipe.profile,
            pool_metrics=recipe.pool_metrics,
            connection_only=recipe.connection_only,
            catalog=catalog,
            store=key,
            cursor_codec=cursor_codec,
            aws=aws,
        )
        wiring = wirings[key] = backends[key].build(ctx, models, context)
        wiring.prepare_models(models)
    return Stores(
        job_settings=job_settings,
        store_plugins={key: backends[key].name for key in stores},
        models=stores,
        wirings=wirings,
    )


def register(
    project: Project,
    recipe: Recipe,
    stores: Stores,
    aws: AwsClients,
    installers: tuple[Installer, ...],
) -> tuple[Kernel, SqlWiring, tuple[Resource, ...]]:
    """Fill a container and build the kernel on it, then register the job service and SQL.

    The order: the seed keys (the application's ``AppConfig``,
    ``Environment``, ``Observability`` and ``StoreCatalog``); the
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
        aws: The application's AWS clients, registered in the container and
            given to the job transports.
        installers: The discovered installers, built by :func:`build_installers`.

    Returns:
        The kernel, the SQL wiring and the job transports' resources.

    Raises:
        ConfigError: When an entry of ``observability.execution_observers`` is not a
            buildable execution observer, a ``ConfiguredInstaller`` section, the ``cache``
            section, an elevation,
            ``stores.settings.rollback_timeout``,
            ``identity.max_delegation_seconds`` or the ``sql`` section is
            invalid. Raised as is.
        ReservedBindingError: When a caller module or an override binds or
            replaces a framework-owned key.
        BootstrapError: Wrapping any other failure of a module or of a use
            case's compilation.
        JobTransportError: When the job transport is unknown or its extra is
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
    execution_observer = FanOutExecutionObserver.combine(
        recipe.execution_observer,
        build_execution_observers(project.observability_config.execution_observers),
    )
    seeds = {
        type(project.app): project.app,
        Environment: project.environment,
        Observability: observability,
        StoreCatalog: catalog,
    }
    with seeded_container(
        KernelRecipe.container(),
        seeds,
        modules=(*(installer.install for installer in installers), *recipe.overrides),
        reserved=reserved_keys(catalog.specs),
        error=ReservedBindingError.for_key,
    ) as container:
        for wiring in stores.wirings.values():
            wiring.repo_registration_module(container)
        container.register_instance(RepositoryCatalog, RepositoryCatalog(container, stores.models))
        container.register(AwsClients, lambda: aws, scope=Scope.APPLICATION)
        cache(container)
        if sink is not None:
            container.register_instance(ElevationSink, sink)
        kernel = KernelRecipe(
            recipe.discovered.use_cases,
            execution_observer=execution_observer,
            coordinator=coordinator,
        ).install(container)
    job_resources = configure_job_service(
        ctx,
        kernel,
        observability,
        transport=project.app.job_service,
        jobs=recipe.discovered.jobs,
        callbacks=recipe.discovered.callbacks,
        settings=stores.job_settings,
        aws=aws,
        can_wait_in_process=recipe.profile.can_wait_in_process,
    )
    sql = resolve_sql(ctx)
    register_sql_collaborators(kernel.container, sql, observability)
    return kernel, sql, job_resources


class ResourceRoster:
    """The resources of an application, each owner's, unique by label and by readiness key."""

    def __init__(self) -> None:
        self._resources: list[Resource] = []
        self._label_owners: dict[str, str] = {}
        self._readiness_owners: dict[str, str] = {}

    @property
    def resources(self) -> tuple[Resource, ...]:
        """Every resource added, in the order added."""
        return tuple(self._resources)

    def add(self, owner: str, resources: Iterable[Resource]) -> None:
        """Add *resources* as *owner*'s.

        Raises:
            ConfigError: When a resource has the label or the readiness key of
                one already added; the message names both owners.
        """
        for resource in resources:
            self._claim(self._label_owners, resource.label, owner, "resource")
            if resource.answers_health_probe:
                self._claim(self._readiness_owners, resource.readiness_key, owner, "readiness key")
            self._resources.append(resource)

    @staticmethod
    def _claim(owners: dict[str, str], name: str, owner: str, what: str) -> None:
        if name in owners:
            raise ConfigError(
                f"{owner} declares the {what} {name!r}, which {owners[name]} already declares"
            )
        owners[name] = owner


def build_installers(
    config: ConfigContext, installers: Collection[type[Installer]]
) -> tuple[Installer, ...]:
    """Build each installer class from *config*, in order, resolving its placeholders.

    Raises:
        ConfigError: When an installer's configuration section is missing or invalid.
    """
    return tuple(installer.from_config(config) for installer in installers)


def check_installers(config: ConfigContext, installers: Collection[type[Installer]]) -> None:
    """Validate what each installer class reads from *config*, as written, resolving no placeholder.

    Raises:
        ConfigError: When an installer's configuration section is invalid as written.
    """
    for installer in installers:
        installer.check_config(config)


def wire_ai(
    project: Project,
    recipe: Recipe,
    kernel: Kernel,
    analysis: GraphAnalysis,
    sql: SqlWiring,
) -> AiWiring:
    """Build the agent host when the entry point hosts it, else nothing.

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
    declared = MarkerDeclarations.from_analysis(analysis, kernel.compiler, kernel.registry)
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
    kernel: Kernel,
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
