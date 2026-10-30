"""The fixed recipe that composes an application from configuration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from loom.composition._ai import (
    AiWiring,
    bind_agent_resolver,
    bind_mcp_resolver,
    reject_manifest_agents_without_ai_section,
    resolve_ai,
    verify_agent_markers,
    verify_mcp_markers,
)
from loom.composition._jobs import configure_job_service
from loom.composition._reserved import refusing_reserved
from loom.composition._sql import SqlWiring, register_sql_collaborators, resolve_sql
from loom.composition.application import Application
from loom.composition.resources import Resource, ResourceKind, closing
from loom.composition.source import AppConfig, CompositionSource
from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.cache.wiring import CacheGateways, cache_module_for
from loom.core.checks import enforce_deploy_checks
from loom.core.config import ConfigKey, ConfigResolver, with_default_resolvers
from loom.core.config.overrides import load_application_config
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.plan import ExistsStep, LoadStep
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import add_code_path
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import BackendRuntime, PersistenceWiring
from loom.core.use_case.factory import ModelRead

if TYPE_CHECKING:
    from loom.celery.service import CeleryJobService
    from loom.composition.recipe import Adapter, Recipe
    from loom.core.config import ConfigContext
    from loom.core.di.container import LoomContainer

T = TypeVar("T")


def create_app(
    *config_paths: str,
    adapter: Adapter[T],
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> T:
    """Compose an application from configuration and mount *adapter*'s transport on it.

    Args:
        *config_paths: YAML files merged left to right; exclusive with *config_dir*.
        adapter: The transport, and how it wants the application composed.
        config_dir: Directory with ``base.yaml`` and the per-environment overlays.
        environment: Environment of the application; overrides ``LOOM_ENV``.
        code_path: Overrides ``app.code_path``, relative to the first config file.
        resolvers: Resolvers for ``${name:key}`` placeholders, ahead of the
            built-in ``secrets`` and ``ssm``.
        modules: DI modules run before repositories are registered. They may
            bind ``JobService`` and repositories, which the framework then
            leaves alone, but none of the keys it owns (see
            :class:`~loom.composition.errors.ReservedBindingError`).

    Returns:
        What *adapter* binds: an ASGI app, a Celery app, a handler.

    Raises:
        ReservedBindingError: When a module binds a framework-owned key.
    """
    application = build_application(
        *config_paths,
        adapter=adapter,
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        modules=modules,
    )
    return adapter.bind(application)


def build_application(
    *config_paths: str,
    adapter: Adapter[object],
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> Application:
    """Compose the application *adapter* asks for, without mounting a transport.

    Opens nothing: building a backend creates its engine or client, not a
    connection. :meth:`Application.check` and :meth:`Application.running`
    are what reach the outside.

    Args:
        *config_paths: See :func:`create_app`.
        adapter: Whose recipe to compose.
        config_dir: See :func:`create_app`.
        environment: See :func:`create_app`.
        code_path: See :func:`create_app`.
        resolvers: See :func:`create_app`.
        modules: See :func:`create_app`.

    Returns:
        The composed, closed application.

    Raises:
        ConfigError: When the configuration is incomplete or invalid.
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
        ReservedBindingError: When a module binds a framework-owned key.
    """
    source = load_source(
        *config_paths,
        config_dir=config_dir,
        environment=environment,
        code_path=code_path,
        resolvers=resolvers,
        caller="create_app",
    )
    return compose_application(source, adapter, modules=modules)


def load_source(
    *config_paths: str,
    config_dir: str | None,
    environment: Environment | str | None,
    code_path: str | None,
    resolvers: Sequence[ConfigResolver],
    caller: str,
    default_app: AppConfig | None = None,
) -> CompositionSource:
    """Load the configuration, environment and code an application composes from.

    Args:
        *config_paths: See :func:`create_app`.
        config_dir: See :func:`create_app`.
        environment: See :func:`create_app`.
        code_path: See :func:`create_app`.
        resolvers: See :func:`create_app`.
        caller: Entry point named when the paths are invalid.
        default_app: Taken when ``app`` is absent; ``None`` makes ``app`` required.
            Its type decodes a present section.

    Returns:
        The loaded source.

    Raises:
        ConfigError: When the configuration is incomplete or invalid.
    """
    resolved_environment = resolve_environment(environment)
    paths = select_config_paths(config_paths, config_dir, resolved_environment, caller=caller)
    ctx = load_application_config(
        *paths, resolvers=with_default_resolvers(resolvers), environment=resolved_environment
    )
    observability = ObservabilityRuntime.from_config(
        ctx.section_or_default(ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()),
        environment=resolved_environment,
    )
    app_cfg = (
        ctx.section(ConfigKey.APP, AppConfig)
        if default_app is None
        else ctx.section_optional(ConfigKey.APP, type(default_app)) or default_app
    )
    config_root = Path(paths[0]).resolve().parent
    return CompositionSource(
        config=ctx,
        environment=resolved_environment,
        code_path=add_code_path(
            code_path if code_path is not None else app_cfg.code_path, config_root
        ),
        config_root=config_root,
        app=app_cfg,
        observability=observability,
    )


def compose_application(
    source: CompositionSource,
    adapter: Adapter[object],
    *,
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> Application:
    """Compose the application *adapter* asks for from an already loaded *source*.

    :func:`build_application` loads the source from files; a test or a host
    that holds its configuration in memory calls this directly.

    Args:
        source: Configuration, environment and code of the application.
        adapter: Whose recipe to compose.
        modules: DI modules run before repositories are registered; see
            :func:`create_app`.

    Returns:
        The composed, closed application.

    Raises:
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
        ReservedBindingError: When a module binds a framework-owned key.
    """
    ctx, observability = source.config, source.observability
    recipe = adapter.recipe(source)
    # After the code path and discovery: checks.modules may name the
    # application's modules, and no resource exists yet.
    enforce_deploy_checks(ctx, source.environment, skip=recipe.skip_checks)

    discovered = recipe.discovered
    runtime = BackendRuntime(
        recipe.profile, pool_metrics=recipe.pool_metrics, connection_only=recipe.connection_only
    )
    wiring = recipe.backend.build(ctx, discovered.models, runtime)
    wiring.prepare_models(discovered.models)
    # Caller modules first, then repositories, then the framework's own. A
    # caller module binding a framework-owned key fails right after it runs.
    kernel = create_kernel(
        config=source.app,
        use_cases=discovered.use_cases,
        modules=[
            *refusing_reserved(modules),
            wiring.repo_registration_module,
            cache_module_for(ctx),
        ],
        uow_factory=wiring.uow_factory,
        metrics=recipe.metrics,
        environment=source.environment,
    )
    celery_service = configure_job_service(ctx, kernel, observability)
    sql = resolve_sql(ctx)
    register_sql_collaborators(kernel.container, sql, observability)
    # Registered, not merely passed: capability spans resolve it from the
    # container, and an unregistered runtime makes every Scope.TOOL span a
    # silent no-op in production while passing every test that injects one.
    kernel.container.register(ObservabilityRuntime, lambda: observability, scope=Scope.APPLICATION)
    ai = _compose_agents(ctx, recipe, kernel, sql, source.code_path)
    # Last: every service a use case may inject is registered by now.
    kernel.factory.verify(reads=_marker_reads(kernel, discovered.use_cases))

    return Application(
        kernel=kernel,
        config=ctx,
        environment=source.environment,
        code_path=source.code_path,
        discovered=discovered,
        observability=observability,
        sql=sql,
        ai=ai,
        persistence=wiring,
        resources=_resources(kernel, wiring, recipe.backend.name, sql, ai, celery_service),
    )


def _marker_reads(
    kernel: KernelRuntime, use_cases: Sequence[type[Compilable]]
) -> tuple[ModelRead, ...]:
    """The models the compiled plans read through ``LoadById``, ``Load`` or ``Exists``."""
    reads: list[ModelRead] = []
    for use_case in use_cases:
        plan = kernel.compiler.get_plan(use_case)
        if plan is None:
            continue
        steps: tuple[LoadStep | ExistsStep, ...] = (*plan.load_steps, *plan.exists_steps)
        reads.extend(ModelRead(use_case, step.name, step.entity_type) for step in steps)
    return tuple(reads)


def _compose_agents(
    ctx: ConfigContext,
    recipe: Recipe,
    kernel: KernelRuntime,
    sql: SqlWiring,
    code_path: Path,
) -> AiWiring:
    """Build the agent runtime when the entry point hosts it, else nothing."""
    if not recipe.agents:
        return AiWiring(config=None, runtime=None)
    discovered = recipe.discovered
    reject_manifest_agents_without_ai_section(ctx, discovered.agent_specs)
    use_case_mcp_bindings = verify_mcp_markers(
        discovered.use_cases, kernel.compiler, kernel.registry, ctx
    )
    ai = resolve_ai(
        ctx,
        kernel=kernel,
        sql_cfg=sql.config,
        code_path=code_path,
        manifest_agent_specs=discovered.agent_specs,
        use_case_mcp_bindings=use_case_mcp_bindings,
    )
    verify_agent_markers(discovered.use_cases, kernel.compiler, kernel.registry, ai)
    bind_agent_resolver(kernel, ai)
    bind_mcp_resolver(kernel, ai)
    return ai


def _resources(
    kernel: KernelRuntime,
    wiring: PersistenceWiring,
    backend_name: str,
    sql: SqlWiring,
    ai: AiWiring,
    celery_service: CeleryJobService | None,
) -> tuple[Resource, ...]:
    """Everything the application owns; :class:`Application` orders it by kind."""
    resources: list[Resource] = []
    if celery_service is not None:
        resources.append(
            Resource(
                kind=ResourceKind.CELERY,
                name="jobs",
                manager=closing(lambda: asyncio.to_thread(celery_service.celery_app.close)),
            )
        )
    if kernel.container.is_registered(CacheGateways):
        gateways = kernel.container.resolve(CacheGateways).distinct()
        for name, gateway in zip(("data", "counters"), gateways, strict=False):
            resources.append(
                Resource(kind=ResourceKind.CACHE, name=name, manager=closing(gateway.close))
            )
    registry, runtime = sql.registry, ai.runtime
    if registry is not None:
        resources.append(
            Resource(
                kind=ResourceKind.SQL,
                name="clickhouse",
                manager=lambda: registry,
                check=registry.check,
            )
        )
    if runtime is not None:
        resources.append(Resource(kind=ResourceKind.AGENTS, name="agents", manager=lambda: runtime))
    resources.append(
        Resource(
            kind=ResourceKind.STORE,
            name=backend_name,
            manager=wiring.lifespan_init,
            check=wiring.startup_checks,
            readiness=wiring.readiness,
        )
    )
    return tuple(resources)
