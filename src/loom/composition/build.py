"""The fixed recipe that composes an application from configuration."""

from __future__ import annotations

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
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.locator import add_code_path
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import BackendRuntime, PersistenceWiring

if TYPE_CHECKING:
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
        modules: DI modules run before repositories are registered.

    Returns:
        What *adapter* binds: an ASGI app, a Celery app, a handler.
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
    """
    resolved_environment = resolve_environment(environment)
    paths = select_config_paths(config_paths, config_dir, resolved_environment, caller="create_app")
    ctx = load_application_config(
        *paths, resolvers=with_default_resolvers(resolvers), environment=resolved_environment
    )
    observability = ObservabilityRuntime.from_config(
        ctx.section_or_default(ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()),
        environment=resolved_environment,
    )
    app_cfg = ctx.section(ConfigKey.APP, AppConfig)
    config_root = Path(paths[0]).resolve().parent
    effective_code_path = add_code_path(
        code_path if code_path is not None else app_cfg.code_path, config_root
    )
    source = CompositionSource(
        config=ctx,
        environment=resolved_environment,
        code_path=effective_code_path,
        config_root=config_root,
        app=app_cfg,
        observability=observability,
    )
    return compose_application(source, adapter, modules=modules)


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
        modules: DI modules run before repositories are registered.

    Returns:
        The composed, closed application.

    Raises:
        DeployCheckError: When a deploy check of level error fails in a
            deployed environment.
    """
    ctx, observability = source.config, source.observability
    recipe = adapter.recipe(source)
    # After the code path and discovery: checks.modules may name the
    # application's modules, and no resource exists yet.
    enforce_deploy_checks(ctx, source.environment, skip=recipe.skip_checks)

    discovered = recipe.discovered
    wiring = recipe.backend.build(
        ctx, discovered.models, BackendRuntime(recipe.profile, pool_metrics=recipe.pool_metrics)
    )
    wiring.prepare_models(discovered.models)
    # Caller modules first, then repositories, then the framework's own.
    kernel = create_kernel(
        config=source.app,
        use_cases=discovered.use_cases,
        modules=[*modules, wiring.repo_registration_module, cache_module_for(ctx)],
        uow_factory=wiring.uow_factory,
        metrics=recipe.metrics,
        environment=source.environment,
    )
    configure_job_service(ctx, kernel, observability)
    sql = resolve_sql(ctx)
    register_sql_collaborators(kernel.container, sql, observability)
    # Registered, not merely passed: capability spans resolve it from the
    # container, and an unregistered runtime makes every Scope.TOOL span a
    # silent no-op in production while passing every test that injects one.
    kernel.container.register(ObservabilityRuntime, lambda: observability, scope=Scope.APPLICATION)
    ai = _compose_agents(ctx, recipe, kernel, sql, source.code_path)
    # Last: every service a use case may inject is registered by now.
    kernel.factory.verify()

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
        resources=_resources(kernel, wiring, recipe.backend.name, sql, ai),
    )


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
) -> tuple[Resource, ...]:
    """Everything the application owns; :class:`Application` orders it by kind."""
    resources: list[Resource] = []
    if kernel.container.is_registered(CacheGateways):
        gateways = kernel.container.resolve(CacheGateways).distinct()
        for name, gateway in zip(("data", "counters"), gateways, strict=False):
            resources.append(
                Resource(kind=ResourceKind.CACHE, name=name, manager=closing(gateway.close))
            )
    registry, runtime = sql.registry, ai.runtime
    if registry is not None:
        resources.append(
            Resource(kind=ResourceKind.SQL, name="clickhouse", manager=lambda: registry)
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
