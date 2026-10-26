"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import dataclasses
import importlib.util
import logging
import warnings
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import prometheus_client
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.cache.wiring import CacheGateways, cache_module_for
from loom.core.checks import enforce_deploy_checks
from loom.core.config import (
    ConfigContext,
    ConfigKey,
    ConfigResolver,
    with_default_resolvers,
)
from loom.core.config.errors import ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.discovery import (
    InterfacesDiscoveryEngine,
    ManifestDiscoveryEngine,
    ModulesDiscoveryEngine,
)
from loom.core.discovery._utils import (
    collect_use_cases_from_interfaces,
    infer_model_from_use_case,
)
from loom.core.discovery.base import AGENTS_ONLY_HINT, DiscoveryResult
from loom.core.environment import Environment, resolve_environment, select_config_paths
from loom.core.introspection import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    ContributorRef,
    IntrospectionError,
    describe_app,
)
from loom.core.job.service import InlineJobService, JobService
from loom.core.locator import DEFAULT_CODE_PATH, add_code_path
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import (
    SERVER_PROFILE,
    BackendRuntime,
    PersistenceWiring,
    PoolMetricsSink,
    resolve_backend,
)
from loom.core.use_case.constants import CrudOp
from loom.prometheus import PrometheusMetricsAdapter
from loom.prometheus.db_pool import PrometheusPoolMetrics
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest._body import DEFAULT_MAX_BODY_BYTES, BodySizeLimitMiddleware
from loom.rest.auth import (
    Authenticator,
    JwtAuthConfig,
)
from loom.rest.auth.config import DEFAULT_EXCLUDE_PATHS
from loom.rest.autocrud import (
    CRUD_OP_CAPABILITY,
    build_auto_routes,
    crud_op_of,
    registered_repository_type,
    supported_crud_ops,
)
from loom.rest.compiler import RouteSources
from loom.rest.config import (
    DisableRouteConfig,
    RestInterfaceConfig,
    build_interfaces_from_config,
    validate_disable_routes_config,
    validate_interfaces_config,
)
from loom.rest.cors import CorsConfig
from loom.rest.fastapi._auto_ai import (
    _AgentDepsFactory as _AgentDepsFactory,
)
from loom.rest.fastapi._auto_ai import (
    _AiWiring,
    _bind_agent_resolver,
    _bind_agent_surface,
    _bind_mcp_resolver,
    _resolve_ai,
    _verify_agent_markers,
    _verify_mcp_markers,
)
from loom.rest.fastapi._auto_ai import (
    _compile_use_case_mcp as _compile_use_case_mcp,
)
from loom.rest.fastapi._auto_auth import _AuthWiring, _mount_authentication, _resolve_authentication
from loom.rest.fastapi._auto_sql import (
    _register_sql_collaborators,
    _resolve_sql,
    _warn_sql_endpoints,
)
from loom.rest.fastapi._auto_sql import (
    _SqlWiring as _SqlWiring,
)
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.health import HEALTH_PATH, mount_health, reject_health_collision
from loom.rest.fastapi.sql import (
    bind_sql_endpoints,
)
from loom.rest.middleware import TraceIdMiddleware
from loom.rest.model import RestInterface

_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # Annotations only: the AI pillar and the ClickHouse extra are imported at
    # run time solely by the branch that needs them.
    pass


# Written as data, never imported here: an application without an 'ai:'
# section must not gain a single AI import (FR-050).  The reference resolves
# inside 'describe_app', and only when a description is asked for.
_AGENTS_SECTION = "agents"
_AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"


class _DiscoveryInterfaces(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    modules: list[str] = msgspec.field(default_factory=list)
    warn_recommended: bool = True


class _DiscoveryModules(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    include: list[str] = msgspec.field(default_factory=list)


class _DiscoveryManifest(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    module: str = ""


class _DiscoveryConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    mode: str = "interfaces"
    interfaces: _DiscoveryInterfaces = msgspec.field(default_factory=_DiscoveryInterfaces)
    modules: _DiscoveryModules = msgspec.field(default_factory=_DiscoveryModules)
    manifest: _DiscoveryManifest = msgspec.field(default_factory=_DiscoveryManifest)


class _RestAuthConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    jwt: JwtAuthConfig | None = None
    # True when the composition root passes create_app(authenticator=...):
    # the configuration then says authentication is supplied in code.
    custom: bool = False
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS


class _RestConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    backend: str = "fastapi"
    title: str = "Loom API"
    version: str = "0.1.0"
    docs_url: str | None = "/docs"
    redoc_url: str | None = "/redoc"
    openapi_url: str | None = "/openapi.json"
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    auth: _RestAuthConfig = msgspec.field(default_factory=_RestAuthConfig)
    cors: CorsConfig | None = None
    # Endpoints declared without Python (loom.rest.config); converted into
    # the same RestInterface objects a Python subclass produces and handed to
    # the same compiler — see _fold_config_interfaces and create_fastapi_app.
    interfaces: dict[str, RestInterfaceConfig] = msgspec.field(default_factory=dict)
    # Subtractive per-environment overlay: every entry must match a route
    # declared in Python, or startup aborts (see
    # RestInterfaceCompiler.compile_sources).
    disable_routes: tuple[DisableRouteConfig, ...] = ()


class _AppConfig(msgspec.Struct, kw_only=True):
    name: str
    code_path: str = DEFAULT_CODE_PATH
    discovery: _DiscoveryConfig = msgspec.field(default_factory=_DiscoveryConfig)
    rest: _RestConfig = msgspec.field(default_factory=_RestConfig)


class _PersistenceConfig(msgspec.Struct, kw_only=True):
    backend: str = "sqlalchemy"


_DISCOVERY_ENGINES: dict[str, Callable[[_DiscoveryConfig], DiscoveryResult]] = {
    "interfaces": lambda cfg: InterfacesDiscoveryEngine(
        cfg.interfaces.modules,
        warn_recommended=cfg.interfaces.warn_recommended,
    ).discover(),
    "modules": lambda cfg: ModulesDiscoveryEngine(cfg.modules.include).discover(),
    "manifest": lambda cfg: ManifestDiscoveryEngine(cfg.manifest.module).discover(),
}


def _build_discovery_result(discovery_cfg: _DiscoveryConfig) -> DiscoveryResult:
    engine = _DISCOVERY_ENGINES.get(discovery_cfg.mode)
    if engine is None:
        raise ValueError(f"Unsupported discovery mode: {discovery_cfg.mode!r}")
    return engine(discovery_cfg)


def _build_celery_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
) -> Any | None:
    """Return a ``CeleryJobService`` if the celery extra is installed and configured.

    Returns ``None`` when the ``loom[celery]`` extra is absent or the
    ``celery`` section is, so the caller can fall back to
    :func:`_build_inline_service`. A section that fails validation raises.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.

    Returns:
        ``CeleryJobService`` instance, or ``None``.
    """
    if not ctx.has(ConfigKey.CELERY):
        # Checked before any import: an application without a celery section
        # must not pay for loading Celery. find_spec locates without importing.
        if importlib.util.find_spec("celery") is not None:
            warnings.warn(
                "Celery config section is missing — falling back to InlineJobService. "
                "Add a 'celery' section to your config to suppress this.",
                stacklevel=3,
            )
        return None
    try:
        from loom.celery.config import (  # type: ignore[import-untyped,unused-ignore]
            CeleryConfig as _CC,
        )
        from loom.celery.config import (
            create_celery_app,
        )
        from loom.celery.service import (
            CeleryJobService,  # type: ignore[import-untyped,unused-ignore]
        )

    except ImportError:
        return None
    celery_cfg = ctx.section(ConfigKey.CELERY, _CC)
    return CeleryJobService(
        create_celery_app(celery_cfg),
        metrics=result.metrics,
        factory=result.factory,
        executor=result.executor,
        observability_runtime=observability_runtime,
    )


def _build_inline_service(result: KernelRuntime) -> InlineJobService:
    """Return an ``InlineJobService`` backed by the kernel's factory and executor.

    Args:
        result: Kernel runtime carrying factory and executor.

    Returns:
        ``InlineJobService`` instance.
    """
    return InlineJobService(result.factory, result.executor)


def _configure_job_service(
    ctx: ConfigContext,
    result: KernelRuntime,
    observability_runtime: ObservabilityRuntime | None,
) -> None:
    """Register a ``JobService`` implementation in the container.

    Registers :class:`~loom.celery.service.CeleryJobService` when a
    ``celery`` config section is present and the ``loom[celery]`` extra is
    installed.  Falls back to
    :class:`~loom.core.job.service.InlineJobService` otherwise — enabling
    local development and tests without a broker.

    The registration uses ``APPLICATION`` scope so the service is created
    once and shared across all requests.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.
    """
    svc = _build_celery_service(ctx, result, observability_runtime) or _build_inline_service(result)
    result.container.register(JobService, lambda: svc, scope=Scope.APPLICATION)


def _load_observability_config(ctx: ConfigContext) -> ObservabilityConfig:
    """Load the top-level observability config; defaults only when it is absent.

    A section that fails validation stops startup: falling back to defaults
    would silently drop every setting it holds.
    """
    return ctx.section_or_default(
        ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()
    )


def _build_bootstrap(
    app_cfg: _AppConfig,
    ctx: ConfigContext,
    metrics: Any | None = None,
    config_interfaces: tuple[type[RestInterface[Any]], ...] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
    environment: Environment = Environment.PRODUCTION,
    pool_metrics: PoolMetricsSink | None = None,
) -> tuple[KernelRuntime, PersistenceWiring, DiscoveryResult, tuple[type[RestInterface[Any]], ...]]:
    discovered = _discover_components(app_cfg)
    # Captured before folding in config_interfaces: the explicit Python-only
    # set create_app needs to build RouteSources, instead of recovering it
    # afterwards from discovered.interfaces by index arithmetic.
    python_interfaces = discovered.interfaces
    discovered = _fold_config_interfaces(discovered, config_interfaces)
    persistence_cfg = _load_persistence_config(ctx)
    # Runs before the backend allocates resources (e.g. an engine): resolving
    # persistence is what creates them.
    _reject_autocrud_without_model(discovered)
    wiring = _resolve_persistence(ctx, persistence_cfg, discovered, pool_metrics)
    discovered = _gate_autocrud_capabilities(discovered, wiring, persistence_cfg.backend)
    wiring.prepare_models(discovered.models)
    result = _build_kernel_runtime(
        app_cfg,
        discovered,
        wiring,
        metrics=metrics,
        user_modules=modules,
        extra_modules=(cache_module_for(ctx),),
        environment=environment,
    )
    return result, wiring, discovered, python_interfaces


def _fold_config_interfaces(
    discovered: DiscoveryResult,
    config_interfaces: tuple[type[RestInterface[Any]], ...],
) -> DiscoveryResult:
    """Extend discovery with interfaces declared in ``app.rest.interfaces``.

    Folded in right after discovery so every later step — CRUD gating, model
    registration, use-case compilation — treats a config interface exactly
    like a Python one.  Additive only: a use case or model discovery already
    found is never duplicated, so a use case reached by both a Python route
    and a manifest/module scan still compiles once.

    Args:
        discovered: Result of the configured discovery engine.
        config_interfaces: Interfaces built from ``app.rest.interfaces``.

    Returns:
        *discovered*, unchanged when ``config_interfaces`` is empty, else
        with its ``models``, ``use_cases`` and ``interfaces`` extended.
    """
    if not config_interfaces:
        return discovered
    known_use_cases = set(discovered.use_cases)
    new_use_cases = [
        use_case
        for use_case in collect_use_cases_from_interfaces(list(config_interfaces))
        if use_case not in known_use_cases
    ]
    models = list(discovered.models)
    known_models = set(models)
    for use_case in new_use_cases:
        model = infer_model_from_use_case(use_case)
        if model is not None and model not in known_models:
            models.append(model)
            known_models.add(model)
    # ``replace`` copies fields added later.
    return dataclasses.replace(
        discovered,
        models=tuple(models),
        use_cases=discovered.use_cases + tuple(new_use_cases),
        interfaces=discovered.interfaces + config_interfaces,
    )


def _reject_autocrud_without_model(discovered: DiscoveryResult) -> None:
    """Reject generated CRUD routes over a model discovery never found.

    Auto-CRUD derives its operations from the model's repository, so an
    interface whose generated routes name an undiscovered model would mount
    routes that fail on their first request — whatever backend serves them, and
    including ``none``, which registers no repository at all. An interface that
    declared its own routes generates none and is left alone.

    Raises:
        RuntimeError: When such an interface exists, naming both classes.
    """
    known = set(discovered.models)
    for interface in discovered.interfaces:
        model = interface.auto_crud_model
        if model is not None and model not in known:
            raise RuntimeError(
                f"{interface.__name__} generates CRUD routes over "
                f"{model.__name__}, which discovery did not find. Add it to MODELS in "
                "your manifest module, or include its module in app.discovery, so its "
                "repository is registered."
            )


def _gate_autocrud_capabilities(
    discovered: DiscoveryResult, wiring: PersistenceWiring, backend: str
) -> DiscoveryResult:
    """Mount only the CRUD operations the serving repository class declares.

    The repository is the explicit ``repository_for`` class when there is one,
    else the backend's default repository type. An empty ``include`` is
    recomputed from scratch and narrowed to the declared capabilities, so a
    later boot on another backend in the same process sees its own routes;
    an explicit ``include`` is honoured verbatim or refused.

    Returns:
        The discovery result without the use cases of the routes that were
        pruned, so the kernel neither compiles nor registers them.

    Raises:
        RuntimeError: When the backend serves no repositories, when an
            explicit ``include`` names an operation the repository lacks, or
            when narrowing leaves no route; each names model and backend.
    """
    pruned: set[type[Any]] = set()
    for interface in discovered.interfaces:
        if interface.auto_crud_model is not None:
            pruned.update(_gate_interface(interface, interface.auto_crud_model, wiring, backend))
    mounted = {route.use_case for iface in discovered.interfaces for route in iface.routes}
    dropped = pruned - mounted
    # ``replace`` copies fields added later.
    return dataclasses.replace(
        discovered,
        use_cases=tuple(uc for uc in discovered.use_cases if uc not in dropped),
    )


def _gate_interface(
    interface: type[RestInterface[Any]],
    model: type[Any],
    wiring: PersistenceWiring,
    backend: str,
) -> frozenset[type[Any]]:
    """Narrow one interface's generated routes; return the use cases pruned from it."""
    repository_type = _serving_repository_type(model, wiring)
    if repository_type is None:
        raise RuntimeError(
            f"{interface.__name__} generates CRUD routes over {model.__name__}, but "
            f"persistence.backend {backend!r} serves no repositories."
        )
    supported = supported_crud_ops(repository_type)
    if interface.include:
        _reject_unsupported_include(interface, model, backend, supported)
        return frozenset()
    generated = build_auto_routes(model, ())
    interface.routes = tuple(r for r in generated if crud_op_of(r.use_case) in supported)
    if not interface.routes:
        raise RuntimeError(
            f"{interface.__name__} generates CRUD routes over {model.__name__}, but "
            f"backend {backend!r} ({repository_type.__name__}) supports none of "
            f"{sorted(op.value for op in CRUD_OP_CAPABILITY)}."
        )
    kept = {route.use_case for route in interface.routes}
    return frozenset(route.use_case for route in generated if route.use_case not in kept)


def _serving_repository_type(model: type[Any], wiring: PersistenceWiring) -> type[Any] | None:
    """Return the repository class that will serve *model* under *wiring*."""
    registered = registered_repository_type(model)
    return registered if registered is not None else wiring.default_repository_type


def _reject_unsupported_include(
    interface: type[RestInterface[Any]],
    model: type[Any],
    backend: str,
    supported: frozenset[CrudOp],
) -> None:
    missing = [op for op in interface.include if op in CRUD_OP_CAPABILITY and op not in supported]
    if missing:
        raise RuntimeError(
            f"{interface.__name__} includes {missing[0]!r} over {model.__name__}, which "
            f"backend {backend!r} does not support (missing: {missing})."
        )


def _discover_components(app_cfg: _AppConfig) -> DiscoveryResult:
    """Run discovery and reject a result with nothing to serve.

    Use cases, REST interfaces and manifest agents are each optional on their
    own: an application whose only content is agents has neither use cases nor
    interfaces. Only a result with none of the three is an error.
    """
    discovered = _build_discovery_result(app_cfg.discovery)
    if not discovered.use_cases and not discovered.interfaces and not discovered.agent_specs:
        raise RuntimeError(
            f"Nothing discovered: no UseCase, no RestInterface and no agents. {AGENTS_ONLY_HINT}"
        )
    return discovered


def _load_persistence_config(ctx: ConfigContext) -> _PersistenceConfig:
    return ctx.section_or_default(ConfigKey.PERSISTENCE, _PersistenceConfig, _PersistenceConfig())


def _resolve_persistence(
    ctx: ConfigContext,
    persistence_cfg: _PersistenceConfig,
    discovered: DiscoveryResult,
    pool_metrics: PoolMetricsSink | None = None,
) -> PersistenceWiring:
    """Build the wiring of the backend registered under ``persistence.backend``.

    Raises:
        ConfigError: When no backend is registered under that name.
    """
    return resolve_backend(persistence_cfg.backend).build(
        ctx, discovered.models, BackendRuntime(SERVER_PROFILE, pool_metrics=pool_metrics)
    )


def _documentation_paths(
    rest_cfg: _RestConfig,
    metrics_cfg: PrometheusObservabilityConfig,
) -> tuple[str, ...]:
    """Return the effective docs, schema, metrics and health paths of the application."""
    candidates = [rest_cfg.docs_url, rest_cfg.redoc_url, rest_cfg.openapi_url]
    if metrics_cfg.enabled:
        candidates.append(_metrics_path(metrics_cfg))
    candidates.append(HEALTH_PATH)
    return tuple(dict.fromkeys(path for path in candidates if path))


def _build_kernel_runtime(
    app_cfg: _AppConfig,
    discovered: DiscoveryResult,
    wiring: PersistenceWiring,
    metrics: Any | None = None,
    user_modules: Sequence[Callable[[LoomContainer], None]] = (),
    extra_modules: Sequence[Callable[[LoomContainer], None]] = (),
    environment: Environment = Environment.PRODUCTION,
) -> KernelRuntime:
    # Same order as the Celery worker bootstrap: caller modules first, then
    # repositories, then the framework's own modules.
    return create_kernel(
        config=app_cfg,
        use_cases=discovered.use_cases,
        modules=[*user_modules, wiring.repo_registration_module, *extra_modules],
        uow_factory=wiring.uow_factory,
        metrics=metrics,
        environment=environment,
    )


def _build_metrics_adapter(
    cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> Any | None:
    """Return a ``PrometheusMetricsAdapter`` when metrics are enabled, else ``None``.

    Args:
        cfg: Metrics feature config.
        registry: Optional Prometheus registry override.

    Returns:
        ``PrometheusMetricsAdapter`` or ``None``.
    """
    if not cfg.enabled:
        return None
    return PrometheusMetricsAdapter(registry=registry)


def _build_pool_metrics(
    cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
    application: str,
) -> PoolMetricsSink | None:
    """Return the connection-pool metrics sink when metrics are enabled, else ``None``.

    Shares the registry of :func:`_build_metrics_adapter`, so the pool gauges
    are served on the same scrape path as every other metric.
    """
    if not cfg.enabled:
        return None
    return PrometheusPoolMetrics(registry, application=application, entrypoint=SERVER_PROFILE.name)


def _metrics_path(cfg: PrometheusObservabilityConfig) -> str:
    """Return the REST metrics path declared in the Prometheus config."""
    return cfg.config.path if cfg.config is not None else "/metrics"


def _mount_optional_middlewares(
    app: FastAPI,
    rest_cfg: _RestConfig,
    metrics_cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Mount the body cap, request tracing, CORS and metrics middlewares.

    Order matters and is the reason this lives in one place.  Each call wraps
    the previous one, so the body cap ends up outside authentication (an
    oversized body is refused before any token is verified) while tracing wraps
    the cap (its ``413`` still carries a trace id).  CORS goes outermost so a
    preflight ``OPTIONS`` is answered without ever reaching the authentication
    middleware, which would refuse it for carrying no credentials.

    Args:
        app: FastAPI application to mutate.
        rest_cfg: Parsed ``app.rest`` section.
        metrics_cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=rest_cfg.max_body_bytes)
    app.add_middleware(TraceIdMiddleware)
    if metrics_cfg.enabled:
        _mount_metrics(app, metrics_cfg, registry)
    _mount_cors(app, rest_cfg.cors)


def _mount_cors(app: FastAPI, cors_cfg: CorsConfig | None) -> None:
    """Mount the CORS middleware when ``app.rest.cors`` is configured.

    Absent section means no middleware at all: an application that never
    intended to be called cross-origin does not start answering preflights.
    """
    if cors_cfg is None:
        return
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_cfg.allow_origins),
        allow_origin_regex=cors_cfg.allow_origin_regex,
        allow_methods=list(cors_cfg.allow_methods),
        allow_headers=list(cors_cfg.allow_headers),
        allow_credentials=cors_cfg.allow_credentials,
        expose_headers=list(cors_cfg.expose_headers),
        max_age=cors_cfg.max_age,
    )


def _mount_metrics(
    app: FastAPI,
    cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> None:
    """Add Prometheus middleware and scrape endpoint to *app*.

    Args:
        app: FastAPI application to mutate.
        cfg: Metrics feature config.
        registry: Optional Prometheus registry override.
    """
    path = _metrics_path(cfg)
    if "{" in path:
        raise ValueError(f"metrics.path must not contain path parameters, got: {path!r}")
    app.add_middleware(PrometheusMiddleware, registry=registry)
    scrape_registry = registry or prometheus_client.REGISTRY

    def _scrape() -> Response:
        return Response(
            content=prometheus_client.generate_latest(scrape_registry),
            media_type=prometheus_client.CONTENT_TYPE_LATEST,
        )

    def _scrape_trailing_slash() -> Response:
        # Return 404 for trailing-slash variant to avoid ambiguous scrape targets.
        return Response(status_code=404)

    app.add_api_route(path, _scrape, methods=["GET"], include_in_schema=False)
    app.add_api_route(f"{path}/", _scrape_trailing_slash, methods=["GET"], include_in_schema=False)


def _section_app_config(ctx: ConfigContext) -> _AppConfig:
    """Decode ``app``, naming the interface behind an unknown ``app.rest`` key.

    Validates ``app.rest.interfaces`` and ``app.rest.disable_routes`` against
    their raw, not-yet-decoded mapping first, since a ``dict``-keyed value
    (an interface name) is not information ``msgspec`` can recover once
    conversion to :class:`~loom.rest.config.RestInterfaceConfig` has already
    failed. Every other section of ``app`` decodes exactly as
    :func:`~loom.core.config.loader.section` already does — this only adds a
    check ahead of it for this one section.

    Both sections decode with untyped (``Any``) entries on purpose: typing an
    entry as ``dict[str, Any]`` would make ``msgspec`` reject a malformed one
    (a string where a mapping belongs) before the validators below ever see
    it, with a generic ``Expected 'object', got 'str'`` — reachable, but
    silent about which interface or disable-routes entry is at fault. Leaving
    the entry ``Any`` defers that check to :func:`validate_interfaces_config`
    and :func:`validate_disable_routes_config`, whose diagnostic names it.

    Args:
        ctx: Config context built from the supplied YAML files.

    Returns:
        The decoded :class:`_AppConfig`.

    Raises:
        RestInterfaceConfigError: If an interface, route, or disable-routes
            entry uses a key its config struct does not declare, or is not a
            mapping where one is required.
        ConfigError: If decoding fails for any other reason.
    """
    raw_interfaces = ctx.section_or_default("app.rest.interfaces", dict[str, Any], {})
    validate_interfaces_config(raw_interfaces)
    raw_disable_routes = ctx.section_or_default("app.rest.disable_routes", tuple[Any, ...], ())
    validate_disable_routes_config(raw_disable_routes)
    return ctx.section(ConfigKey.APP, _AppConfig)


def create_app(
    *config_paths: str,
    config_dir: str | None = None,
    environment: Environment | str | None = None,
    code_path: str | None = None,
    metrics_registry: CollectorRegistry | None = None,
    authenticator: Authenticator | None = None,
    resolvers: Sequence[ConfigResolver] = (),
    modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> FastAPI:
    """Create a FastAPI application from a config directory or explicit YAML files.

    The environment is resolved once: *environment* when given, else the
    ``LOOM_ENV`` variable, else production (see :mod:`loom.core.environment`).
    It picks the configuration overlay, drives the logger defaults, and is
    registered in the container for use cases to inject.

    Configuration comes from exactly one of two forms. *config_dir* loads its
    ``base.yaml``, then ``<environment>.yaml`` when present. Explicit
    *config_paths* are merged left-to-right as given. Either way later files
    override earlier ones, each file's ``includes`` merge before its own
    values, and the ``LOOM__SECTION__KEY`` variables merge last (see
    :mod:`loom.core.config.overrides`).

    In production and staging the deploy checks run once the logger is
    configured and the code path is importable, before the kernel is built
    (see :mod:`loom.core.checks`); ``checks.modules`` names the application
    modules registering its own checks:
    an error stops startup, a warning is logged, and ``checks.silenced``
    lists the ids accepted on purpose.

    ``${secrets:...}`` and ``${ssm:...}`` placeholders resolve through loom's
    built-in AWS resolvers unless *resolvers* supplies one with the same name.

    ``TraceIdMiddleware`` is mounted automatically. Structured logging and
    OTEL come from the top-level ``observability:`` section. Prometheus
    middleware is mounted when ``observability.prometheus.enabled`` is
    ``true``.

    Authentication is mechanism-agnostic: configure ``app.rest.auth.jwt`` for
    the built-in stateless JWT mechanism, or set ``app.rest.auth.custom:
    true`` and pass *authenticator* for any other.  Exactly one of the two
    may be active, and the configuration always names it.

    The optional ``sql:`` section wires the SQL subsystem: its connections
    open inside the app lifespan and two collaborators are registered in the
    container (null implementations raising an actionable ``ConfigError`` when
    the section is absent). ``CallerBoundSql`` derives the query roles from the
    verified caller and is what a use case serving a request should inject;
    ``SqlQueryService`` takes the roles as an argument and is for system work
    that has no caller. Connections opting in with
    ``sql_endpoint.enabled`` plus an explicit ``sql_endpoint.auth`` mount a
    ``POST /sql/{name}`` endpoint; ``auth: identity`` additionally requires a
    configured authentication mechanism, and a non-empty ``allowed_roles``
    also requires that mechanism to bind roles to the verified caller
    identity (for JWT, ``app.rest.auth.jwt.roles_claim``).

    Args:
        *config_paths: Paths to YAML configuration files, merged left to
            right. Mutually exclusive with *config_dir*.
        config_dir: Local directory holding ``base.yaml`` and the
            per-environment overlays. Mutually exclusive with *config_paths*.
        environment: Environment of the application; overrides ``LOOM_ENV``.
        code_path: Optional override for ``app.code_path``.  Resolved relative
            to the first config file when not absolute.
        metrics_registry: Optional Prometheus ``CollectorRegistry`` used for
            ``PrometheusMiddleware`` and the scrape endpoint.  Defaults to the
            global registry.  Pass a fresh ``CollectorRegistry()`` in tests to
            avoid ``ValueError: Duplicated timeseries`` when multiple apps with
            ``observability.prometheus.enabled: true`` are created in the same
            process.
        authenticator: Custom authentication mechanism.  Requires
            ``app.rest.auth.custom: true`` and excludes ``app.rest.auth.jwt``.
        resolvers: Resolvers for ``${name:key}`` placeholders, registered
            before the built-in ``secrets`` and ``ssm`` defaults.  A resolver
            named like a default replaces it.
        modules: Optional DI registration modules, each called with the
            container in order before repositories are registered and before
            use cases are verified.  Use them to bind ports no repository
            provides (a clock, an identity verifier, a bridge to another
            bounded context).  Same semantics as the Celery ``create_app``.
            Registration is last-write-wins and the framework registers its
            own bindings after these modules (repositories, cache,
            ``JobService``, ``SqlQueryService``, ``CallerBoundSql``,
            ``ObservabilityRuntime`` and, on PostgreSQL with an external
            schema, ``ElevationSink``), so
            a module binding one of those keys is silently replaced.

    Returns:
        Configured :class:`fastapi.FastAPI` application, ready to serve.

    Raises:
        ConfigError: When neither or both of *config_paths* and *config_dir*
            are given, when the environment is unknown, or when *authenticator*
            and ``app.rest.auth.custom: true`` do not come together.
        DeployCheckError: When a deploy check of level error fails in
            production or staging.

    Example — config directory, overlay chosen by ``LOOM_ENV``::

        # config/base.yaml, config/development.yaml, config/production.yaml
        app = create_app(config_dir="config")

    Example — single config::

        app = create_app("config/app.yaml")

    Example — a mechanism of your own (``app.rest.auth.custom: true``)::

        app = create_app("config/app.yaml", authenticator=MyMtlsAuthenticator())

    Example — bind a port a use case injects::

        def clock_module(container: LoomContainer) -> None:
            container.register(Clock, SystemClock, scope=Scope.APPLICATION)

        app = create_app("config/app.yaml", modules=[clock_module])

    Example — explicit layers, the environment named in code::

        app = create_app(
            "config/base.yaml", "config/production.yaml", environment="production"
        )

    Example — single file using inline includes::

        # config/app.yaml
        # includes:
        #   - base.yaml
        #   - secrets.yaml
        app = create_app("config/app.yaml")
    """
    resolved_environment = resolve_environment(environment)
    paths = select_config_paths(config_paths, config_dir, resolved_environment, caller="create_app")
    ctx = load_application_config(
        *paths, resolvers=with_default_resolvers(resolvers), environment=resolved_environment
    )
    app_cfg = _section_app_config(ctx)
    observability_cfg = _load_observability_config(ctx)
    observability_runtime = ObservabilityRuntime.from_config(
        observability_cfg, environment=resolved_environment
    )
    metrics_cfg = observability_cfg.prometheus

    auth = _resolve_authentication(
        app_cfg,
        authenticator,
        _documentation_paths(app_cfg.rest, metrics_cfg),
    )
    sql = _resolve_sql(ctx, auth)

    effective_code_path = add_code_path(
        code_path if code_path is not None else app_cfg.code_path,
        Path(paths[0]).resolve().parent,
    )
    # After the code path: checks.modules may name the application's modules.
    enforce_deploy_checks(ctx, resolved_environment)

    metrics_adapter = _build_metrics_adapter(metrics_cfg, metrics_registry)
    pool_metrics = _build_pool_metrics(metrics_cfg, metrics_registry, app_cfg.name)

    # Built ahead of _build_bootstrap so its use cases join discovery (CRUD
    # gating, model registration, kernel compilation) exactly like a
    # Python-discovered interface's; discovered.interfaces gains them right
    # after, in this same, deterministic Python-then-config order.
    config_interfaces = build_interfaces_from_config(app_cfg.rest.interfaces)
    result, wiring, discovered, python_interfaces = _build_bootstrap(
        app_cfg,
        ctx,
        metrics=metrics_adapter,
        config_interfaces=config_interfaces,
        modules=modules,
        environment=resolved_environment,
        pool_metrics=pool_metrics,
    )
    _configure_job_service(ctx, result, observability_runtime)
    _register_sql_collaborators(result.container, sql, observability_runtime)
    # Registered, not merely passed: capability spans resolve it from the
    # container, and an unregistered runtime makes every Scope.TOOL span a
    # silent no-op in production while passing every test that injects one.
    result.container.register(
        ObservabilityRuntime, lambda: observability_runtime, scope=Scope.APPLICATION
    )
    _reject_manifest_agents_without_ai_section(ctx, discovered.agent_specs)
    use_case_mcp_bindings = _verify_mcp_markers(
        discovered.use_cases, result.compiler, result.registry, ctx
    )
    ai = _resolve_ai(
        ctx,
        kernel=result,
        sql_cfg=sql.config,
        code_path=effective_code_path,
        manifest_agent_specs=discovered.agent_specs,
        use_case_mcp_bindings=use_case_mcp_bindings,
    )
    _verify_agent_markers(discovered.use_cases, result.compiler, result.registry, ai)
    _bind_agent_resolver(result, ai)
    _bind_mcp_resolver(result, ai)
    # Last: every service a use case may inject is registered by now.
    result.factory.verify()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as stack:
            # Pushed first, so the gateways close after the SQL registry.
            if result.container.is_registered(CacheGateways):
                for gateway in result.container.resolve(CacheGateways).distinct():
                    stack.push_async_callback(gateway.close)
            # Registry next: its clients close even when the persistence
            # lifespan fails to start or to shut down.
            if sql.registry is not None:
                await stack.enter_async_context(sql.registry)
            if ai.runtime is not None:
                await stack.enter_async_context(ai.runtime)
            await stack.enter_async_context(wiring.lifespan_init())
            yield

    route_sources = RouteSources(
        python=python_interfaces,
        config=config_interfaces,
        disabled=tuple((d.method, d.path) for d in app_cfg.rest.disable_routes),
    )
    app = create_fastapi_app(
        result,
        route_sources,
        observability_runtime=observability_runtime,
        title=app_cfg.rest.title,
        version=app_cfg.rest.version,
        docs_url=app_cfg.rest.docs_url,
        redoc_url=app_cfg.rest.redoc_url,
        openapi_url=app_cfg.rest.openapi_url,
        lifespan=lifespan,
    )
    mount_health(app, _load_persistence_config(ctx).backend, wiring.readiness)
    _mount_authentication(app, auth)
    _mount_optional_middlewares(app, app_cfg.rest, metrics_cfg, metrics_registry)
    if sql.config is not None:
        bind_sql_endpoints(
            app,
            service=sql.service,
            config=sql.config,
            authenticator=auth.authenticator,
            observability_runtime=observability_runtime,
        )
        _warn_sql_endpoints(sql.config, auth)
    _bind_agent_surface(app, ai, auth, observability_runtime)
    setattr(app.state, INTROSPECTION_STATE_ATTR, _build_introspection(app_cfg, ai))
    # Last: every route the application will ever serve is registered by now,
    # which is what makes the exclusion and collision checks meaningful.
    reject_health_collision(app)
    verify_exclusion_paths(app, auth.exclude_paths)
    _warn_anonymous_schema(app_cfg.rest, auth)
    return app


def _reject_manifest_agents_without_ai_section(
    ctx: ConfigContext,
    manifest_agent_specs: tuple[str, ...],
) -> None:
    """Refuse a manifest declaring agents in an application with no ``ai:`` section.

    Raises:
        ConfigError: When ``AGENTS`` names artifacts nothing can compile.
    """
    if manifest_agent_specs and not ctx.has(ConfigKey.AI):
        raise ConfigError(
            "The manifest declares AGENTS but the configuration has no 'ai:' section: "
            "agent artifacts need an engine and its model bindings to compile. Add the "
            "'ai:' section, or remove AGENTS from the manifest."
        )


def _build_introspection(app_cfg: _AppConfig, ai: _AiWiring) -> AppIntrospection:
    """Build the self-description of the application under construction.

    The AI contribution is a *string* reference resolved only when the
    description is asked for, so an application without an ``ai:`` section
    never imports the pillar (FR-050).
    """
    contributors: tuple[ContributorRef, ...] = ()
    if ai.config is not None:
        contributors = (
            ContributorRef(
                section=_AGENTS_SECTION,
                contributor=_AGENTS_CONTRIBUTOR,
                subject=ai.plans,
            ),
        )
    return AppIntrospection(
        name=app_cfg.name,
        version=app_cfg.rest.version,
        contributors=contributors,
    )


def describe_fastapi_app(app: FastAPI) -> dict[str, Any]:
    """Describe an application built by :func:`create_app`.

    The document always carries the application identity under ``"app"``, plus
    one section per wired pillar — ``"agents"`` when an ``ai:`` section is
    configured.  Only publishable values appear: no instructions, no model
    binding, no URL and no credential reference.

    Args:
        app: Application previously built by :func:`create_app`.

    Returns:
        JSON-encodable self-description of the application.

    Raises:
        IntrospectionError: When *app* was not built by :func:`create_app`, or
            when a pillar contribution cannot be resolved.

    Example::

        app = create_app("config/app.yaml")
        describe_fastapi_app(app)["app"]
        # {'name': 'billing', 'version': '1.4.0'}
    """
    introspection = getattr(app.state, INTROSPECTION_STATE_ATTR, None)
    if not isinstance(introspection, AppIntrospection):
        raise IntrospectionError(
            "this application carries no introspection state; only applications built by "
            "'create_app' can describe themselves."
        )
    return describe_app(introspection)


def _warn_anonymous_schema(rest_cfg: _RestConfig, auth: _AuthWiring) -> None:
    """Warn when an authenticated application publishes its schema anonymously.

    The OpenAPI document lists every route, parameter and field: it is the map
    of the attack surface. An application that bothers to authenticate and then
    serves that map without credentials is almost always an oversight.
    """
    if auth.authenticator is None:
        return
    anonymous = [
        path
        for path in (rest_cfg.openapi_url, rest_cfg.docs_url, rest_cfg.redoc_url)
        if path and path in auth.exclude_paths
    ]
    if not anonymous:
        return
    warnings.warn(
        f"Authentication is enabled but {', '.join(anonymous)} "
        f"{'are' if len(anonymous) > 1 else 'is'} served without it: the API schema "
        "describes every route, parameter and field of this service. Set "
        "'app.rest.openapi_url: null' (and the docs urls) in production, or remove "
        "them from the authentication 'exclude_paths'.",
        stacklevel=3,
    )
