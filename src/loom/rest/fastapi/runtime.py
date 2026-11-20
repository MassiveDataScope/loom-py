"""The FastAPI adapter: how an HTTP entry point composes and serves an application."""

from __future__ import annotations

import dataclasses
import functools
import logging
import warnings
from collections.abc import AsyncIterator, Callable, Collection, Mapping
from contextlib import asynccontextmanager
from typing import Any

import msgspec
import prometheus_client
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

from loom.composition import (
    AiWiring,
    Application,
    CompositionErrorCode,
    CompositionStateError,
    Project,
    Recipe,
)
from loom.core.checks import CheckScope
from loom.core.config import (
    ConfigContext,
    ConfigKey,
)
from loom.core.config.errors import ConfigError
from loom.core.config.loader import convert_section
from loom.core.discovery._utils import infer_model_from_use_case
from loom.core.discovery.base import DiscoveryResult
from loom.core.introspection import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    ContributorRef,
)
from loom.core.observability.config import PrometheusObservabilityConfig
from loom.core.stores import (
    SERVER_PROFILE,
    PoolMetricsSink,
    StorePlugin,
)
from loom.core.stores.catalog import SQLALCHEMY_BACKEND, StoreKey, store_of
from loom.core.use_case.constants import CrudOp
from loom.prometheus import PrometheusMetricsRecorder
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
from loom.rest.discovery import interfaces_in, route_use_cases
from loom.rest.fastapi._auto_ai import _bind_agent_surfaces
from loom.rest.fastapi._auto_auth import (
    _AuthWiring,
    _mount_authentication,
    _require_deployed_jwt_claims,
    _resolve_authentication,
)
from loom.rest.fastapi._auto_sql import _check_sql_section, _warn_sql_endpoints
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.health import HEALTH_PATH, mount_health, reject_health_collision
from loom.rest.fastapi.sql import (
    mount_sql_endpoints,
)
from loom.rest.middleware import TraceIdMiddleware
from loom.rest.model import RestInterface

_logger = logging.getLogger(__name__)


_AGENTS_SECTION = "agents"
_AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"


class _RestAuthConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    jwt: JwtAuthConfig | None = None
    custom: bool = False
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS


_REST_SECTION = "app.rest"
_INTERFACES_KEY = "app.rest.interfaces"
_DISABLE_ROUTES_KEY = "app.rest.disable_routes"


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
    interfaces: dict[str, Any] = msgspec.field(default_factory=dict)
    disable_routes: tuple[Any, ...] = ()


def _fold_config_interfaces(
    discovered: DiscoveryResult,
    config_interfaces: tuple[type[RestInterface[Any]], ...],
) -> DiscoveryResult:
    """Extend discovery with interfaces declared in ``app.rest.interfaces``.

    Folded in right after discovery so every later step — CRUD gating, model
    registration, use-case compilation — treats a config interface exactly
    like a Python one.  Additive only: a use case or model discovery already
    found is never duplicated, so a use case reached by both a Python route
    and the package scan still compiles once.

    Args:
        discovered: What the scan of ``app.packages`` found.
        config_interfaces: Interfaces built from ``app.rest.interfaces``.

    Returns:
        *discovered*, unchanged when ``config_interfaces`` is empty, else
        with its ``models`` and ``use_cases`` extended.
    """
    if not config_interfaces:
        return discovered
    new_use_cases = _unknown_route_use_cases(discovered, config_interfaces)
    models = list(discovered.models)
    known_models = set(models)
    for use_case in new_use_cases:
        model = infer_model_from_use_case(use_case)
        if model is not None and model not in known_models:
            models.append(model)
            known_models.add(model)
    return discovered.with_changes(
        models=tuple(models),
        use_cases=discovered.use_cases + new_use_cases,
    )


def _unknown_route_use_cases(
    discovered: DiscoveryResult, interfaces: tuple[type[RestInterface[Any]], ...]
) -> tuple[type[Any], ...]:
    """The use cases the routes of *interfaces* reference that *discovered* does not hold."""
    known = set(discovered.use_cases)
    return tuple(use_case for use_case in route_use_cases(interfaces) if use_case not in known)


def _reject_autocrud_without_model(
    discovered: DiscoveryResult, interfaces: tuple[type[RestInterface[Any]], ...]
) -> None:
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
    for interface in interfaces:
        model = interface.auto_crud_model
        if model is not None and model not in known:
            raise RuntimeError(
                f"{interface.__name__} generates CRUD routes over "
                f"{model.__name__}, which discovery did not find. Add its package to "
                "app.packages so its "
                "repository is registered."
            )


def _gate_autocrud_capabilities(
    discovered: DiscoveryResult,
    interfaces: tuple[type[RestInterface[Any]], ...],
    project: Project,
) -> DiscoveryResult:
    """Mount only the CRUD operations the serving repository class declares.

    The repository is the explicit ``repository_for`` class when there is one,
    else the ``repository_type`` of the backend of the model's store. An
    empty ``include`` is recomputed from scratch and narrowed to the declared
    capabilities, so a later boot on another backend in the same process
    sees its own routes; an explicit ``include`` is honoured verbatim or
    refused.

    Returns:
        The discovery result without the use cases of the routes that were
        pruned, so the kernel neither compiles nor registers them.

    Raises:
        RuntimeError: When the backend serves no repositories, when an
            explicit ``include`` names an operation the repository lacks, or
            when narrowing leaves no route; each names model and backend.
    """
    pruned: set[type[Any]] = set()
    for interface in interfaces:
        if interface.auto_crud_model is not None:
            model = interface.auto_crud_model
            key = store_of(model)
            backend = project.resolve_store_backend(key, default=SQLALCHEMY_BACKEND)
            pruned.update(_gate_interface(interface, model, key, backend))
    mounted = {route.use_case for iface in interfaces for route in iface.routes}
    dropped = pruned - mounted
    return discovered.with_changes(
        use_cases=tuple(uc for uc in discovered.use_cases if uc not in dropped),
    )


def _gate_interface(
    interface: type[RestInterface[Any]],
    model: type[Any],
    key: StoreKey,
    backend: StorePlugin,
) -> frozenset[type[Any]]:
    """Narrow one interface's generated routes; return the use cases pruned from it."""
    backend_name = backend.name
    repository_type = _serving_repository_type(model, backend)
    if repository_type is None:
        raise RuntimeError(
            f"{interface.__name__} generates CRUD routes over {model.__name__}, but "
            f"stores.{key}.backend {backend_name!r} serves no repositories."
        )
    supported = supported_crud_ops(repository_type)
    if interface.include:
        _reject_unsupported_include(interface, model, backend_name, supported)
        return frozenset()
    generated = build_auto_routes(model, ())
    interface.routes = tuple(r for r in generated if crud_op_of(r.use_case) in supported)
    if not interface.routes:
        raise RuntimeError(
            f"{interface.__name__} generates CRUD routes over {model.__name__}, but "
            f"backend {backend_name!r} ({repository_type.__name__}) supports none of "
            f"{sorted(op.value for op in CRUD_OP_CAPABILITY)}."
        )
    kept = {route.use_case for route in interface.routes}
    return frozenset(route.use_case for route in generated if route.use_case not in kept)


def _serving_repository_type(model: type[Any], backend: StorePlugin) -> type[Any] | None:
    """Return the repository class that will serve *model* on *backend*."""
    registered = registered_repository_type(model)
    if registered is not None:
        return registered
    try:
        return backend.repository_type
    except AttributeError as exc:
        raise ConfigError(
            f"store backend {type(backend).__name__} does not declare repository_type"
        ) from exc


def _reject_unsupported_include(
    interface: type[RestInterface[Any]],
    model: type[Any],
    backend_name: str,
    supported: frozenset[CrudOp],
) -> None:
    missing = [op for op in interface.include if op in CRUD_OP_CAPABILITY and op not in supported]
    if missing:
        raise RuntimeError(
            f"{interface.__name__} includes {missing[0]!r} over {model.__name__}, which "
            f"backend {backend_name!r} does not support (missing: {missing})."
        )


def _discover_components(project: Project) -> DiscoveryResult:
    """Run discovery, add its interfaces' use cases, and refuse a result with nothing to serve.

    Use cases, REST interfaces and ``app.agents`` are each optional on their
    own: an application whose only content is agents has neither use cases nor
    interfaces. Only a result with none of the three is an error.
    """
    discovered = project.discovered
    interfaces = interfaces_in(discovered)
    if not discovered.use_cases and not interfaces and not discovered.agent_specs:
        raise RuntimeError(
            "Nothing discovered: no UseCase, no RestInterface and no agents. "
            "Set app.packages or app.agents."
        )
    return discovered.with_changes(
        use_cases=discovered.use_cases + _unknown_route_use_cases(discovered, interfaces)
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


def _build_metrics_adapter(
    cfg: PrometheusObservabilityConfig,
    registry: CollectorRegistry | None,
) -> Any | None:
    """Return a ``PrometheusMetricsRecorder`` when metrics are enabled, else ``None``.

    Args:
        cfg: Metrics feature config.
        registry: Optional Prometheus registry override.

    Returns:
        ``PrometheusMetricsRecorder`` or ``None``.
    """
    if not cfg.enabled:
        return None
    return PrometheusMetricsRecorder(registry=registry)


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


def mount_request_stack(
    app: Starlette,
    declared: RestDeclaration,
    *,
    mount_metrics: Callable[[], None] | None = None,
) -> None:
    """Mount the request middlewares every HTTP transport serves, in their one order.

    Each call wraps the previous one: from the outside in, CORS, metrics,
    request tracing, the body cap and authentication. The body cap sits
    outside authentication, so an oversized body is refused before any token
    is verified; tracing wraps the cap, so its ``413`` and every ``401`` carry
    a trace id; CORS goes outermost, so a preflight ``OPTIONS`` is answered
    without reaching authentication, which would refuse it for carrying no
    credentials.

    Args:
        app: Application to mutate.
        declared: What the recipe resolved.
        mount_metrics: Mounts the metrics middleware, for a transport serving
            a scrape.
    """
    _mount_authentication(app, declared.auth)
    _mount_request_guards(app, declared.rest)
    if mount_metrics is not None:
        mount_metrics()
    _mount_cors(app, declared.rest.cors)


def _mount_request_guards(app: Starlette, rest_cfg: _RestConfig) -> None:
    """Mount the body cap and, around it, request tracing."""
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=rest_cfg.max_body_bytes)
    app.add_middleware(TraceIdMiddleware)


def _mount_cors(app: Starlette, cors_cfg: CorsConfig | None) -> None:
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
        return Response(status_code=404)

    app.add_api_route(path, _scrape, methods=["GET"], include_in_schema=False)
    app.add_api_route(f"{path}/", _scrape_trailing_slash, methods=["GET"], include_in_schema=False)


def _section_rest_config(ctx: ConfigContext) -> _RestConfig:
    """Decode ``app.rest``; its defaults when the section is absent.

    ``interfaces`` and ``disable_routes`` come out untouched: :func:`served_routes`
    validates and decodes them, naming the entry at fault.

    Raises:
        ConfigError: If the section does not decode.
    """
    return ctx.section_or_default(_REST_SECTION, _RestConfig, _RestConfig())


def _route_config(
    ctx: ConfigContext,
) -> tuple[dict[str, RestInterfaceConfig], tuple[DisableRouteConfig, ...]]:
    """Validate route declarations and decode them without resolving REST auth."""
    raw_interfaces = convert_section(ctx.raw(_INTERFACES_KEY, {}), _INTERFACES_KEY, dict[str, Any])
    validate_interfaces_config(raw_interfaces)
    raw_disable_routes = convert_section(
        ctx.raw(_DISABLE_ROUTES_KEY, ()), _DISABLE_ROUTES_KEY, tuple[Any, ...]
    )
    validate_disable_routes_config(raw_disable_routes)
    return (
        convert_section(raw_interfaces, _INTERFACES_KEY, dict[str, RestInterfaceConfig]),
        convert_section(raw_disable_routes, _DISABLE_ROUTES_KEY, tuple[DisableRouteConfig, ...]),
    )


def _build_introspection(name: str, rest_cfg: _RestConfig, ai: AiWiring) -> AppIntrospection:
    """Build the self-description of the application under construction.

    The AI contribution is a *string* reference resolved only when the
    description is asked for, so an application without an ``ai:`` section
    never imports the pillar.
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
        name=name,
        version=rest_cfg.version,
        contributors=contributors,
    )


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


InterfaceSelector = Callable[
    [Mapping[str, tuple[type[RestInterface[Any]], ...]]], Collection[type[RestInterface[Any]]]
]
"""Picks the interfaces to serve from every interface by name: a Python one by
its class name, a YAML one by its ``app.rest.interfaces`` key."""


def _interfaces_by_name(
    python: tuple[type[RestInterface[Any]], ...],
    yaml_keys: Collection[str],
    config: tuple[type[RestInterface[Any]], ...],
) -> dict[str, tuple[type[RestInterface[Any]], ...]]:
    named: dict[str, list[type[RestInterface[Any]]]] = {}
    for interface in python:
        named.setdefault(interface.__name__, []).append(interface)
    for key, interface in zip(yaml_keys, config, strict=True):
        named.setdefault(key, []).append(interface)
    return {name: tuple(found) for name, found in named.items()}


def served_routes(
    project: Project,
    select_interfaces: InterfaceSelector | None = None,
) -> tuple[DiscoveryResult, RouteSources]:
    """Validate REST config, discover routes, and narrow to *select_interfaces*.

    Reads no authentication, so it resolves no secret.

    Returns:
        What the kernel composes, and the routes the transport mounts.

    Raises:
        RestInterfaceConfigError: When a YAML interface is malformed.
        RuntimeError: When discovery finds nothing to serve, or a CRUD
            route cannot be served by the backend.
    """
    interfaces, disabled = _route_config(project.config)
    config_interfaces = build_interfaces_from_config(interfaces)
    discovered = _discover_components(project)
    python_interfaces = interfaces_in(discovered)
    unserved: tuple[type[RestInterface[Any]], ...] = ()
    if select_interfaces is not None:
        named = _interfaces_by_name(python_interfaces, interfaces, config_interfaces)
        served = frozenset(select_interfaces(named))
        unserved = tuple(i for i in python_interfaces if i not in served)
        python_interfaces = tuple(i for i in python_interfaces if i in served)
        config_interfaces = tuple(i for i in config_interfaces if i in served)
    discovered = _fold_config_interfaces(discovered, config_interfaces)
    served_interfaces = python_interfaces + config_interfaces
    _reject_autocrud_without_model(discovered, served_interfaces)
    discovered = _gate_autocrud_capabilities(discovered, served_interfaces, project)
    routes = RouteSources(
        python=python_interfaces,
        config=config_interfaces,
        disabled=tuple((d.method, d.path) for d in disabled),
        unserved=unserved,
    )
    return discovered, routes


@dataclasses.dataclass(frozen=True)
class RestDeclaration:
    """What :func:`compose_rest` resolved and an HTTP adapter's ``mount`` mounts."""

    name: str
    rest: _RestConfig
    auth: _AuthWiring
    metrics_cfg: PrometheusObservabilityConfig
    routes: RouteSources


def compose_rest(
    project: Project,
    *,
    authenticator: Authenticator | None = None,
    metrics_registry: CollectorRegistry | None = None,
    serves_documentation: bool = True,
    auth_section: str | None = None,
    select_interfaces: InterfaceSelector | None = None,
) -> tuple[Recipe, RestDeclaration]:
    """Validate ``app.rest``, discover, add the YAML routes and prune the CRUD ones.

    The one composition of every HTTP adapter, so the configuration, the
    authentication, discovery and the CRUD gating have one source of truth.

    Args:
        project: What the entry point composes from.
        authenticator: Custom authentication mechanism; requires
            ``app.rest.auth.custom: true``.
        metrics_registry: Prometheus registry of the metrics; the global one
            when ``None``.
        serves_documentation: Whether the transport serves the docs, schema,
            metrics and health paths. The default authentication exclusions
            are those paths, so they are none when it does not; an explicit
            ``exclude_paths`` is honoured as written either way.
        auth_section: Dot-path of a section with the ``app.rest.auth``
            schema that replaces it.
        select_interfaces: Narrows the served interfaces; every one when ``None``.

    Raises:
        ConfigError: When ``app.rest``, the authentication or an SQL
            endpoint is misdeclared; before any discovery.
        RestInterfaceConfigError: When a YAML interface or disabled route
            is malformed.
        RuntimeError: When discovery finds nothing to serve, or a CRUD
            route cannot be served by the backend.
    """
    ctx = project.config
    rest_cfg = _section_rest_config(ctx)
    metrics_cfg = project.observability_config.prometheus
    auth_cfg = rest_cfg.auth if auth_section is None else ctx.section(auth_section, _RestAuthConfig)
    auth = _resolve_authentication(
        auth_cfg,
        authenticator,
        _documentation_paths(rest_cfg, metrics_cfg) if serves_documentation else (),
    )
    _require_deployed_jwt_claims(ctx, project.environment, auth.jwt_config)
    _check_sql_section(ctx, auth)
    discovered, routes = served_routes(project, select_interfaces)
    declared = RestDeclaration(
        name=project.app.name, rest=rest_cfg, auth=auth, metrics_cfg=metrics_cfg, routes=routes
    )
    recipe = Recipe(
        profile=SERVER_PROFILE,
        discovered=discovered,
        default_store_backend=SQLALCHEMY_BACKEND,
        check_scope=CheckScope(required=frozenset({ConfigKey.APP_REST})),
        agents=True,
        metrics=_build_metrics_adapter(metrics_cfg, metrics_registry),
        pool_metrics=_build_pool_metrics(metrics_cfg, metrics_registry, project.app.name),
    )
    return recipe, declared


class FastAPIRuntime:
    """Serve a composed application over HTTP with FastAPI.

    One instance composes one application: :meth:`recipe` keeps the
    authentication and routes it resolves for :meth:`mount`.

    Args:
        metrics_registry: Prometheus registry for the middleware and the
            scrape endpoint; the global one when ``None``.
        authenticator: Custom authentication mechanism; requires
            ``app.rest.auth.custom: true``.
    """

    def __init__(
        self,
        *,
        metrics_registry: CollectorRegistry | None = None,
        authenticator: Authenticator | None = None,
    ) -> None:
        self._metrics_registry = metrics_registry
        self._authenticator = authenticator
        self._declared: RestDeclaration | None = None

    def recipe(self, project: Project) -> Recipe:
        """Validate ``app.rest``, discover, add the YAML routes and prune the CRUD ones.

        Raises:
            ConfigError: When ``app.rest``, the authentication or an SQL
                endpoint is misdeclared; before any discovery.
            RestInterfaceConfigError: When a YAML interface or disabled route
                is malformed.
            RuntimeError: When discovery finds nothing to serve, or a CRUD
                route cannot be served by the backend.
            CompositionStateError: When this instance already composed an
                application.
        """
        if self._declared is not None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_ALREADY_COMPOSED.issue(component="FastAPIRuntime")
            )
        recipe, self._declared = compose_rest(
            project,
            authenticator=self._authenticator,
            metrics_registry=self._metrics_registry,
        )
        return recipe

    def mount(self, application: Application) -> FastAPI:
        """Mount routes, authentication, health, SQL and agent endpoints on *application*.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this instance.
        """
        if self._declared is None:
            raise CompositionStateError(
                CompositionErrorCode.RUNTIME_RECIPE_MISSING.issue(component="FastAPIRuntime")
            )
        declared = self._declared
        rest_cfg, auth = declared.rest, declared.auth

        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncIterator[None]:
            async with application.running():
                yield

        app = create_fastapi_app(
            application.kernel,
            declared.routes,
            observability=application.observability,
            title=rest_cfg.title,
            version=rest_cfg.version,
            docs_url=rest_cfg.docs_url,
            redoc_url=rest_cfg.redoc_url,
            openapi_url=rest_cfg.openapi_url,
            lifespan=lifespan,
        )
        mount_health(app, application.readiness_probes())
        mount_request_stack(app, declared, mount_metrics=self._metrics_mounter(app, declared))
        sql = application.sql
        if sql.config is not None:
            mount_sql_endpoints(
                app,
                service=sql.service,
                config=sql.config,
                authenticator=auth.authenticator,
                observability=application.observability,
            )
            _warn_sql_endpoints(sql.config, auth)
        _bind_agent_surfaces(app, application.ai, auth, application.observability)
        setattr(
            app.state,
            INTROSPECTION_STATE_ATTR,
            _build_introspection(declared.name, rest_cfg, application.ai),
        )
        reject_health_collision(app)
        verify_exclusion_paths(app, auth.exclude_paths)
        _warn_anonymous_schema(rest_cfg, auth)
        return app

    def _metrics_mounter(
        self, app: FastAPI, declared: RestDeclaration
    ) -> Callable[[], None] | None:
        """Mount the metrics middleware and scrape endpoint, when metrics are enabled."""
        if not declared.metrics_cfg.enabled:
            return None
        return functools.partial(_mount_metrics, app, declared.metrics_cfg, self._metrics_registry)
