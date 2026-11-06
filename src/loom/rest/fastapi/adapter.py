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
    CompositionSource,
    CompositionStateError,
    Recipe,
)
from loom.core.config import (
    ConfigContext,
)
from loom.core.config.errors import ConfigError
from loom.core.config.loader import convert_section
from loom.core.discovery._utils import (
    collect_use_cases_from_interfaces,
    infer_model_from_use_case,
)
from loom.core.discovery.base import AGENTS_ONLY_HINT, DiscoveryResult
from loom.core.introspection import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    ContributorRef,
)
from loom.core.observability.config import PrometheusObservabilityConfig
from loom.core.persistence import (
    SERVER_PROFILE,
    PersistenceBackend,
    PoolMetricsSink,
)
from loom.core.persistence.stores import SQLALCHEMY_BACKEND, StoreKey, store_of
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
from loom.rest.fastapi._auto_ai import _bind_agent_surface
from loom.rest.fastapi._auto_auth import _AuthWiring, _mount_authentication, _resolve_authentication
from loom.rest.fastapi._auto_sql import _check_sql_section, _warn_sql_endpoints
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.health import HEALTH_PATH, mount_health, reject_health_collision
from loom.rest.fastapi.sql import (
    bind_sql_endpoints,
)
from loom.rest.middleware import TraceIdMiddleware
from loom.rest.model import RestInterface

_logger = logging.getLogger(__name__)


# Written as data, never imported here: an application without an 'ai:'
# section must not gain a single AI import.  The reference resolves
# inside 'describe_app', and only when a description is asked for.
_AGENTS_SECTION = "agents"
_AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"


class _RestAuthConfig(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    jwt: JwtAuthConfig | None = None
    # True when the composition root passes create_app(authenticator=...):
    # the configuration then says authentication is supplied in code.
    custom: bool = False
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS


_REST_SECTION = "app.rest"


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
    # Kept as written: _route_config validates the raw entries first, so a
    # typo is reported by interface name, which msgspec cannot recover once a
    # typed decode has failed; served_routes is their one decoder.
    interfaces: dict[str, Any] = msgspec.field(default_factory=dict)
    # Subtractive per-environment overlay: every entry must match a route
    # declared in Python, or startup aborts (see
    # RestInterfaceCompiler.compile_sources).
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
    return discovered.with_changes(
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
    discovered: DiscoveryResult, source: CompositionSource
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
    for interface in discovered.interfaces:
        if interface.auto_crud_model is not None:
            model = interface.auto_crud_model
            key = store_of(model)
            backend = source.backend(key, default=SQLALCHEMY_BACKEND)
            pruned.update(_gate_interface(interface, model, key, backend))
    mounted = {route.use_case for iface in discovered.interfaces for route in iface.routes}
    dropped = pruned - mounted
    return discovered.with_changes(
        use_cases=tuple(uc for uc in discovered.use_cases if uc not in dropped),
    )


def _gate_interface(
    interface: type[RestInterface[Any]],
    model: type[Any],
    key: StoreKey,
    backend: PersistenceBackend,
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


def _serving_repository_type(model: type[Any], backend: PersistenceBackend) -> type[Any] | None:
    """Return the repository class that will serve *model* on *backend*."""
    registered = registered_repository_type(model)
    if registered is not None:
        return registered
    try:
        return backend.repository_type
    except AttributeError as exc:
        raise ConfigError(
            f"persistence backend {type(backend).__name__} does not declare repository_type"
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


def _discover_components(source: CompositionSource) -> DiscoveryResult:
    """Run discovery and reject a result with nothing to serve.

    Use cases, REST interfaces and manifest agents are each optional on their
    own: an application whose only content is agents has neither use cases nor
    interfaces. Only a result with none of the three is an error.
    """
    discovered = source.discover()
    if not discovered.use_cases and not discovered.interfaces and not discovered.agent_specs:
        raise RuntimeError(
            f"Nothing discovered: no UseCase, no RestInterface and no agents. {AGENTS_ONLY_HINT}"
        )
    return discovered


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
        # Return 404 for trailing-slash variant to avoid ambiguous scrape targets.
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
    raw_interfaces = convert_section(
        ctx.raw("app.rest.interfaces", {}), "app.rest.interfaces", dict[str, Any]
    )
    validate_interfaces_config(raw_interfaces)
    raw_disable_routes = convert_section(
        ctx.raw("app.rest.disable_routes", ()), "app.rest.disable_routes", tuple[Any, ...]
    )
    validate_disable_routes_config(raw_disable_routes)
    return (
        convert_section(raw_interfaces, "app.rest.interfaces", dict[str, RestInterfaceConfig]),
        convert_section(
            raw_disable_routes, "app.rest.disable_routes", tuple[DisableRouteConfig, ...]
        ),
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
    source: CompositionSource,
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
    # Built ahead of discovery's pruning so their use cases join CRUD
    # gating, model registration and compilation exactly like a
    # Python-discovered interface's, in Python-then-config order.
    interfaces, disabled = _route_config(source.config)
    config_interfaces = build_interfaces_from_config(interfaces)
    discovered = _discover_components(source)
    python_interfaces = discovered.interfaces
    unserved: tuple[type[RestInterface[Any]], ...] = ()
    if select_interfaces is not None:
        named = _interfaces_by_name(python_interfaces, interfaces, config_interfaces)
        served = frozenset(select_interfaces(named))
        unserved = tuple(i for i in python_interfaces if i not in served)
        python_interfaces = tuple(i for i in python_interfaces if i in served)
        config_interfaces = tuple(i for i in config_interfaces if i in served)
        discovered = discovered.with_changes(interfaces=python_interfaces)
    discovered = _fold_config_interfaces(discovered, config_interfaces)
    # Runs before the backend allocates resources: building it creates them.
    _reject_autocrud_without_model(discovered)
    discovered = _gate_autocrud_capabilities(discovered, source)
    routes = RouteSources(
        python=python_interfaces,
        config=config_interfaces,
        disabled=tuple((d.method, d.path) for d in disabled),
        unserved=unserved,
    )
    return discovered, routes


@dataclasses.dataclass(frozen=True)
class RestDeclaration:
    """What :func:`compose_rest` resolved and an HTTP adapter's ``bind`` mounts."""

    name: str
    rest: _RestConfig
    auth: _AuthWiring
    metrics_cfg: PrometheusObservabilityConfig
    routes: RouteSources


def compose_rest(
    source: CompositionSource,
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
        source: What the entry point composes from.
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
    ctx = source.config
    rest_cfg = _section_rest_config(ctx)
    metrics_cfg = source.observability_config.prometheus
    auth_cfg = rest_cfg.auth if auth_section is None else ctx.section(auth_section, _RestAuthConfig)
    auth = _resolve_authentication(
        auth_cfg,
        authenticator,
        _documentation_paths(rest_cfg, metrics_cfg) if serves_documentation else (),
    )
    _check_sql_section(ctx, auth)
    discovered, routes = served_routes(source, select_interfaces)
    declared = RestDeclaration(
        name=source.app.name, rest=rest_cfg, auth=auth, metrics_cfg=metrics_cfg, routes=routes
    )
    recipe = Recipe(
        profile=SERVER_PROFILE,
        discovered=discovered,
        default_backend=SQLALCHEMY_BACKEND,
        agents=True,
        metrics=_build_metrics_adapter(metrics_cfg, metrics_registry),
        pool_metrics=_build_pool_metrics(metrics_cfg, metrics_registry, source.app.name),
    )
    return recipe, declared


class FastAPIAdapter:
    """Serve a composed application over HTTP with FastAPI.

    One instance composes one application: :meth:`recipe` keeps the
    authentication and routes it resolves for :meth:`bind`.

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

    def recipe(self, source: CompositionSource) -> Recipe:
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
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component="FastAPIAdapter")
            )
        recipe, self._declared = compose_rest(
            source,
            authenticator=self._authenticator,
            metrics_registry=self._metrics_registry,
        )
        return recipe

    def bind(self, application: Application) -> FastAPI:
        """Mount routes, authentication, health, SQL and agent endpoints on *application*.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this instance.
        """
        if self._declared is None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component="FastAPIAdapter")
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
            observability_runtime=application.observability,
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
            bind_sql_endpoints(
                app,
                service=sql.service,
                config=sql.config,
                authenticator=auth.authenticator,
                observability_runtime=application.observability,
            )
            _warn_sql_endpoints(sql.config, auth)
        _bind_agent_surface(app, application.ai, auth, application.observability)
        setattr(
            app.state,
            INTROSPECTION_STATE_ATTR,
            _build_introspection(declared.name, rest_cfg, application.ai),
        )
        # Last: every route the application will ever serve is registered by now,
        # which is what makes the exclusion and collision checks meaningful.
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
