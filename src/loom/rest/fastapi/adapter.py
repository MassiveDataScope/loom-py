"""The FastAPI adapter: how an HTTP entry point composes and serves an application."""

from __future__ import annotations

import dataclasses
import logging
import warnings
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import msgspec
import prometheus_client
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

from loom.composition import AppConfig, Application, CompositionSource, Recipe, ResourceKind
from loom.composition._ai import (
    AiWiring,
)
from loom.core.config import (
    ConfigContext,
    ConfigKey,
)
from loom.core.config.errors import ConfigError
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
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.persistence import (
    SERVER_PROFILE,
    PersistenceBackend,
    PoolMetricsSink,
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

if TYPE_CHECKING:
    # Annotations only: the AI pillar and the ClickHouse extra are imported at
    # run time solely by the branch that needs them.
    pass


# Written as data, never imported here: an application without an 'ai:'
# section must not gain a single AI import (FR-050).  The reference resolves
# inside 'describe_app', and only when a description is asked for.
_AGENTS_SECTION = "agents"
_AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"


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


class _AppConfig(AppConfig, kw_only=True):
    """``app`` as the REST entry point reads it: the shared keys plus ``rest``."""

    rest: _RestConfig = msgspec.field(default_factory=_RestConfig)


def _load_observability_config(ctx: ConfigContext) -> ObservabilityConfig:
    """Load the top-level observability config; defaults only when it is absent.

    A section that fails validation stops startup: falling back to defaults
    would silently drop every setting it holds.
    """
    return ctx.section_or_default(
        ConfigKey.OBSERVABILITY, ObservabilityConfig, ObservabilityConfig()
    )


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
    discovered: DiscoveryResult, backend: PersistenceBackend, backend_name: str
) -> DiscoveryResult:
    """Mount only the CRUD operations the serving repository class declares.

    The repository is the explicit ``repository_for`` class when there is one,
    else ``backend.repository_type``. An empty ``include`` is recomputed from
    scratch and narrowed to the declared capabilities, so a later boot on
    another backend in the same process sees its own routes; an explicit
    ``include`` is honoured verbatim or refused.

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
            pruned.update(
                _gate_interface(interface, interface.auto_crud_model, backend, backend_name)
            )
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
    backend: PersistenceBackend,
    backend_name: str,
) -> frozenset[type[Any]]:
    """Narrow one interface's generated routes; return the use cases pruned from it."""
    repository_type = _serving_repository_type(model, backend)
    if repository_type is None:
        raise RuntimeError(
            f"{interface.__name__} generates CRUD routes over {model.__name__}, but "
            f"persistence.backend {backend_name!r} serves no repositories."
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


def _build_introspection(app_cfg: _AppConfig, ai: AiWiring) -> AppIntrospection:
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


@dataclasses.dataclass(frozen=True)
class _Declared:
    """What :meth:`FastAPIAdapter.recipe` resolved and :meth:`FastAPIAdapter.bind` mounts."""

    app_cfg: _AppConfig
    auth: _AuthWiring
    metrics_cfg: PrometheusObservabilityConfig
    routes: RouteSources


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
        self._declared: _Declared | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """Validate ``app.rest``, discover, add the YAML routes and prune the CRUD ones.

        Raises:
            ConfigError: When ``app.rest``, the authentication or an SQL
                endpoint is misdeclared; before any discovery.
            RestInterfaceConfigError: When a YAML interface or disabled route
                is malformed.
            RuntimeError: When discovery finds nothing to serve, a CRUD
                route cannot be served by the backend, or this instance
                already composed an application.
        """
        if self._declared is not None:
            raise RuntimeError(
                "one adapter instance composes one application; create another adapter"
            )
        ctx = source.config
        app_cfg = _section_app_config(ctx)
        metrics_cfg = _load_observability_config(ctx).prometheus
        auth = _resolve_authentication(
            app_cfg,
            self._authenticator,
            _documentation_paths(app_cfg.rest, metrics_cfg),
        )
        _check_sql_section(ctx, auth)

        # Built ahead of discovery's pruning so their use cases join CRUD
        # gating, model registration and compilation exactly like a
        # Python-discovered interface's, in Python-then-config order.
        config_interfaces = build_interfaces_from_config(app_cfg.rest.interfaces)
        discovered = _discover_components(source)
        python_interfaces = discovered.interfaces
        discovered = _fold_config_interfaces(discovered, config_interfaces)
        # Runs before the backend allocates resources: building it creates them.
        _reject_autocrud_without_model(discovered)
        backend = source.backend(default="sqlalchemy")
        discovered = _gate_autocrud_capabilities(discovered, backend, backend.name)

        self._declared = _Declared(
            app_cfg=app_cfg,
            auth=auth,
            metrics_cfg=metrics_cfg,
            routes=RouteSources(
                python=python_interfaces,
                config=config_interfaces,
                disabled=tuple((d.method, d.path) for d in app_cfg.rest.disable_routes),
            ),
        )
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=discovered,
            backend=backend,
            agents=True,
            metrics=_build_metrics_adapter(metrics_cfg, self._metrics_registry),
            pool_metrics=_build_pool_metrics(metrics_cfg, self._metrics_registry, app_cfg.name),
        )

    def bind(self, application: Application) -> FastAPI:
        """Mount routes, authentication, health, SQL and agent endpoints on *application*.

        Raises:
            RuntimeError: When :meth:`recipe` has not run on this instance.
        """
        if self._declared is None:
            raise RuntimeError(
                "FastAPIAdapter.bind needs the recipe it declared; call recipe first"
            )
        declared = self._declared
        app_cfg, auth = declared.app_cfg, declared.auth

        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncIterator[None]:
            async with application.running():
                yield

        app = create_fastapi_app(
            application.kernel,
            declared.routes,
            observability_runtime=application.observability,
            title=app_cfg.rest.title,
            version=app_cfg.rest.version,
            docs_url=app_cfg.rest.docs_url,
            redoc_url=app_cfg.rest.redoc_url,
            openapi_url=app_cfg.rest.openapi_url,
            lifespan=lifespan,
        )
        store = next(r for r in application.resources if r.kind is ResourceKind.STORE)
        mount_health(app, store.name, store.readiness)
        _mount_authentication(app, auth)
        _mount_optional_middlewares(app, app_cfg.rest, declared.metrics_cfg, self._metrics_registry)
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
        setattr(app.state, INTROSPECTION_STATE_ATTR, _build_introspection(app_cfg, application.ai))
        # Last: every route the application will ever serve is registered by now,
        # which is what makes the exclusion and collision checks meaningful.
        reject_health_collision(app)
        verify_exclusion_paths(app, auth.exclude_paths)
        _warn_anonymous_schema(app_cfg.rest, auth)
        return app
