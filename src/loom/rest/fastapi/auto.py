"""Automatic FastAPI app creation from YAML configuration."""

from __future__ import annotations

import dataclasses
import logging
import sys
import warnings
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import msgspec
import prometheus_client
from fastapi import FastAPI
from prometheus_client import CollectorRegistry
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response

from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.cache.wiring import CacheGateways, cache_module_for
from loom.core.config import (
    ConfigContext,
    ConfigKey,
    ConfigResolver,
    with_default_resolvers,
)
from loom.core.config.errors import ConfigError
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
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.identity import Identity
from loom.core.introspection import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    ContributorRef,
    IntrospectionError,
    describe_app,
)
from loom.core.job.service import InlineJobService, JobService
from loom.core.observability.config import ObservabilityConfig, PrometheusObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence import PersistenceWiring, resolve_backend
from loom.core.sql import (
    CallerBoundSql,
    NullSqlQueryService,
    SqlConfig,
    SqlExecutor,
    SqlQueryService,
)
from loom.core.sql.config import roles_need_identity_binding
from loom.core.use_case.agent_markers import declaring_agent_bindings
from loom.core.use_case.constants import CrudOp
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.mcp_markers import DeclaringMcpBindings, declaring_mcp_bindings
from loom.core.use_case.registry import UseCaseRegistry
from loom.prometheus import PrometheusMetricsAdapter
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest._body import DEFAULT_MAX_BODY_BYTES, BodySizeLimitMiddleware
from loom.rest.auth import (
    AuthenticationMiddleware,
    Authenticator,
    JwtAuthConfig,
    JwtAuthenticator,
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
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.fastapi.health import HEALTH_PATH, mount_health, reject_health_collision
from loom.rest.fastapi.sql import (
    _connection_mechanism,
    _role_exposure_notice,
    _roles_mechanism,
    bind_sql_endpoints,
)
from loom.rest.middleware import TraceIdMiddleware
from loom.rest.model import RestInterface

_logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # Annotations only: the AI pillar and the ClickHouse extra are imported at
    # run time solely by the branch that needs them.
    from loom.ai.compiler import AgentPlan
    from loom.ai.config import AiConfig, McpServerConfig
    from loom.ai.errors import AgentCompilationIssue
    from loom.ai.runtime import AgentRuntime, UseCaseMcpGrant
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry


# Written as data, never imported here: an application without an 'ai:'
# section must not gain a single AI import (FR-050).  The reference resolves
# inside 'describe_app', and only when a description is asked for.
_AGENTS_SECTION = "agents"
# Single source of the agents prefix for both agent surfaces. Passed
# explicitly to 'bind_agent_endpoints' and to 'bind_a2a_endpoints' so the
# FR-041b exclusion guard measures the prefix the application really mounts.
_AGENTS_PREFIX: Final[str] = "/agents"
_AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"


class _DiscoveryInterfaces(msgspec.Struct, kw_only=True):
    modules: list[str] = msgspec.field(default_factory=list)
    warn_recommended: bool = True


class _DiscoveryModules(msgspec.Struct, kw_only=True):
    include: list[str] = msgspec.field(default_factory=list)


class _DiscoveryManifest(msgspec.Struct, kw_only=True):
    module: str = ""


class _DiscoveryConfig(msgspec.Struct, kw_only=True):
    mode: str = "interfaces"
    interfaces: _DiscoveryInterfaces = msgspec.field(default_factory=_DiscoveryInterfaces)
    modules: _DiscoveryModules = msgspec.field(default_factory=_DiscoveryModules)
    manifest: _DiscoveryManifest = msgspec.field(default_factory=_DiscoveryManifest)


class _RestAuthConfig(msgspec.Struct, kw_only=True):
    jwt: JwtAuthConfig | None = None
    exclude_paths: tuple[str, ...] = DEFAULT_EXCLUDE_PATHS


class _RestConfig(msgspec.Struct, kw_only=True):
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
    code_path: str = "src"
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


def _ensure_code_path(code_path: Path) -> None:
    path_str = str(code_path.resolve())
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


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
    ``celery`` config section is missing / malformed, so the caller can
    fall back to :func:`_build_inline_service`.

    Args:
        ctx: Resolved configuration context.
        result: Kernel runtime carrying container, factory and executor.

    Returns:
        ``CeleryJobService`` instance, or ``None``.
    """
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

        celery_cfg = ctx.section(ConfigKey.CELERY, _CC)
        return CeleryJobService(
            create_celery_app(celery_cfg),
            metrics=result.metrics,
            factory=result.factory,
            executor=result.executor,
            observability_runtime=observability_runtime,
        )
    except ImportError:
        return None
    except ConfigError:
        warnings.warn(
            "Celery config section is missing or malformed — falling back to InlineJobService. "
            "Add a 'celery' section to your config or install loom[celery] to suppress this.",
            stacklevel=3,
        )
        return None


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
    """Load top-level observability config or fall back to defaults."""
    try:
        return ctx.section(ConfigKey.OBSERVABILITY, ObservabilityConfig)
    except ConfigError:
        return ObservabilityConfig()


def _build_bootstrap(
    app_cfg: _AppConfig,
    ctx: ConfigContext,
    metrics: Any | None = None,
    config_interfaces: tuple[type[RestInterface[Any]], ...] = (),
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
    wiring = _resolve_persistence(ctx, persistence_cfg, discovered)
    discovered = _gate_autocrud_capabilities(discovered, wiring, persistence_cfg.backend)
    wiring.prepare_models(discovered.models)
    result = _build_kernel_runtime(
        app_cfg,
        discovered,
        wiring,
        metrics=metrics,
        extra_modules=(cache_module_for(ctx),),
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
    # ``replace`` copies fields added later; the cast Sonar asks for fails mypy.
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
) -> PersistenceWiring:
    """Build the wiring of the backend registered under ``persistence.backend``.

    Raises:
        ConfigError: When no backend is registered under that name.
    """
    return resolve_backend(persistence_cfg.backend).build(ctx, discovered.models)


@dataclass(frozen=True)
class _SqlWiring:
    """Resolved SQL subsystem pieces for the auto-bootstrap REST app.

    Args:
        config: Parsed ``sql:`` section, or ``None`` when absent.
        registry: ClickHouse connection registry entered by the app lifespan,
            or ``None`` when no section is configured.
        service: Query service registered in the container — the null
            implementation when *config* is ``None`` (spec M5).
    """

    config: SqlConfig | None
    registry: ClickHouseConnectionRegistry | None
    service: SqlQueryService


class _RegistryExecutors(Mapping[str, SqlExecutor]):
    """Lazy executor view over the ClickHouse connection registry.

    Lets ``create_app`` construct the SQL service and bind endpoints without
    opening any connection: executors resolve on first access, once the app
    lifespan has entered the registry.
    """

    def __init__(self, registry: ClickHouseConnectionRegistry, names: tuple[str, ...]) -> None:
        self._registry = registry
        self._names = names

    def __getitem__(self, name: str) -> SqlExecutor:
        if name not in self._names:
            raise KeyError(name)
        return self._registry.executor(name)

    def __contains__(self, name: object) -> bool:
        # Membership from config alone: never touches the registry, so the
        # service policy checks work outside the lifespan as well.
        return name in self._names

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


@dataclass(frozen=True)
class _AgentDeps:
    """Per-invocation dependency bundle handed to an agent capability call.

    Args:
        identity: Verified caller of the invocation.
        container: Application container holding the singleton services.
        invoker: Application invoker already bound to ``identity``.
        state: The invocation's state, a mapping normalised against the
            artefact's declared shape, or ``None`` for an artefact that
            declares no state or a call that supplies none.
    """

    identity: Identity
    container: LoomContainer
    invoker: AppInvoker
    state: Mapping[str, Any] | None = None


class _AgentDepsFactory:
    """Builds the per-invocation dependencies of every agent capability call.

    Structural implementation of ``loom.ai.abc.DepsFactory``: it lives in the
    composition root, and depends only on ``loom.core``, so an application with
    no ``ai:`` section never imports the AI pillar to obtain it (FR-050).

    Binding the invoker to the caller happens here, at the one point that knows
    both: a granted use case declaring ``Caller()`` is executed as the identity
    that invoked the agent, and the capability never has to discover the caller
    from ambient state (FR-043).

    Args:
        invoker: Unbound application invoker of this application.
    """

    def __init__(self, invoker: AppInvoker) -> None:
        self._invoker = invoker

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the dependency bundle for one invocation.

        Args:
            identity: Verified caller of this invocation.
            container: Application container holding the singleton services.
            state: The invocation's state, a mapping normalised against the
                artefact's declared shape, or ``None`` for an artefact that
                declares no state or a call that supplies none.

        Returns:
            The bundle the engine passes to its capability calls.
        """
        return _AgentDeps(
            identity=identity,
            container=container,
            invoker=self._invoker.for_identity(identity),
            state=state,
        )


@dataclass(frozen=True)
class _AiWiring:
    """Resolved AI pillar pieces for the auto-bootstrap REST app.

    Args:
        config: Parsed ``ai:`` section, or ``None`` when absent.
        runtime: Agent runtime entered by the app lifespan alongside the SQL
            registry, or ``None`` when no section is configured.
        plans: Compiled agent plans, kept so the application can describe
            itself without reaching into the runtime.
    """

    config: AiConfig | None
    runtime: AgentRuntime | None
    plans: tuple[AgentPlan, ...] = ()


def _effective_agent_specs(
    config_specs: tuple[str, ...],
    manifest_specs: tuple[str, ...],
    *,
    has_declaring_use_case: bool,
) -> tuple[str, ...]:
    """Return the single artifact source of the application.

    ``ai.specs`` and the manifest ``AGENTS`` attribute are mutually exclusive:
    an implicit precedence would silently ignore half of the agents an
    operator declared.

    Args:
        config_specs: Glob patterns from ``ai.specs``.
        manifest_specs: Glob patterns from the manifest ``AGENTS`` attribute.
        has_declaring_use_case: Whether at least one compiled use case
            declares an ``Mcp()`` marker (owner decision D1). When ``True``,
            an empty artifact set is tolerated instead of aborting: a
            deployment whose only AI usage is ``Mcp()`` no longer has to ship
            a filler agent artifact it never runs. It still declares
            ``ai.engine`` and installs the extra — ``_resolve_ai`` resolves
            the provider unconditionally, and that is where the MCP client
            factory comes from — this only sheds the filler *artifact*.

    Raises:
        AgentCompilationError: When both sources declare artifacts, or when
            neither does and no use case declares ``Mcp()``.
    """
    # Local import: same containment rule as '_resolve_ai'; this helper only
    # ever runs from the branch that already decided to load the AI pillar.
    from loom.ai.errors import AgentCompilationError, agent_specs_conflict, agent_specs_missing

    if config_specs and manifest_specs:
        raise AgentCompilationError([agent_specs_conflict()])
    if not config_specs and not manifest_specs and not has_declaring_use_case:
        raise AgentCompilationError([agent_specs_missing()])
    return config_specs or manifest_specs


def _resolve_ai(
    ctx: ConfigContext,
    *,
    kernel: KernelRuntime,
    sql_cfg: SqlConfig | None,
    code_path: Path,
    manifest_agent_specs: tuple[str, ...],
    use_case_mcp_bindings: DeclaringMcpBindings,
) -> _AiWiring:
    """Load the optional ``ai:`` section into its compiled agent runtime.

    Absent section -> empty wiring, and the AI pillar is never imported.
    Present section -> the configured engine is resolved, every declared
    artifact is compiled offline, and the runtime is built without opening a
    single connection: its live clients open inside the app lifespan.

    Args:
        use_case_mcp_bindings: Use cases declaring an ``Mcp()`` marker, from
            ``_verify_mcp_markers`` — already verified against
            ``ai.mcp_servers`` by the caller, before this function ever runs.
    """
    if not ctx.has(ConfigKey.AI):
        return _AiWiring(config=None, runtime=None)
    # Local imports: the AI pillar is optional, and importing it from an
    # application without an 'ai:' section would pull the whole agent layer
    # into every Loom app (FR-050) — the same rule '_build_sql_registry'
    # follows for the ClickHouse extra.
    from loom.ai.compiler import AgentCompiler
    from loom.ai.config import AiConfig
    from loom.ai.declarative import load_specs
    from loom.ai.registry import (
        configure_engine_mcp_connect_timeout,
        engine_client_factories,
        engine_native_tool_support,
        engine_supported_kinds,
        resolve_engine_provider,
    )
    from loom.ai.runtime import AgentRuntime

    ai_cfg = ctx.section(ConfigKey.AI, AiConfig)
    provider = resolve_engine_provider(ai_cfg.engine)
    # `ai.startup_timeout_ms` is the published start-up connect budget; handed
    # to the engine here so its own MCP handshake deadline stops being the
    # engine's undocumented five seconds (FR-051).
    configure_engine_mcp_connect_timeout(provider, ai_cfg.startup_timeout_ms / 1000)
    mcp_factory, a2a_factory = engine_client_factories(provider)
    native_tools = engine_native_tool_support(provider)
    compiler = AgentCompiler(
        config=ai_cfg,
        registry=kernel.registry,
        supported_kinds=engine_supported_kinds(provider, ai_cfg.engine),
        sql=sql_cfg,
        native_tools=native_tools,
    )
    specs = _effective_agent_specs(
        ai_cfg.specs, manifest_agent_specs, has_declaring_use_case=bool(use_case_mcp_bindings)
    )
    decoded = load_specs(specs, root=code_path)
    # DecodedSpec, not .spec: the artifact's own path is what resolves a
    # './library' skill grant, and dropping it fails them in real wiring.
    plans = compiler.compile_all(decoded)
    use_case_mcp = _compile_use_case_mcp(use_case_mcp_bindings, kernel.registry, ai_cfg.mcp_servers)
    runtime = AgentRuntime(
        plans=plans,
        config=ai_cfg,
        engine_provider=provider,
        deps=_AgentDepsFactory(kernel.app),
        container=kernel.container,
        sql_config=sql_cfg,
        # Without these an artifact granting 'mcp' or 'a2a' compiles and then
        # fails to start: the runtime has no way to reach the server it must
        # validate the grant against.
        mcp_client_factory=mcp_factory,
        a2a_client_factory=a2a_factory,
        use_case_mcp=use_case_mcp,
    )
    return _AiWiring(config=ai_cfg, runtime=runtime, plans=plans)


def _compile_use_case_mcp(
    declaring: DeclaringMcpBindings,
    registry: UseCaseRegistry,
    servers: Mapping[str, McpServerConfig],
) -> tuple[UseCaseMcpGrant, ...]:
    """Compile every verified ``Mcp()`` binding into the runtime's own grants.

    ``_verify_mcp_markers`` already checked every binding's server name
    against this same ``servers`` mapping, so the shared compile helper's own
    ``mcp_server_unknown`` is unreachable from this call by construction. The
    caller still raises on one anyway rather than dropping it silently,
    fail-closed should that ordering ever break — one line, no new
    vocabulary.

    Args:
        declaring: Use cases declaring at least one ``Mcp()`` binding, from
            ``_verify_mcp_markers``.
        registry: Resolves each use case's registered key.
        servers: Every MCP server configured for this deployment.

    Returns:
        One ``UseCaseMcpGrant`` per declared binding, in declaration order.

    Raises:
        AgentCompilationError: Only if ``_verify_mcp_markers``'s ordering were
            ever broken and an unknown server reached this call unverified.
    """
    # Local imports: same containment rule as '_resolve_ai' — this helper
    # only ever runs from the branch that already decided to load the AI
    # pillar.
    from loom.ai.compiler.phases import compile_mcp_capability
    from loom.ai.errors import AgentCompilationError
    from loom.ai.runtime import UseCaseMcpGrant

    grants: list[UseCaseMcpGrant] = []
    issues: list[AgentCompilationIssue] = []
    for uc_type, bindings in declaring:
        usecase = registry.key_for(uc_type) or uc_type.__qualname__
        for binding in bindings:
            capability, binding_issues = compile_mcp_capability(
                binding.server,
                include=binding.include,
                exclude=(),
                servers=servers,
                component=usecase,
            )
            issues.extend(binding_issues)
            if capability is not None:
                grants.append(
                    UseCaseMcpGrant(capability=capability, usecase=usecase, parameter=binding.name)
                )
    if issues:
        raise AgentCompilationError(issues)
    return tuple(grants)


def _verify_mcp_markers(
    use_cases: Sequence[type[Compilable]],
    compiler: UseCaseCompiler,
    registry: UseCaseRegistry,
    ctx: ConfigContext,
) -> DeclaringMcpBindings:
    """Abort start-up when an ``Mcp()`` marker names a server not configured.

    Runs immediately before the ``_resolve_ai`` call, not in the
    ``_verify_agent_markers`` slot after it: an unknown server must die here
    so the shared MCP capability compiler (``compile_mcp_capability``) can
    never be asked to report it for a use case — one condition, one message.

    The AI section is decoded a second time here, once per boot, only when at
    least one use case declares the marker — ``ConfigContext.section`` caches
    nothing. Accepted overhead, in exchange for one call site that also
    covers the no-``ai:``-section case (FR-08) instead of two call sites keyed
    on whether the section exists.

    Args:
        use_cases: Every use case compiled for this deployment.
        compiler: Compiler holding the cached plan of each of them.
        registry: Resolves a use case's registered key for the error message.
        ctx: Deployment configuration, read for ``ai.mcp_servers`` only when
            at least one use case declares the marker.

    Returns:
        Every use case declaring an ``Mcp()`` binding, paired with those
        bindings, for ``_resolve_ai`` to compile once verification passes.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown server name.
    """
    declaring = declaring_mcp_bindings(use_cases, compiler)
    if not declaring:
        # No use case declares Mcp(): importing 'loom.ai' here, only to find
        # nothing to check, would be exactly the containment leak
        # '_resolve_ai' itself avoids for the same absent-section case
        # (FR-050) — an app with no 'ai:' section must never pull the pillar
        # in just because start-up ran.
        return declaring
    # Local imports: same containment rule as '_resolve_ai'.
    from loom.ai._startup import verify_mcp_markers
    from loom.ai.config import AiConfig

    servers = ctx.section(ConfigKey.AI, AiConfig).mcp_servers if ctx.has(ConfigKey.AI) else {}
    verify_mcp_markers(declaring, registry, servers)
    return declaring


def _bind_agent_resolver(result: KernelRuntime, ai: _AiWiring) -> None:
    """Wire the executor's ``Agent()`` marker resolver, once the AI runtime exists.

    ``KernelRuntime.executor`` is built before this function can run — the AI
    pillar it depends on is optional and resolved afterwards — so binding
    happens here rather than at kernel construction. A no-op when no
    ``ai:`` section is present: a use case declaring ``Agent()`` in that
    deployment still compiles, and fails informatively at its first
    execution instead, which is what an unresolved resolver already does.

    ``SqlQueryService`` is resolved here, once, and handed to the resolver
    rather than left for a handle to reach into the container for later:
    it is always registered by this point (``_register_sql_service`` runs
    before this function, spec M5), so there is nothing optional about the
    resolution itself.
    """
    if ai.runtime is None:
        return
    # Local import: same containment rule as '_resolve_ai' — the AI pillar
    # is optional, and this branch only runs once that section is present.
    from loom.ai.runtime._handle import agent_marker_resolver

    observability = (
        result.container.resolve(ObservabilityRuntime)
        if result.container.is_registered(ObservabilityRuntime)
        else None
    )
    sql_query_service = result.container.resolve(SqlQueryService)
    resolver = agent_marker_resolver(
        ai.runtime, sql_query_service=sql_query_service, observability=observability
    )
    result.executor.bind_agent_resolver(resolver)


def _bind_mcp_resolver(result: KernelRuntime, ai: _AiWiring) -> None:
    """Wire the executor's ``Mcp()`` marker resolver, once the AI runtime exists.

    Mirrors :func:`_bind_agent_resolver`, on the executor's second resolver.
    A no-op only for a deployment where no use case declares ``Mcp()``: one
    that does never reaches this function, because ``_verify_mcp_markers``
    already aborted start-up with ``MCP_MARKER_UNKNOWN`` against an empty
    server mapping. The executor's own "no resolver bound" refusal is
    therefore unreachable through this composition root, and stays as the
    guard for a host that wires its own.
    """
    if ai.runtime is None:
        return
    # Local import: same containment rule as '_bind_agent_resolver'.
    from loom.ai.runtime._handle import mcp_marker_resolver

    observability = (
        result.container.resolve(ObservabilityRuntime)
        if result.container.is_registered(ObservabilityRuntime)
        else None
    )
    resolver = mcp_marker_resolver(ai.runtime, observability=observability)
    result.executor.bind_mcp_resolver(resolver)


def _verify_agent_markers(
    use_cases: Sequence[type[Compilable]],
    compiler: UseCaseCompiler,
    registry: UseCaseRegistry,
    ai: _AiWiring,
) -> None:
    """Abort start-up when an ``Agent()`` marker cannot be satisfied.

    Runs in the slot between '_resolve_ai' and 'result.factory.verify()':
    every compiled agent plan already exists here, whether or not an ``ai:``
    section is present — an absent section means ``ai.plans`` is simply
    empty, so every declared agent is reported unknown the same way a typo
    would be. The check itself is shared with the Celery worker bootstrap
    (T402), which calls the same :func:`~loom.ai._startup.verify_agent_markers`
    with an always-empty ``plans_by_name``, since a task worker never builds
    an AI runtime at all.

    Args:
        use_cases: Every use case compiled for this deployment.
        compiler: Compiler holding the cached plan of each of them.
        registry: Resolves a use case's registered key for the error message.
        ai: The (possibly empty) AI wiring '_resolve_ai' built.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown agent name
            and per mismatched output type, so a single run reports every
            problem at once.
    """
    declaring = declaring_agent_bindings(use_cases, compiler)
    if not declaring:
        # No use case declares Agent(): importing 'loom.ai' here, only to
        # find nothing to check, would be exactly the containment leak
        # '_resolve_ai' itself avoids for the same absent-section case
        # (FR-050) — an app with no 'ai:' section must never pull the pillar
        # in just because start-up ran.
        return

    # Local import: same containment rule as '_resolve_ai'.
    from loom.ai._startup import verify_agent_markers

    plans_by_name = {plan.name: plan for plan in ai.plans}
    verify_agent_markers(declaring, registry, plans_by_name)


def _bind_agent_surface(
    app: FastAPI,
    ai: _AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Mount the agent surfaces: HTTP for every opt-in, A2A for every exposure.

    Both mounts read the same ``_AGENTS_PREFIX``. That single source is what
    makes the FR-041b exclusion guard meaningful: it measures the agents prefix
    an exclusion could open, so a prefix nobody mounts would leave the real one
    unguarded.

    Raises:
        ConfigError: When an agent opts in without a usable authenticator, or
            when its callers are verified by a JWT without a validated ``aud``.
    """
    if ai.config is None or ai.runtime is None:
        return
    _validate_agent_endpoint_auth(ai.config, auth)
    # Local import: same containment rule as '_resolve_ai'.
    from loom.ai.fastapi.endpoints import bind_agent_endpoints

    bind_agent_endpoints(
        app,
        runtime=ai.runtime,
        config=ai.config,
        authenticator=auth.authenticator,
        observability_runtime=observability_runtime,
        prefix=_AGENTS_PREFIX,
    )
    _bind_a2a_surface(app, ai, auth, observability_runtime)


def _bind_a2a_surface(
    app: FastAPI,
    ai: _AiWiring,
    auth: _AuthWiring,
    observability_runtime: ObservabilityRuntime,
) -> None:
    """Publish the agents named in ``ai.a2a.expose`` over inbound A2A (FR-041).

    Guarded on the section rather than delegated to ``bind_a2a_endpoints``'s own
    early return: the import below pulls the ``ai-a2a`` extra, which a
    deployment without an ``ai.a2a`` section is not required to have installed.
    """
    if ai.config is None or ai.runtime is None or ai.config.a2a is None:
        return
    # Local import: the A2A transport lives behind the 'ai-a2a' extra.
    from loom.ai.a2a.server import bind_a2a_endpoints

    bind_a2a_endpoints(
        app,
        runtime=ai.runtime,
        config=ai.config,
        plans=ai.plans,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
        observability_runtime=observability_runtime,
        agents_prefix=_AGENTS_PREFIX,
    )


def _validate_agent_endpoint_auth(ai_cfg: AiConfig, auth: _AuthWiring) -> None:
    """Apply the JWT start-up gates to every agent bound to a verified caller (§4).

    An agent surface is at least as privileged as a SQL one: the verified
    caller drives every capability the agent holds, as that caller. So it must
    not boot in a configuration the SQL surface already refuses.
    """
    if auth.jwt_config is None:
        return
    for name in _agents_bound_to_a_verified_caller(ai_cfg):
        _require_jwt_audience(f"Agent {name!r}", auth.jwt_config)


def _agents_bound_to_a_verified_caller(ai_cfg: AiConfig) -> tuple[str, ...]:
    """Name every agent whose callers are authenticated, over either surface.

    An A2A-published agent counts too, and is the stricter case of the two: the
    HTTP mount is opt-in per agent, while ``ai.a2a.expose`` puts the agent on
    the public internet, where an unvalidated ``aud`` accepts a token minted
    for another service as the caller driving every capability the agent holds.
    """
    http = {
        # The double opt-in 'bind_agent_endpoints' mounts on.
        name
        for name, endpoint in ai_cfg.endpoints.items()
        if endpoint.enabled and endpoint.auth.strip() and not endpoint.allow_anonymous
    }
    a2a = {name for name in _a2a_exposed(ai_cfg) if not _a2a_allows_anonymous(ai_cfg, name)}
    return tuple(sorted(http | a2a))


def _a2a_exposed(ai_cfg: AiConfig) -> tuple[str, ...]:
    return ai_cfg.a2a.expose if ai_cfg.a2a is not None else ()


def _a2a_allows_anonymous(ai_cfg: AiConfig, name: str) -> bool:
    """Mirror ``loom.ai.a2a._binding``'s rule for an unverified A2A caller.

    Anonymity is only in force when the HTTP stanza's own double opt-in holds:
    a disabled or unnamed-``auth`` stanza grants nothing, so its
    ``allow_anonymous`` does not travel to the A2A surface either.
    """
    endpoint = ai_cfg.endpoints.get(name)
    if endpoint is None or not endpoint.enabled or not endpoint.auth.strip():
        return False
    return endpoint.allow_anonymous


@dataclass(frozen=True)
class _AuthWiring:
    """Resolved authentication for the auto-bootstrap REST app.

    Args:
        authenticator: Mechanism authenticating callers, or ``None`` when the
            application configures none.
        exclude_paths: Paths served without authentication.
        jwt_config: JWT settings when the built-in mechanism is the one in use.
            Kept so the JWT-specific startup gates stay where the JWT contract
            is known, instead of leaking into the agnostic layers.
    """

    authenticator: Authenticator | None
    exclude_paths: tuple[str, ...]
    jwt_config: JwtAuthConfig | None


def _resolve_authentication(
    app_cfg: _AppConfig,
    authenticator: Authenticator | None,
    documentation_paths: tuple[str, ...],
) -> _AuthWiring:
    """Pick the single authentication mechanism of the application.

    Args:
        app_cfg: Parsed ``app`` section.
        authenticator: Mechanism supplied by the composition root, if any.
        documentation_paths: Effective docs/schema/metrics/health paths, used
            as the default exclusion list.

    Raises:
        ConfigError: When both a custom authenticator and the built-in JWT
            section are supplied — two mechanisms would mean two sources of
            truth for the caller identity.
    """
    auth_cfg = app_cfg.rest.auth
    jwt_cfg = auth_cfg.jwt
    if authenticator is not None and jwt_cfg is not None:
        raise ConfigError(
            "create_app received an 'authenticator' but 'app.rest.auth.jwt' is also "
            "configured. Exactly one authentication mechanism may be active: drop the "
            "JWT section, or drop the authenticator argument."
        )
    if authenticator is not None:
        return _AuthWiring(
            authenticator=authenticator,
            exclude_paths=_effective_exclusions(auth_cfg.exclude_paths, documentation_paths),
            jwt_config=None,
        )
    if jwt_cfg is not None:
        return _AuthWiring(
            authenticator=JwtAuthenticator(jwt_cfg),
            exclude_paths=_effective_exclusions(jwt_cfg.exclude_paths, documentation_paths),
            jwt_config=jwt_cfg,
        )
    return _AuthWiring(authenticator=None, exclude_paths=(), jwt_config=None)


def _effective_exclusions(
    configured: tuple[str, ...],
    documentation_paths: tuple[str, ...],
) -> tuple[str, ...]:
    """Resolve which paths are served without authentication.

    Left at its default, the exclusion list follows the paths the application
    actually publishes: a hardcoded ``/docs`` excludes nothing when the operator
    moved Swagger elsewhere, while still opening a hole if a route captures it.
    An explicit list is honoured as written.
    """
    if configured != DEFAULT_EXCLUDE_PATHS:
        return tuple(dict.fromkeys(configured))
    return documentation_paths


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


def _resolve_sql(ctx: ConfigContext, auth: _AuthWiring) -> _SqlWiring:
    """Load the optional ``sql:`` section into its registry and service.

    Absent section → no registry and the null service, keeping
    ``SqlQueryService`` always resolvable with an actionable error (spec M5).
    Present section → startup auth gate (spec §4) plus a registry whose
    connections only open inside the app lifespan.
    """
    sql_cfg = ctx.section_optional(ConfigKey.SQL, SqlConfig)
    if sql_cfg is None:
        return _SqlWiring(config=None, registry=None, service=NullSqlQueryService())
    _validate_sql_endpoint_auth(sql_cfg, auth)
    registry = _build_sql_registry(sql_cfg)
    executors = _RegistryExecutors(registry, tuple(sql_cfg.connections))
    service = SqlQueryService(executors=executors, config=sql_cfg)
    return _SqlWiring(config=sql_cfg, registry=registry, service=service)


def _build_sql_registry(sql_cfg: SqlConfig) -> ClickHouseConnectionRegistry:
    """Construct the ClickHouse registry — no connection is opened here."""
    # Local import: clickhouse-connect is an optional dependency
    # (loom-kernel[clickhouse]) and must not be required without a 'sql' section.
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry

    return ClickHouseConnectionRegistry(config=sql_cfg)


def _validate_sql_endpoint_auth(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Enforce the startup gates of every mounted identity-bound endpoint (§4)."""
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled or not endpoint.binds_identity:
            continue
        authenticator = _require_authenticator(name, auth)
        if auth.jwt_config is not None:
            _require_jwt_audience(f"SQL connection {name!r}", auth.jwt_config)
        _require_role_binding(name, connection.allowed_roles, authenticator)
        _require_authenticated_path(name, endpoint.path or f"/sql/{name}", auth.exclude_paths)


def _require_authenticator(name: str, auth: _AuthWiring) -> Authenticator:
    """An identity-bound endpoint without a mechanism has no identity to bind (§4)."""
    if auth.authenticator is not None:
        return auth.authenticator
    raise ConfigError(
        f"SQL connection {name!r}: sql_endpoint.auth requires a verified caller but the "
        "application configures no authentication. Add the 'app.rest.auth.jwt' section, "
        "pass create_app(authenticator=...), or switch the endpoint to auth: external."
    )


def _require_jwt_audience(subject: str, jwt_cfg: JwtAuthConfig) -> None:
    """Binding an endpoint to a token is void without a validated ``aud`` (§4).

    Args:
        subject: What is being gated, already formatted for the message — for
            example ``"SQL connection 'analytics'"`` or ``"Agent 'analyst'"``.
        jwt_cfg: JWT settings of the application.
    """
    if jwt_cfg.audience is not None:
        return
    raise ConfigError(
        f"{subject}: the endpoint requires a verified caller but "
        "'app.rest.auth.jwt.audience' is not set. Without a validated 'aud' any "
        "token signed by the same key — including tokens minted for another "
        "service — would be accepted as that verified caller, with whatever "
        "claims it carries."
    )


def _require_role_binding(
    name: str,
    allowed_roles: tuple[str, ...],
    authenticator: Authenticator,
) -> None:
    """A mounted multi-role endpoint must bind its roles to the identity (§4)."""
    if not roles_need_identity_binding(
        allowed_roles, mechanism_binds_roles=authenticator.provides_roles
    ):
        return
    raise ConfigError(
        f"SQL connection {name!r}: 'allowed_roles' is not empty but the "
        f"{authenticator.name!r} authentication mechanism binds no role to the caller "
        "identity. Without that binding the endpoint would let any authenticated caller "
        "pick any allowlisted role. Either configure the mechanism to provide roles "
        "(for JWT: 'app.rest.auth.jwt.roles_claim'), or leave 'allowed_roles' empty and "
        "pin a single 'default_role'."
    )


def _require_authenticated_path(name: str, path: str, exclude_paths: tuple[str, ...]) -> None:
    """A mounted SQL path listed in the exclusions would bypass auth (§4)."""
    if path not in exclude_paths:
        return
    raise ConfigError(
        f"SQL connection {name!r}: the endpoint path {path!r} is listed in the "
        "authentication 'exclude_paths', which would serve SQL without "
        "authentication. Remove it from the exclusion list."
    )


def _register_sql_collaborators(
    container: LoomContainer, sql: _SqlWiring, observability: ObservabilityRuntime
) -> None:
    """Register both SQL collaborators (APPLICATION scope) — always present (M5).

    ``SqlQueryService`` is the unbound path, for system work with no caller;
    ``CallerBoundSql`` derives the roles from the verified identity and is what
    a use case acting on behalf of a caller injects. Both are always
    resolvable, so neither choice depends on the config being present.

    The runtime is handed to the bound path so its queries leave the same audit
    trail as the REST endpoint: one span per query, labelled with the effective
    roles and the caller subject.
    """
    service = sql.service
    config = sql.config if sql.config is not None else SqlConfig(connections={})
    caller_bound = CallerBoundSql(service, config, observability)
    container.register(SqlQueryService, lambda: service, scope=Scope.APPLICATION)
    container.register(CallerBoundSql, lambda: caller_bound, scope=Scope.APPLICATION)


def _warn_sql_endpoints(sql_cfg: SqlConfig, auth: _AuthWiring) -> None:
    """Emit the spec §4 startup warnings for enabled SQL endpoints."""
    mechanism = _roles_mechanism(auth.authenticator)
    for name, connection in sql_cfg.connections.items():
        endpoint = connection.sql_endpoint
        if not endpoint.enabled:
            continue
        if not connection.readonly:
            warnings.warn(
                f"SQL connection {name!r} has an enabled endpoint with 'readonly: false' — "
                "callers can mutate data through it. Ensure this is intentional.",
                stacklevel=3,
            )
        if endpoint.auth is None:
            continue
        path = endpoint.path or f"/sql/{name}"
        # Narrowed per connection: a global mechanism does not bind the roles of
        # a connection whose allowlist is empty (see _connection_mechanism).
        bound = _connection_mechanism(connection, mechanism)
        warnings.warn(
            f"SQL endpoint mounted at {path} (connection={name!r}, "
            f"readonly={connection.readonly}, auth={endpoint.auth}, "
            f"allowed_roles={len(connection.allowed_roles)}). "
            "'auth' only authenticates the caller; the roles it may use come from the "
            "identity binding: "
            f"{_role_exposure_notice(bound, len(connection.allowed_roles))}"
            f"{_roles_source_detail(auth, bound)}.",
            stacklevel=3,
        )


def _roles_source_detail(auth: _AuthWiring, bound_mechanism: str | None) -> str:
    """Name the JWT claim carrying the roles, when the JWT mechanism is in use.

    The agnostic layers only know "the mechanism binds roles"; the composition
    root knows which claim, and operators need that to debug a denied caller.
    """
    jwt_cfg = auth.jwt_config
    if bound_mechanism is None or jwt_cfg is None or jwt_cfg.roles_claim is None:
        return ""
    return f" (JWT roles claim: {jwt_cfg.roles_claim!r})"


def _mount_authentication(app: FastAPI, auth: _AuthWiring) -> None:
    """Mount :class:`AuthenticationMiddleware` when a mechanism is configured.

    Mounted before the optional middlewares so ``TraceIdMiddleware`` wraps it
    and 401 bodies carry a trace id. A missing ``pyjwt`` extra surfaces at app
    startup as an ``ImportError`` with an install hint (fail-closed).
    """
    if auth.authenticator is None:
        return
    app.add_middleware(
        AuthenticationMiddleware,
        authenticator=auth.authenticator,
        exclude_paths=auth.exclude_paths,
    )


def _build_kernel_runtime(
    app_cfg: _AppConfig,
    discovered: DiscoveryResult,
    wiring: PersistenceWiring,
    metrics: Any | None = None,
    extra_modules: Sequence[Callable[[LoomContainer], None]] = (),
) -> KernelRuntime:
    return create_kernel(
        config=app_cfg,
        use_cases=discovered.use_cases,
        modules=[wiring.repo_registration_module, *extra_modules],
        uow_factory=wiring.uow_factory,
        metrics=metrics,
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
    code_path: str | None = None,
    metrics_registry: CollectorRegistry | None = None,
    authenticator: Authenticator | None = None,
    resolvers: Sequence[ConfigResolver] = (),
) -> FastAPI:
    """Create a FastAPI application from one or more YAML config files.

    Config files are merged left-to-right — later files override earlier ones.
    Each file may also declare a top-level ``includes`` list to pull in
    additional base files before its own values (resolved by
    :meth:`loom.core.config.ConfigContext.from_yaml`).

    ``${secrets:...}`` and ``${ssm:...}`` placeholders resolve through loom's
    built-in AWS resolvers unless *resolvers* supplies one with the same name.

    ``TraceIdMiddleware`` is mounted automatically. Structured logging and
    OTEL come from the top-level ``observability:`` section. Prometheus
    middleware is mounted when ``observability.prometheus.enabled`` is
    ``true``.

    Authentication is mechanism-agnostic: configure ``app.rest.auth.jwt`` for
    the built-in stateless JWT mechanism, or pass *authenticator* for any
    other.  Exactly one of the two may be active.

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
        *config_paths: One or more paths to YAML configuration files.
        code_path: Optional override for ``app.code_path``.  Resolved relative
            to the first config file when not absolute.
        metrics_registry: Optional Prometheus ``CollectorRegistry`` used for
            ``PrometheusMiddleware`` and the scrape endpoint.  Defaults to the
            global registry.  Pass a fresh ``CollectorRegistry()`` in tests to
            avoid ``ValueError: Duplicated timeseries`` when multiple apps with
            ``observability.prometheus.enabled: true`` are created in the same
            process.
        authenticator: Custom authentication mechanism.  Mutually exclusive
            with the ``app.rest.auth.jwt`` config section.
        resolvers: Resolvers for ``${name:key}`` placeholders, registered
            before the built-in ``secrets`` and ``ssm`` defaults.  A resolver
            named like a default replaces it.

    Returns:
        Configured :class:`fastapi.FastAPI` application, ready to serve.

    Raises:
        ConfigError: When no config path is given, or when both *authenticator*
            and ``app.rest.auth.jwt`` are supplied.

    Example — single config::

        app = create_app("config/app.yaml")

    Example — a mechanism of your own::

        app = create_app("config/app.yaml", authenticator=MyMtlsAuthenticator())

    Example — base + environment override::

        app = create_app("config/base.yaml", "config/production.yaml")

    Example — single file using inline includes::

        # config/app.yaml
        # includes:
        #   - base.yaml
        #   - secrets.yaml
        app = create_app("config/app.yaml")
    """
    if not config_paths:
        raise ConfigError("create_app requires at least one config file path.")

    ctx = ConfigContext.from_yaml(*config_paths, resolvers=with_default_resolvers(resolvers))
    app_cfg = _section_app_config(ctx)
    observability_cfg = _load_observability_config(ctx)
    observability_runtime = ObservabilityRuntime.from_config(observability_cfg)
    metrics_cfg = observability_cfg.prometheus

    auth = _resolve_authentication(
        app_cfg,
        authenticator,
        _documentation_paths(app_cfg.rest, metrics_cfg),
    )
    sql = _resolve_sql(ctx, auth)

    config_file = Path(config_paths[0]).resolve()
    effective_code_path = Path(code_path) if code_path is not None else Path(app_cfg.code_path)
    if not effective_code_path.is_absolute():
        effective_code_path = (config_file.parent / effective_code_path).resolve()
    _ensure_code_path(effective_code_path)

    metrics_adapter = _build_metrics_adapter(metrics_cfg, metrics_registry)

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
