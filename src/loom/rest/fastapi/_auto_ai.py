"""Optional AI wiring for the FastAPI composition root."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.di.container import LoomContainer
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.identity import Identity
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import SqlConfig, SqlQueryService
from loom.core.use_case.agent_markers import declaring_agent_bindings
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.mcp_markers import DeclaringMcpBindings, declaring_mcp_bindings
from loom.core.use_case.registry import UseCaseRegistry
from loom.rest.fastapi._auto_auth import _AuthWiring, _require_jwt_audience

if TYPE_CHECKING:
    from loom.ai.compiler import AgentPlan
    from loom.ai.config import AiConfig, McpServerConfig
    from loom.ai.errors import AgentCompilationIssue
    from loom.ai.runtime import AgentRuntime, UseCaseMcpGrant


_AGENTS_PREFIX = "/agents"


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
        plans: Compiled agent plans, the runtime's own; empty when there is no
            runtime.
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
            declares an ``Mcp()`` marker. When ``True``, an empty artifact set
            is tolerated instead of aborting: a deployment whose only AI usage
            is ``Mcp()`` need not ship a filler agent artifact it never runs.
            It still declares ``ai.engine`` and installs the extra:
            ``_resolve_ai`` resolves the engine provider, and
            ``build_agent_runtime`` takes the MCP client factory from it.

    Raises:
        AgentCompilationError: When both sources declare artifacts, or when
            neither does and no use case declares ``Mcp()``.
    """
    # Local import: same containment rule as '_resolve_ai'; this helper only
    # ever runs from the branch that already decided to load the AI pillar.
    from loom.ai.errors import AgentCompilationError, AgentErrorCode

    if config_specs and manifest_specs:
        raise AgentCompilationError([AgentErrorCode.AGENT_SPECS_CONFLICT.issue()])
    if not config_specs and not manifest_specs and not has_declaring_use_case:
        raise AgentCompilationError([AgentErrorCode.AGENT_SPECS_MISSING.issue()])
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
    from loom.ai.bootstrap import build_agent_runtime
    from loom.ai.config import AiConfig
    from loom.ai.registry import resolve_engine_provider

    ai_cfg = ctx.section(ConfigKey.AI, AiConfig)
    provider = resolve_engine_provider(ai_cfg.engine)
    specs = _effective_agent_specs(
        ai_cfg.specs, manifest_agent_specs, has_declaring_use_case=bool(use_case_mcp_bindings)
    )
    runtime = build_agent_runtime(
        ai_cfg,
        root=code_path,
        specs=specs,
        engine_provider=provider,
        registry=kernel.registry,
        container=kernel.container,
        deps=_AgentDepsFactory(kernel.app),
        sql=sql_cfg,
        use_case_mcp=_compile_use_case_mcp(
            use_case_mcp_bindings, kernel.registry, ai_cfg.mcp_servers
        ),
    )
    return _AiWiring(config=ai_cfg, runtime=runtime, plans=runtime.plans)


def _compile_use_case_mcp(
    declaring: DeclaringMcpBindings,
    registry: UseCaseRegistry,
    servers: Mapping[str, McpServerConfig],
) -> tuple[UseCaseMcpGrant, ...]:
    """Compile every verified ``Mcp()`` binding into the runtime's own grants.

    ``_verify_mcp_markers`` already checked every binding's server name
    against this same ``servers`` mapping, so the shared compile helper's own
    ``MCP_SERVER_UNKNOWN`` is unreachable from this call by construction. The
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
