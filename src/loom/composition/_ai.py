"""Optional AI wiring for the composition root."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loom.core.bootstrap import KernelRuntime
from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, GraphAnalysis
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.kinds import AGENT, MCP
from loom.core.execution import Kind
from loom.core.identity import Identity
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import SqlConfig, SqlQueryService
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.walk import Reached, reached

if TYPE_CHECKING:
    from loom.ai.compiler import AgentPlan
    from loom.ai.config import AiConfig, McpServerConfig
    from loom.ai.errors import AgentCompilationIssue
    from loom.ai.runtime import AgentRuntime, UseCaseMcpGrant


@dataclass(frozen=True)
class AgentDeps:
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


class AgentDepsFactory:
    """Builds the per-invocation dependencies of every agent capability call.

    Structural implementation of ``loom.ai.abc.DepsFactory``: it lives in the
    composition root, and depends only on ``loom.core``, so an application with
    no ``ai:`` section never imports the AI pillar to obtain it.

    Binding the invoker to the caller happens here, at the one point that knows
    both: a granted use case declaring ``Caller()`` is executed as the identity
    that invoked the agent, and the capability never has to discover the caller
    from ambient state.

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
        return AgentDeps(
            identity=identity,
            container=container,
            invoker=self._invoker.for_identity(identity),
            state=state,
        )


@dataclass(frozen=True)
class AiWiring:
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


@dataclass(frozen=True, slots=True)
class MarkerDeclarations:
    """Every ``Agent()`` and ``Mcp()`` the roots of an analysed graph reach, split by marker.

    Each declaration is paired with the component reaching it: its root's
    registered key, else the root's name. Where it is declared, on the root
    itself or on an adapter, factory or called use case along a path, is
    :attr:`~loom.core.use_case.walk.Reached.where`, which every startup
    message names as the parameter.

    Args:
        agents: The ``Agent()`` declarations, by root then path.
        mcp: The ``Mcp()`` declarations, by root then path.
    """

    agents: tuple[tuple[str, Reached], ...]
    mcp: tuple[tuple[str, Reached], ...]


def marker_declarations(
    analysis: GraphAnalysis, compiler: UseCaseCompiler, registry: UseCaseRegistry
) -> MarkerDeclarations:
    """Walk each root of *analysis* once and keep what it reaches of the two AI markers.

    A marker reached only through an adapter is kept like one in
    ``execute()``; one behind an instance binding is never reached.
    """
    by_kind: dict[Kind, list[tuple[str, Reached]]] = {AGENT: [], MCP: []}
    for root, found in reached(analysis.roots, analysis.graph, compiler).items():
        component = (isinstance(root, ClassNode) and registry.key_for(root.cls)) or str(root)
        for declaration in found:
            of_kind = by_kind.get(declaration.binding.kind)
            if of_kind is not None:
                of_kind.append((component, declaration))
    return MarkerDeclarations(agents=tuple(by_kind[AGENT]), mcp=tuple(by_kind[MCP]))


def effective_agent_specs(
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
        has_declaring_use_case: Whether at least one root reaches an
            ``Mcp()`` declaration, in ``execute()`` or through an adapter.
            When ``True``, an empty artifact set is tolerated instead of
            aborting: a deployment whose only AI usage is ``Mcp()`` need not
            ship a filler agent artifact it never runs.
            It still declares ``ai.engine`` and installs the extra:
            ``resolve_ai`` resolves the engine provider, and
            ``build_agent_runtime`` takes the MCP client factory from it.

    Raises:
        AgentCompilationError: When both sources declare artifacts, or when
            neither does and no use case declares ``Mcp()``.
    """
    # Local import: same containment rule as 'resolve_ai'; this helper only
    # ever runs from the branch that already decided to load the AI pillar.
    from loom.ai.errors import AgentCompilationError, AgentErrorCode

    if config_specs and manifest_specs:
        raise AgentCompilationError([AgentErrorCode.AGENT_SPECS_CONFLICT.issue()])
    if not config_specs and not manifest_specs and not has_declaring_use_case:
        raise AgentCompilationError([AgentErrorCode.AGENT_SPECS_MISSING.issue()])
    return config_specs or manifest_specs


def resolve_ai(
    ctx: ConfigContext,
    *,
    kernel: KernelRuntime,
    sql_cfg: SqlConfig | None,
    code_path: Path,
    manifest_agent_specs: tuple[str, ...],
    mcp: Sequence[tuple[str, Reached]],
    names: Collection[str] | None = None,
) -> AiWiring:
    """Load the optional ``ai:`` section into its compiled agent runtime.

    Absent section -> empty wiring, and the AI pillar is never imported.
    Present section -> the configured engine is resolved, the declared
    artifacts are compiled offline, and the runtime is built without opening
    a single connection: its live clients open inside the app lifespan.

    Args:
        mcp: Every ``Mcp()`` declaration reached, see
            :class:`MarkerDeclarations` — already verified against
            ``ai.mcp_servers`` by ``verify_mcp_markers``, before this
            function ever runs.
        names: The agents to compile, those an ``Agent()`` declaration names; ``None``
            compiles every declared artifact. An agent left out is never
            entered, so its MCP servers are never opened.
    """
    if not ctx.has(ConfigKey.AI):
        return AiWiring(config=None, runtime=None)
    # Local imports: the AI pillar is optional, and importing it from an
    # application without an 'ai:' section would pull the whole agent layer
    # into every Loom app — the same rule 'build_sql_registry'
    # follows for the ClickHouse extra.
    from loom.ai.bootstrap import build_agent_runtime
    from loom.ai.config import AiConfig
    from loom.ai.registry import resolve_engine_provider

    ai_cfg = ctx.section(ConfigKey.AI, AiConfig)
    provider = resolve_engine_provider(ai_cfg.engine)
    specs = effective_agent_specs(
        ai_cfg.specs, manifest_agent_specs, has_declaring_use_case=bool(mcp)
    )
    runtime = build_agent_runtime(
        ai_cfg,
        root=code_path,
        names=names,
        specs=specs,
        engine_provider=provider,
        registry=kernel.registry,
        container=kernel.container,
        deps=AgentDepsFactory(kernel.app),
        sql=sql_cfg,
        use_case_mcp=compile_use_case_mcp(mcp, ai_cfg.mcp_servers),
    )
    return AiWiring(config=ai_cfg, runtime=runtime, plans=runtime.plans)


def compile_use_case_mcp(
    mcp: Sequence[tuple[str, Reached]],
    servers: Mapping[str, McpServerConfig],
) -> tuple[UseCaseMcpGrant, ...]:
    """Compile every verified ``Mcp()`` declaration into the runtime's own grants.

    ``verify_mcp_markers`` already checked every declaration's server name
    against this same ``servers`` mapping, so the shared compile helper's own
    ``MCP_SERVER_UNKNOWN`` is unreachable from this call by construction. The
    caller still raises on one anyway rather than dropping it silently,
    fail-closed should that ordering ever break — one line, no new
    vocabulary.

    Args:
        mcp: Every ``Mcp()`` declaration reached, with the component
            reaching it, see :class:`MarkerDeclarations`.
        servers: Every MCP server configured for this deployment.

    Returns:
        One ``UseCaseMcpGrant`` per declaration, in order; its parameter is
        where the declaration is.

    Raises:
        AgentCompilationError: Only if ``verify_mcp_markers``'s ordering were
            ever broken and an unknown server reached this call unverified.
    """
    # Local imports: same containment rule as 'resolve_ai' — this helper
    # only ever runs from the branch that already decided to load the AI
    # pillar.
    from loom.ai.compiler.phases import compile_mcp_capability
    from loom.ai.errors import AgentCompilationError
    from loom.ai.runtime import UseCaseMcpGrant

    grants: list[UseCaseMcpGrant] = []
    issues: list[AgentCompilationIssue] = []
    for component, found in mcp:
        marker = found.binding.declaration
        capability, found_issues = compile_mcp_capability(
            marker.server, include=marker.include, exclude=(), servers=servers, component=component
        )
        issues.extend(found_issues)
        if capability is not None:
            grants.append(
                UseCaseMcpGrant(capability=capability, usecase=component, parameter=found.where)
            )
    if issues:
        raise AgentCompilationError(issues)
    return tuple(grants)


def verify_mcp_markers(mcp: Sequence[tuple[str, Reached]], ctx: ConfigContext) -> None:
    """Abort start-up when an ``Mcp()`` declaration names a server not configured.

    Runs immediately before the ``resolve_ai`` call, not in the
    ``verify_agent_markers`` slot after it: an unknown server must die here
    so the shared MCP capability compiler (``compile_mcp_capability``) can
    never be asked to report it for a use case — one condition, one message.

    The AI section is decoded a second time here, once per boot, only when at
    least one declaration is reached — ``ConfigContext.section`` caches
    nothing. Accepted overhead, in exchange for one call site that also
    covers the no-``ai:``-section case instead of two call sites keyed
    on whether the section exists.

    Args:
        mcp: Every ``Mcp()`` declaration reached, see :class:`MarkerDeclarations`.
        ctx: Deployment configuration, read for ``ai.mcp_servers`` only when
            at least one declaration is reached.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown server name,
            each naming where it is declared.
    """
    if not mcp:
        return
    import loom.ai._startup as startup
    from loom.ai.config import AiConfig

    servers = ctx.section(ConfigKey.AI, AiConfig).mcp_servers if ctx.has(ConfigKey.AI) else {}
    startup.verify_mcp_markers(mcp, servers)


def bind_agent_resolver(result: KernelRuntime, ai: AiWiring) -> None:
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
    # Local import: same containment rule as 'resolve_ai' — the AI pillar
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


def bind_mcp_resolver(result: KernelRuntime, ai: AiWiring) -> None:
    """Wire the executor's ``Mcp()`` marker resolver, once the AI runtime exists.

    Mirrors :func:`bind_agent_resolver`, on the executor's second resolver.
    A no-op only for a deployment where no use case declares ``Mcp()``: one
    that does never reaches this function, because ``verify_mcp_markers``
    already aborted start-up with ``MCP_MARKER_UNKNOWN`` against an empty
    server mapping. The executor's own "no resolver bound" refusal is
    therefore unreachable through this composition root, and stays as the
    guard for a host that wires its own.
    """
    if ai.runtime is None:
        return
    # Local import: same containment rule as 'bind_agent_resolver'.
    from loom.ai.runtime._handle import mcp_marker_resolver

    observability = (
        result.container.resolve(ObservabilityRuntime)
        if result.container.is_registered(ObservabilityRuntime)
        else None
    )
    resolver = mcp_marker_resolver(ai.runtime, observability=observability)
    result.executor.bind_mcp_resolver(resolver)


def verify_agent_markers(agents: Sequence[tuple[str, Reached]], ai: AiWiring) -> None:
    """Abort start-up when an ``Agent()`` declaration cannot be satisfied.

    Runs in the slot between 'resolve_ai' and 'result.factory.verify()':
    every compiled agent plan already exists here, whether or not an ``ai:``
    section is present — an absent section means ``ai.plans`` is simply
    empty, so every declared agent is reported unknown the same way a typo
    would be. The check itself is shared with :func:`reject_ai_markers`
    (T402), which calls the same :func:`~loom.ai._startup.verify_agent_markers`
    with an always-empty ``plans_by_name`` for an entry point that never
    builds an AI runtime at all.

    Args:
        agents: Every ``Agent()`` declaration reached, see :class:`MarkerDeclarations`.
        ai: The (possibly empty) AI wiring 'resolve_ai' built.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown agent name
            and per mismatched output type, so a single run reports every
            problem at once.
    """
    if not agents:
        return
    import loom.ai._startup as startup

    startup.verify_agent_markers(agents, {plan.name: plan for plan in ai.plans})


def reject_ai_markers(declared: MarkerDeclarations) -> None:
    """Abort start-up when an entry point without agents reaches an ``Agent()`` or ``Mcp()``.

    An entry point whose recipe declares ``agents=False`` (a task worker, for
    instance) never builds an AI runtime, so a declaration reached in
    ``execute()`` or through an adapter would compile and only fail once
    something actually invoked it. Refusing at start-up instead reuses the
    checks an agent-hosting entry point runs,
    :func:`loom.ai._startup.verify_agent_markers` then
    :func:`loom.ai._startup.verify_mcp_markers`, called with nothing
    configured, which reports every declared name as unknown.

    Args:
        declared: Every declaration the roots reach, see :class:`MarkerDeclarations`.

    Raises:
        AgentCompilationError: Naming each declaring component and where the
            declaration is, when at least one is reached.
    """
    if not declared.agents and not declared.mcp:
        return
    import loom.ai._startup as startup

    startup.verify_agent_markers(declared.agents, {})
    startup.verify_mcp_markers(declared.mcp, {})


def reject_manifest_agents_without_ai_section(
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
