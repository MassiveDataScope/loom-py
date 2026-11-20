"""Optional AI wiring for the composition root."""

from __future__ import annotations

from collections import deque
from collections.abc import Collection, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, get_args

from loom.composition.resources import Resource, ResourceKind
from loom.core.bootstrap import Kernel
from loom.core.config import ConfigContext, ConfigKey
from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraph, DependencyPath, GraphAnalysis, Node
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.execution import ExecutionBinding, Kind
from loom.core.identity import Identity
from loom.core.observability.observability import ObservabilityRuntime
from loom.core.stores.sql import SqlConfig, SqlQueryService
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.kinds import AGENT, MCP
from loom.core.use_case.registry import UseCaseRegistry

if TYPE_CHECKING:
    import pathlib

    from loom.ai.compiler import AgentPlan
    from loom.ai.config import AiConfig, McpServerConfig
    from loom.ai.errors import AgentCompilationIssue
    from loom.ai.execution import AgentRuntime, UseCaseMcpGrant


@dataclass(frozen=True)
class AgentDeps:
    """Per-invocation dependency bundle handed to an agent capability call.

    Args:
        container: Application container holding the singleton services.
        invoker: The application's invoker, for the deployment's own tools;
            each use case it invokes acts as the identity acting where it is
            called.
        state: The invocation's state, a mapping normalised against the
            artefact's declared shape, or ``None`` for an artefact that
            declares no state or a call that supplies none.
    """

    container: LoomContainer
    invoker: AppInvoker
    state: Mapping[str, Any] | None = None


class AgentDepsFactory:
    """Builds the per-invocation dependencies of every agent capability call.

    Structural implementation of ``loom.ai.abc.DepsFactory``, depending only on
    ``loom.core``. Every bundle carries the application's invoker.

    Args:
        invoker: The application's invoker.
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
            identity: The caller of this invocation; not read.
            container: Application container holding the singleton services.
            state: The invocation's state, a mapping normalised against the
                artefact's declared shape, or ``None`` for an artefact that
                declares no state or a call that supplies none.

        Returns:
            The bundle the engine passes to its capability calls.
        """
        del identity
        return AgentDeps(container=container, invoker=self._invoker, state=state)


@dataclass(frozen=True)
class AiWiring:
    """The agent runtime of a composed application and the ``ai:`` section it was built from.

    Args:
        config: Parsed ``ai:`` section, or ``None`` when absent.
        runtime: Agent runtime entered by the app lifespan alongside the SQL
            registry, or ``None`` when no section is configured.
        plans: Compiled agent plans, the runtime's own; empty when there is no
            runtime.
    """

    ABSENT: ClassVar[AiWiring]

    config: AiConfig | None
    runtime: AgentRuntime | None
    plans: tuple[AgentPlan, ...] = ()

    @classmethod
    def hosted(cls, config: AiConfig, runtime: AgentRuntime) -> AiWiring:
        """The wiring of *runtime*, built from *config*, with the runtime's own plans."""
        return cls(config=config, runtime=runtime, plans=runtime.plans)

    @classmethod
    def build(
        cls,
        ctx: ConfigContext,
        *,
        kernel: Kernel,
        sql_cfg: SqlConfig | None,
        code_path: pathlib.Path,
        agent_specs: tuple[str, ...],
        declared: MarkerDeclarations,
        names: Collection[str] | None = None,
    ) -> AiWiring:
        """Build the agent runtime the ``ai:`` section configures, without opening a connection.

        Without the section the AI pillar is never imported. With it, every
        ``Mcp()`` declaration is checked against ``ai.mcp_servers`` and
        granted, the engine is resolved and the artifacts are compiled
        offline; the runtime's live clients open with the application.

        Args:
            ctx: The application's configuration.
            kernel: The kernel the runtime serves.
            sql_cfg: The ``sql`` section, or ``None``.
            code_path: Root the artifact globs are relative to.
            agent_specs: Glob patterns from ``app.agents``.
            declared: Every ``Agent()`` and ``Mcp()`` declaration reached.
            names: The agents to compile, those an ``Agent()`` declaration
                names; ``None`` compiles every declared artifact. An agent left
                out is never entered, so its MCP servers are never opened.

        Returns:
            The wiring; :attr:`ABSENT` without the section.

        Raises:
            ConfigError: When ``app.agents`` declares artifacts and there is no
                ``ai:`` section.
            AgentCompilationError: When an ``Mcp()`` declaration names an
                unknown server, the artifact sources conflict or are missing,
                or an artifact does not compile.
        """
        if not ctx.has(ConfigKey.AI):
            if agent_specs:
                raise ConfigError(
                    "app.agents declares artifacts but the configuration has no 'ai:' section: "
                    "agent artifacts need an engine and its model bindings to compile. Add the "
                    "'ai:' section, or remove app.agents."
                )
            declared.mcp_grants({})
            return cls.ABSENT
        from loom.ai.bootstrap import build_agent_runtime
        from loom.ai.config import AiConfig
        from loom.ai.registry import resolve_engine_provider

        ai_cfg = ctx.section(ConfigKey.AI, AiConfig)
        grants = declared.mcp_grants(ai_cfg.mcp_servers)
        provider = resolve_engine_provider(ai_cfg.engine)
        runtime = build_agent_runtime(
            ai_cfg,
            root=code_path,
            names=names,
            specs=ai_cfg.artifact_specs(agent_specs, mcp_declared=bool(declared.mcp)),
            engine_provider=provider,
            registry=kernel.registry,
            container=kernel.container,
            deps=AgentDepsFactory(kernel.invoker),
            sql=sql_cfg,
            use_case_mcp=grants,
        )
        return cls.hosted(ai_cfg, runtime)

    @property
    def mcp_servers(self) -> Mapping[str, McpServerConfig]:
        """The configured MCP servers, by name; empty without an ``ai:`` section."""
        return self.config.mcp_servers if self.config is not None else {}

    @property
    def resource(self) -> Resource | None:
        """The agent runtime as the application's resource; ``None`` without a runtime."""
        runtime = self.runtime
        if runtime is None:
            return None
        return Resource(kind=ResourceKind.AGENTS, name="agents", manager=lambda: runtime)

    def bind_resolvers(self, kernel: Kernel) -> None:
        """Bind the executor's ``Agent()`` and ``Mcp()`` resolvers over :attr:`runtime`, if any.

        Args:
            kernel: The kernel whose executor resolves the markers; its
                container serves ``SqlQueryService`` and, optionally,
                ``ObservabilityRuntime``.
        """
        if self.runtime is None:
            return
        from loom.ai.execution._handle import AgentMarkerResolver, McpMarkerResolver

        container = kernel.container
        observability = container.resolve_or_none(ObservabilityRuntime)
        kernel.executor.bind_agent_resolver(
            AgentMarkerResolver(
                self.runtime,
                sql_query_service=container.resolve(SqlQueryService),
                observability=observability,
            )
        )
        kernel.executor.bind_mcp_resolver(
            McpMarkerResolver(self.runtime, observability=observability)
        )


AiWiring.ABSENT = AiWiring(config=None, runtime=None)


@dataclass(frozen=True, slots=True)
class ReachedDeclaration:
    """One execution declaration a root reaches.

    Attributes:
        binding: The declaring parameter: its name, kind, declaration and annotation.
        where: The parameter name for one the root declares itself, else the
            path from the root to the declaring node then the name, as the
            graph's refusals word it: ``"Market -> queries: Queries: gateway"``.
    """

    binding: ExecutionBinding
    where: str


@dataclass(frozen=True, slots=True)
class DeclarationWalk:
    """Every execution declaration a root reaches through *graph*.

    A node declares the execution kinds of its constructor or factory
    parameters, then those of its plan's ``execute()``.

    Args:
        graph: The analysed dependency graph.
        compiler: Holds the plan of each compiled use case.
    """

    graph: DependencyGraph
    compiler: UseCaseCompiler

    def reached(self, roots: Iterable[Node]) -> dict[Node, tuple[ReachedDeclaration, ...]]:
        """Every declaration each of *roots* reaches, by root, in their order.

        Each root is walked once, breadth first, along every edge of
        :meth:`~loom.core.di.graph.DependencyGraph.edges`, call edges included,
        so a called use case adds its declarations to its caller's. Each node
        is visited once per root, under the first (shortest) path to it, so a
        declaration appears once per parameter and path. A binding with no
        edges, such as an instance or a zero-argument factory, ends the walk
        there.

        Args:
            roots: The nodes to walk from.

        Returns:
            The declarations by root; a root reaching nothing maps to ``()``.
        """
        return {root: tuple(self._walk(root)) for root in roots}

    def _walk(self, root: Node) -> Iterator[ReachedDeclaration]:
        visited = {root}
        pending = deque([(root, DependencyPath(root))])
        while pending:
            node, path = pending.popleft()
            declared = self._declared(node)
            if declared:
                prefix = "" if node == root else f"{path}: "
                yield from (ReachedDeclaration(item, prefix + item.name) for item in declared)
            for edge in self.graph.edges(node):
                if edge.target not in visited:
                    visited.add(edge.target)
                    pending.append((edge.target, path.to(edge)))

    def _declared(self, node: Node) -> list[ExecutionBinding]:
        declared = [
            dependency
            for _, dependency in self.graph.constructor(node)
            if isinstance(dependency, ExecutionBinding)
        ]
        plan = self.compiler.get_plan(node.cls) if isinstance(node, ClassNode) else None
        if plan is not None:
            declared.extend(plan.execution_bindings)
        return declared


@dataclass(frozen=True, slots=True)
class MarkerDeclarations:
    """Every ``Agent()`` and ``Mcp()`` the roots of an analysed graph reach, split by marker.

    Each declaration is paired with the component reaching it: its root's
    registered key, else the root's name. Where it is declared, on the root
    itself or on an adapter, factory or called use case along a path, is
    :attr:`ReachedDeclaration.where`, which every startup message names as
    the parameter.

    Args:
        agents: The ``Agent()`` declarations, by root then path.
        mcp: The ``Mcp()`` declarations, by root then path.
    """

    agents: tuple[tuple[str, ReachedDeclaration], ...]
    mcp: tuple[tuple[str, ReachedDeclaration], ...]

    @classmethod
    def of(
        cls, analysis: GraphAnalysis, compiler: UseCaseCompiler, registry: UseCaseRegistry
    ) -> MarkerDeclarations:
        """Walk each root of *analysis* once and keep what it reaches of the two AI markers.

        A marker reached only through an adapter is kept like one in
        ``execute()``; one behind an instance binding is never reached.

        Args:
            analysis: The analysed dependency graph and its roots.
            compiler: Holds the plan of each compiled use case.
            registry: Names each root registered as a use case.

        Returns:
            The declarations found.
        """
        by_kind: dict[Kind, list[tuple[str, ReachedDeclaration]]] = {AGENT: [], MCP: []}
        walk = DeclarationWalk(analysis.graph, compiler)
        for root, found in walk.reached(analysis.roots).items():
            component = (isinstance(root, ClassNode) and registry.key_for(root.cls)) or str(root)
            for declaration in found:
                of_kind = by_kind.get(declaration.binding.kind)
                if of_kind is not None:
                    of_kind.append((component, declaration))
        return cls(agents=tuple(by_kind[AGENT]), mcp=tuple(by_kind[MCP]))

    def verify(self, plans: Mapping[str, AgentPlan]) -> None:
        """Refuse every ``Agent()`` declaration start-up cannot satisfy.

        Args:
            plans: Every compiled agent plan, by name; empty when no runtime
                is built, which reports every declared agent as unknown.

        Raises:
            AgentCompilationError: With one issue per unknown agent, then per
                mismatched output type.
        """
        if not self.agents:
            return
        from loom.ai.errors import AgentCompilationError

        issues = list(self._agent_issues(plans))
        if issues:
            raise AgentCompilationError(issues)

    def mcp_grants(self, servers: Mapping[str, McpServerConfig]) -> tuple[UseCaseMcpGrant, ...]:
        """Compile every ``Mcp()`` declaration into the runtime's own grants.

        Args:
            servers: Every configured MCP server, by name; empty when no
                ``ai:`` section is present, which reports every declared
                server as unknown.

        Returns:
            One grant per declaration, in order; its parameter is where the
            declaration is.

        Raises:
            AgentCompilationError: With one issue per declaration naming a
                server not in *servers*.
        """
        if not self.mcp:
            return ()
        from loom.ai.compiler.phases import compile_mcp_capability
        from loom.ai.errors import AgentCompilationError, AgentErrorCode
        from loom.ai.execution import UseCaseMcpGrant

        available = tuple(servers)
        issues: list[AgentCompilationIssue] = []
        grants: list[UseCaseMcpGrant] = []
        for component, found in self.mcp:
            marker = found.binding.declaration
            capability, _ = compile_mcp_capability(
                marker.server,
                include=marker.include,
                exclude=(),
                servers=servers,
                component=component,
            )
            if capability is None:
                issues.append(
                    AgentErrorCode.MCP_MARKER_UNKNOWN.issue(
                        component=component,
                        parameter=found.where,
                        server=marker.server,
                        available=available,
                    )
                )
                continue
            grants.append(
                UseCaseMcpGrant(capability=capability, usecase=component, parameter=found.where)
            )
        if issues:
            raise AgentCompilationError(issues)
        return tuple(grants)

    def _agent_issues(self, plans: Mapping[str, AgentPlan]) -> Iterator[AgentCompilationIssue]:
        from loom.ai.errors import AgentErrorCode

        available = tuple(plans)
        for component, found in self.agents:
            agent = found.binding.declaration.name
            plan = plans.get(agent)
            if plan is None:
                yield AgentErrorCode.AGENT_MARKER_UNKNOWN.issue(
                    component=component, parameter=found.where, agent=agent, available=available
                )
                continue
            expected_args = get_args(found.binding.annotation)
            if not expected_args:
                continue
            expected, declared = expected_args[0], plan.output.loom_type.type
            if expected is declared:
                continue
            yield AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH.issue(
                component=component,
                parameter=found.where,
                agent=agent,
                expected=getattr(expected, "__name__", str(expected)),
                declared=getattr(declared, "__name__", str(declared)),
            )
