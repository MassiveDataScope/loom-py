"""One entered agent host: shared clients opened once, engines built once.

Entering opens every live client concurrently through one
:class:`~contextlib.AsyncExitStack` owned by the entering task, verifies the
declared tool filters and the read-only state of every SQL grant, and builds
one engine per plan, all within ``startup_timeout_ms``; under
``ai.remote_clients: optional`` a tolerated connection failure gives the
filter pass a fresh budget. Leaving closes everything in reverse order.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType, TracebackType
from typing import Any, Self, cast
from uuid import uuid4

from loom.ai._filters import select_names
from loom.ai.abc import (
    CONVERSATION_ID_MAX_LENGTH,
    AgentEngine,
    AgentEnginePlugin,
    AgentEvent,
    AgentResult,
    Conversation,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthState,
    McpSession,
    McpToolInfo,
    NativeCapableEngine,
    Prompt,
    StateShape,
)
from loom.ai.compiler._plan import (
    HOOK_OUTPUT_FIELD,
    AgentPlan,
    CompiledA2ACapability,
    CompiledMcpCapability,
    CompiledSqlCapability,
    mcp_connection,
)
from loom.ai.config import AiConfig
from loom.ai.errors import (
    INVOKER_MISSING_REASON,
    NO_TERMINAL_MESSAGE,
    AgentCompilationError,
    AgentCompilationIssue,
    AgentErrorCode,
    AgentRunError,
    AgentRunErrorCode,
    require_authenticated,
)
from loom.ai.event import AiEvent, AiKey
from loom.ai.execution._chain import enter_agent_call, exit_agent_call
from loom.ai.execution._grants import AgentGrants, McpGrant
from loom.ai.execution._limits import cancel_task, supervised_events
from loom.ai.execution._mcp import (
    FilterTarget,
    McpClientFactory,
    connection_conflicts,
    filter_issues,
    filter_targets,
    listing_timeout_issues,
    mcp_session_for,
)
from loom.ai.execution._turn import TurnCallbacks
from loom.core.di import LoomContainer
from loom.core.errors import InvalidUse, InvalidValue, UnknownKey
from loom.core.errors.summary import ErrorSummary
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.log_names import LogField
from loom.core.model import LoomFrozenStruct, LoomType
from loom.core.stores.sql.config import SqlConfig
from loom.core.use_case.invoker import ApplicationInvoker

_logger = logging.getLogger(__name__)


A2AClientFactory = Callable[[CompiledA2ACapability], AbstractAsyncContextManager[object]]
"""Builds the (not yet opened) client of one compiled A2A capability."""

_EMPTY_CHECKS: Mapping[str, HealthState] = MappingProxyType({})


class AgentHealth(LoomFrozenStruct, frozen=True, kw_only=True):
    """Cached health of one agent and of its live dependencies.

    Attributes:
        status: Aggregate state, the worst of every check.
        checks: Per-dependency state, keyed ``"model"``, ``"mcp:<server>"``,
            ``"a2a:<agent>"`` or ``"sql:<connection>"``, always by the name the
            deployment registered rather than by URL. Internal topology: only
            an authenticated caller ever sees it.
        detail: Optional explanation, ``"probing"`` until the first probe of
            the background refresher completes.
    """

    status: HealthState
    checks: Mapping[str, HealthState] = _EMPTY_CHECKS
    detail: str | None = None


_PROBING = AgentHealth(status=HealthState.DEGRADED, detail="probing")

_PROBE_FAILED = "the health probe failed; the detail is recorded server-side"
"""Detail of an agent whose engine probe raised. No exception text: the probe
reaches a model provider, so its failures carry endpoints and credential
references that an anonymous ``/health`` scrape must never receive."""

_OPTIONAL_REMOTE_CLIENTS = "optional"
"""Value of ``ai.remote_clients`` under which a connection failure is tolerated."""


@dataclass(frozen=True, slots=True)
class _AgentSlot:
    """One compiled plan and the single engine built for it."""

    plan: AgentPlan
    engine: AgentEngine


@dataclass(frozen=True, slots=True)
class _OpenedClient:
    """One live client, in the order its connection actually completed."""

    key: str
    client: AbstractAsyncContextManager[Any]
    session: object


@dataclass(frozen=True, slots=True)
class UseCaseMcpGrant:
    """One verified ``Mcp()`` marker binding, compiled into a host input.

    Its server joins the clients the host opens even when no agent plan
    declares it.

    Attributes:
        capability: The compiled server grant, with the marker's own ``include``,
            checked at start-up against the server's tool list.
        usecase: The declaring use case.
        parameter: The declaring parameter, named when its ``include`` matches
            no tool.
    """

    capability: CompiledMcpCapability
    usecase: str
    parameter: str


class AgentHost:
    """Owns the live agents of one worker: clients, engines and their limits.

    Usable only as an async context manager, and only from the task that
    entered it, which closes every client in reverse order.

    Args:
        plans: Compiled plans this worker serves.
        config: Deployment configuration of the AI pillar.
        engine_plugin: Provider building one engine per plan, exactly once.
        deps: Per-invocation dependency factory handed to every engine.
        container: Application container the engines resolve services from.
        sql_config: Live ``sql:`` configuration, re-verified at start-up
            against what the plans were compiled against.
        mcp_client_factory: Builds the client of one MCP capability; required,
            even under ``ai.remote_clients: optional``, when any plan declares
            an ``mcp`` capability.
        a2a_client_factory: Builds the client of one A2A capability, with the
            same fail-closed rule.
        use_case_mcp: Every ``Mcp()`` marker binding a declaring use case
            carries (:class:`UseCaseMcpGrant`). Each server joins the clients
            this host opens; one named only this way is no ``/health``
            check entry.

    Runs emit no span of their own; the transport opens it.

    Raises:
        AgentCompilationError: From ``__aenter__``, aggregating every start-up
            failure — an unreachable server (named as the deployment
            registered it, never by URL; tolerated under
            ``ai.remote_clients: optional``), a tool filter
            matching nothing, two grants of one MCP server name describing
            different connections, or a SQL connection whose read-only state
            drifted.

    Example::

        async with AgentHost(
            plans=plans, config=ai_config, engine_plugin=provider,
            deps=deps, container=container,
        ) as host:
            result = await host.run("analyst", prompt, identity=identity)
    """

    def __init__(
        self,
        *,
        plans: Sequence[AgentPlan],
        config: AiConfig,
        engine_plugin: AgentEnginePlugin,
        deps: DepsFactory,
        container: LoomContainer,
        sql_config: SqlConfig | None = None,
        mcp_client_factory: McpClientFactory | None = None,
        a2a_client_factory: A2AClientFactory | None = None,
        use_case_mcp: Sequence[UseCaseMcpGrant] = (),
    ) -> None:
        self._plans: Mapping[str, AgentPlan] = MappingProxyType({p.name: p for p in plans})
        self._config = config
        self._engine_plugin = engine_plugin
        self._deps = deps
        self._container = container
        self._invoker: ApplicationInvoker | None = None
        self._sql_config = sql_config
        self._mcp_client_factory = mcp_client_factory
        self._a2a_client_factory = a2a_client_factory
        self._use_case_mcp = tuple(use_case_mcp)
        self._stack: AsyncExitStack | None = None
        self._owner: asyncio.Task[Any] | None = None
        self._slots: dict[str, _AgentSlot] = {}
        self._sessions: dict[str, McpSession] = {}
        self._tool_catalog: dict[str, tuple[McpToolInfo, ...]] = {}
        self._live: set[str] = set()
        self._health: dict[str, AgentHealth] = {}
        self._grants: dict[str, AgentGrants] = {}
        self._use_case_grants: dict[str, McpGrant | None] = {}
        self._runs = asyncio.Semaphore(config.max_concurrent_runs)

    async def __aenter__(self) -> Self:
        """Open every live client, validate the plans and build the engines.

        Returns:
            The entered host.

        Raises:
            RuntimeError: When the host was already entered.
            AgentCompilationError: Aggregating every start-up failure. Every
                client opened so far is closed before the error propagates.
        """
        if self._stack is not None:
            raise InvalidUse("AgentHost is already entered")
        stack = AsyncExitStack()
        self._stack = stack
        self._owner = asyncio.current_task()
        deadline = self._startup_deadline()
        try:
            self._admit_sql_grants()
            self._invoker = self._resolve_invoker()
            self._verify_mcp_connections()
            tolerated = await self._open_clients(stack, deadline)
            await self._verify_tool_filters(self._startup_deadline() if tolerated else deadline)
            self._build_engines()
            self._grants = self._build_grants()
            self._use_case_grants = self._build_use_case_grants()
            self._start_health_probe(stack)
        except BaseException:
            self._stack = None
            self._owner = None
            await stack.aclose()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close every live client in reverse order, in the task that opened them.

        Raises:
            RuntimeError: When the host was never entered, or when the
                exiting task is not the one that entered it — closing a
                session-affine client from a foreign task is a latent
                corruption, not a detail to paper over.
        """
        stack = self._stack
        if stack is None:
            raise InvalidUse("AgentHost was not entered")
        if asyncio.current_task() is not self._owner:
            raise InvalidUse(
                "AgentHost must be exited by the task that entered it: the live "
                "clients are session-affine and closing them from another task "
                "corrupts them"
            )
        self._stack = None
        self._owner = None
        self._slots.clear()
        self._sessions.clear()
        self._tool_catalog.clear()
        self._live.clear()
        self._grants.clear()
        self._use_case_grants.clear()
        await stack.aclose()

    @property
    def plans(self) -> tuple[AgentPlan, ...]:
        """Every compiled plan this host serves, in the order they were given."""
        return tuple(self._plans.values())

    def agent_names(self) -> tuple[str, ...]:
        """Return the names of every agent this host serves.

        Returns:
            One name per compiled plan, in the order the plans were given.
        """
        return tuple(self._plans)

    def has_agent(self, name: str) -> bool:
        """Report whether an agent with that name is served by this host.

        Args:
            name: Agent name to look for.

        Returns:
            ``True`` when the host holds a plan with that name.
        """
        return name in self._plans

    def has_conversation(self, name: str) -> bool:
        """Report whether one agent declares a conversation loader.

        Args:
            name: Agent to describe.

        Returns:
            ``True`` when the artifact declares a ``conversation`` loader.

        Raises:
            KeyError: When no agent is named *name*.
        """
        return self._require_plan(name).conversation is not None

    def capability_kinds(self, name: str) -> tuple[str, ...]:
        """Return the capability kinds one agent was granted.

        Lets a transport state what a route exposes without reaching into the
        compiled plan, which carries resolved handles and instructions.

        Args:
            name: Agent to describe.

        Returns:
            The distinct ``kind`` identifiers of the agent's capabilities, in
            declaration order.

        Raises:
            KeyError: When no agent is named *name*.
        """
        plan = self._require_plan(name)
        kinds = {capability.kind: None for capability in plan.capabilities}
        return tuple(kinds)

    async def run(
        self,
        name: str,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation_id: str | None = None,
        output_type: type[Any] | None = None,
        state: object | None = None,
    ) -> AgentResult:
        """Run one agent to completion.

        Args:
            name: Agent to run.
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation_id: Opaque value the application supplies; selects
                the conversation the loader use case receives, when the
                artifact declares one, and is copied verbatim into the output
                hook's command.  Never read by loom.
            output_type: When given, decode this run's answer into this type
                instead of the plan's own declared output, without the plan's
                output check; ``None`` runs the plan's declared shape.
            state: This run's state, already decoded by whichever boundary
                received it and passed as a normalised mapping, or ``None``.
                Resolved against *name*'s declared shape before this call
                reaches the engine: ``None`` on a stateful
                artefact whose every field carries a default carries the
                shape's own declared defaults; ``None`` against a shape with
                a field that has no default fails the call, since there is
                no default to fill it with; a non-``None`` value against an
                artefact declaring no shape fails the call.

        Returns:
            The decoded output, the run's usage, its ``interaction_id``, the
            output hook's result and the run's new messages.

        Raises:
            KeyError: When no agent is named *name*.
            ValueError: When ``conversation_id`` is empty or longer than
                :data:`~loom.ai.abc.CONVERSATION_ID_MAX_LENGTH`.
            AgentRunError: When the run is refused (``TOO_MANY_RUNS``), the
                conversation cannot be loaded (``CONVERSATION_LOAD_FAILED``,
                ``CONVERSATION_LOAD_TIMEOUT``), ``state`` is given against an
                artefact declaring none (``STATE_UNDECLARED``), ``state`` is
                omitted against a shape with a field that has no default
                (``STATE_REQUIRED``), breaches a declared limit, or ends in a
                failure event.
        """
        result: AgentResult | None = None
        stream = self._run_stream(
            name,
            prompt,
            identity=identity,
            conversation_id=conversation_id,
            output_type=output_type,
            state=state,
        )
        async with stream as events:
            async for event in events:
                if type(event) is ErrorEvent:
                    raise AgentRunError(
                        event.code,
                        str(event.message),
                        interaction_id=event.interaction_id,
                        usage=event.usage,
                    )
                if type(event) is FinalEvent:
                    result = AgentResult(
                        output=event.output,
                        usage=event.usage,
                        interaction_id=event.interaction_id,
                        hook_result=event.hook_result,
                        messages=event.messages,
                        provider_details=event.provider_details,
                    )
        if result is None:
            raise AgentRunError(
                AgentRunErrorCode.PROVIDER_UNAVAILABLE, NO_TERMINAL_MESSAGE.format(agent=name)
            )
        return result

    def run_stream(
        self,
        name: str,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Run one agent, streaming its supervised events.

        The stream is an async context manager so the engine connection behind
        it closes deterministically instead of waiting for the collector.

        Args:
            name: Agent to run.
            prompt: Caller prompt.
            identity: Verified caller; every capability call runs as them.
            conversation_id: Opaque value the application supplies; selects
                the conversation the loader use case receives, when the
                artifact declares one, and is copied verbatim into the output
                hook's command.  Never read by loom.
            state: This run's state; see :meth:`run`'s own ``state`` for what
                it carries and how it is resolved against *name*'s declared
                shape.

        Returns:
            An async context manager yielding the limit-supervised events; the
            terminal event carries the run's ``interaction_id``.

        Raises:
            KeyError: When no agent is named *name*.
            ValueError: On entry, when ``conversation_id`` is empty or longer
                than :data:`~loom.ai.abc.CONVERSATION_ID_MAX_LENGTH`.
            AgentRunError: On entry, when the worker's ``max_concurrent_runs``
                is already taken (``TOO_MANY_RUNS``), the conversation cannot
                be loaded (``CONVERSATION_LOAD_FAILED``,
                ``CONVERSATION_LOAD_TIMEOUT``), ``state`` is given against an
                artefact declaring none (``STATE_UNDECLARED``), or ``state``
                is omitted against a shape with a field that has no default
                (``STATE_REQUIRED``).
        """
        return self._run_stream(
            name, prompt, identity=identity, conversation_id=conversation_id, state=state
        )

    async def health(self, name: str) -> AgentHealth:
        """Return the cached health of one agent — never network I/O per call.

        Args:
            name: Agent to report on.

        Returns:
            The last state the background probe recorded, or a ``degraded``
            health with ``detail="probing"`` before its first pass completes.

        Raises:
            KeyError: When no agent is named *name*.
        """
        self._require_plan(name)
        return self._health.get(name, _PROBING)

    def _admit_sql_grants(self) -> None:
        """Mark every SQL grant live, aborting start-up when one's read-only state drifted.

        Raises:
            AgentCompilationError: With one issue per drifted grant.
        """
        grants = [
            capability
            for plan in self._plans.values()
            for capability in plan.capabilities
            if type(capability) is CompiledSqlCapability
        ]
        issues = [
            AgentErrorCode.SQL_READONLY_DRIFT.issue(connection=capability.connection)
            for capability in grants
            if self._sql_drifted(capability)
        ]
        if issues:
            raise AgentCompilationError(issues)
        self._live.update(capability.dependency_key for capability in grants)

    def _sql_drifted(self, capability: CompiledSqlCapability) -> bool:
        live = (
            None
            if self._sql_config is None
            else self._sql_config.connections.get(capability.connection)
        )
        return live is None or live.readonly != capability.config.readonly

    async def _open_clients(self, stack: AsyncExitStack, deadline: float) -> bool:
        """Open every live client concurrently, before the start-up deadline.

        Under ``ai.remote_clients: optional`` a client that does not connect is
        logged and dropped; a declared grant with no client factory is fatal
        under both values.

        Returns:
            Whether a connection failure was tolerated, so the caller knows the
            shared budget was spent on a server start-up proceeds without.

        Raises:
            AgentCompilationError: Aggregating every fatal start-up failure.
        """
        mcp, a2a = _remote_capabilities(self._plans.values())
        mcp = _merge_use_case_mcp(mcp, self._use_case_mcp)
        if not mcp and not a2a:
            return False
        opened: list[_OpenedClient] = []
        unreachable: list[AgentCompilationIssue] = []
        unwired: list[AgentCompilationIssue] = []
        try:
            async with asyncio.timeout_at(deadline), asyncio.TaskGroup() as group:
                for capability in mcp:
                    group.create_task(self._open_mcp(capability, opened, unreachable, unwired))
                for remote in a2a:
                    group.create_task(self._open_a2a(remote, opened, unreachable, unwired))
        except TimeoutError:
            unreachable.extend(self._timeout_issues(mcp, a2a, opened))
        finally:
            self._register_opened(stack, opened)
        tolerated = bool(unreachable) and self._tolerates_unreachable()
        if tolerated:
            _log_tolerated(unreachable)
            unreachable = []
        if unwired or unreachable:
            raise AgentCompilationError([*unwired, *unreachable])
        return tolerated

    def _resolve_invoker(self) -> ApplicationInvoker | None:
        """Resolve the application's invoker once; refuse start-up when it cannot serve the plans.

        Returns:
            The container's invoker, or ``None`` when it holds none and no plan
            declares a hook, a loader or a use case.

        Raises:
            AgentCompilationError: ``INVOKER_BOUND`` when the invoker is bound
                to an identity; ``ON_OUTPUT_INVOKER_MISSING``,
                ``CONVERSATION_INVOKER_MISSING`` and ``USECASE_INVOKER_MISSING``
                naming the agents that need the missing invoker.
        """
        invoker = cast(
            ApplicationInvoker | None, self._container.resolve_or_none(ApplicationInvoker)
        )
        if invoker is None:
            self._refuse_plans_needing_an_invoker()
        elif getattr(invoker, "identity", None) is not None:
            raise AgentCompilationError([AgentErrorCode.INVOKER_BOUND.issue()])
        return invoker

    def _refuse_plans_needing_an_invoker(self) -> None:
        plans = self._plans.items()
        needs = {
            AgentErrorCode.ON_OUTPUT_INVOKER_MISSING: [
                name for name, plan in plans if plan.on_output is not None
            ],
            AgentErrorCode.CONVERSATION_INVOKER_MISSING: [
                name for name, plan in plans if plan.conversation is not None
            ],
            AgentErrorCode.USECASE_INVOKER_MISSING: [
                name for name, plan in plans if plan.grants_use_cases
            ],
        }
        issues = [
            code.issue(agents=agents, reason=INVOKER_MISSING_REASON)
            for code, agents in needs.items()
            if agents
        ]
        if issues:
            raise AgentCompilationError(issues)

    def _verify_mcp_connections(self) -> None:
        """Refuse two grants of one server name that describe different connections.

        Checked before any client opens.

        Raises:
            AgentCompilationError: Naming the server and both agents.
        """
        issues = connection_conflicts(self._plans.values())
        if issues:
            raise AgentCompilationError(issues)

    def _tolerates_unreachable(self) -> bool:
        """Report whether ``ai.remote_clients`` tolerates a failed connection."""
        return self._config.remote_clients == _OPTIONAL_REMOTE_CLIENTS

    def _startup_deadline(self) -> float:
        """Return an absolute deadline one whole ``startup_timeout_ms`` away."""
        return asyncio.get_running_loop().time() + self._config.startup_timeout_ms / 1000

    async def _open_mcp(
        self,
        capability: CompiledMcpCapability,
        opened: list[_OpenedClient],
        unreachable: list[AgentCompilationIssue],
        unwired: list[AgentCompilationIssue],
    ) -> None:
        factory = self._mcp_client_factory
        if factory is None:
            unwired.append(
                AgentErrorCode.MCP_SERVER_UNREACHABLE.issue(
                    server=capability.server, reason="no MCP client factory is configured"
                )
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:
            unreachable.append(
                AgentErrorCode.MCP_SERVER_UNREACHABLE.issue(
                    server=capability.server, reason=ErrorSummary.message_for(exc)
                )
            )
            return
        opened.append(_OpenedClient(key=capability.dependency_key, client=client, session=session))

    async def _open_a2a(
        self,
        capability: CompiledA2ACapability,
        opened: list[_OpenedClient],
        unreachable: list[AgentCompilationIssue],
        unwired: list[AgentCompilationIssue],
    ) -> None:
        factory = self._a2a_client_factory
        if factory is None:
            unwired.append(
                AgentErrorCode.A2A_AGENT_UNREACHABLE.issue(
                    agent=capability.agent, reason="no A2A client factory is configured"
                )
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:
            unreachable.append(
                AgentErrorCode.A2A_AGENT_UNREACHABLE.issue(
                    agent=capability.agent, reason=ErrorSummary.message_for(exc)
                )
            )
            return
        opened.append(_OpenedClient(key=capability.dependency_key, client=client, session=session))

    def _register_opened(self, stack: AsyncExitStack, opened: Sequence[_OpenedClient]) -> None:
        for entry in opened:
            stack.push_async_exit(entry.client)
            self._live.add(entry.key)
            if entry.key.startswith("mcp:"):
                session: McpSession = entry.session  # type: ignore[assignment]
                self._sessions[entry.key] = mcp_session_for(session, label=entry.key)

    def _timeout_issues(
        self,
        mcp: Sequence[CompiledMcpCapability],
        a2a: Sequence[CompiledA2ACapability],
        opened: Sequence[_OpenedClient],
    ) -> list[AgentCompilationIssue]:
        """Name every server whose connection did not complete in the budget."""
        reason = f"connection did not complete within {self._config.startup_timeout_ms} ms"
        live = {entry.key for entry in opened}
        issues: list[AgentCompilationIssue] = [
            AgentErrorCode.MCP_SERVER_UNREACHABLE.issue(server=capability.server, reason=reason)
            for capability in mcp
            if capability.dependency_key not in live
        ]
        issues.extend(
            AgentErrorCode.A2A_AGENT_UNREACHABLE.issue(agent=remote.agent, reason=reason)
            for remote in a2a
            if remote.dependency_key not in live
        )
        return issues

    async def _verify_tool_filters(self, deadline: float) -> None:
        """Apply every declared tool filter to the tools really offered.

        Tools are listed once per shared session and kept on
        ``self._tool_catalog``. Under ``ai.remote_clients: optional`` only a
        server that never connected is skipped; a listing timing out aborts
        start-up.

        Args:
            deadline: Absolute loop time the whole listing must complete by.

        Raises:
            AgentCompilationError: When a listing does not complete in the
                budget, or a declared filter matches no offered tool.
        """
        targets = [*filter_targets(self._plans.values()), *self._use_case_filter_targets()]
        if not targets:
            return
        listed: dict[str, tuple[McpToolInfo, ...]] = {}
        try:
            async with asyncio.timeout_at(deadline):
                await self._list_tools_once(targets, listed)
        except TimeoutError:
            raise AgentCompilationError(listing_timeout_issues(targets, listed)) from None
        finally:
            self._tool_catalog.update(listed)
        issues = filter_issues(targets, listed)
        issues.extend(self._use_case_filter_issues())
        if issues:
            raise AgentCompilationError(issues)

    def _use_case_filter_targets(self) -> tuple[FilterTarget, ...]:
        """One unfiltered target per declaring use case's server.

        Each server is listed even with no agent plan; the marker's own
        ``include`` is checked by :meth:`_use_case_filter_issues`.
        """
        return tuple(
            FilterTarget(
                agent=grant.usecase,
                server=grant.capability.server,
                key=grant.capability.dependency_key,
                include=(),
                exclude=(),
            )
            for grant in self._use_case_mcp
        )

    def _use_case_filter_issues(self) -> list[AgentCompilationIssue]:
        """One issue per marker whose own ``include`` matches no listed tool.

        A server that was never listed is skipped.
        """
        issues: list[AgentCompilationIssue] = []
        for grant in self._use_case_mcp:
            catalogue = self._tool_catalog.get(grant.capability.dependency_key)
            if catalogue is None:
                continue
            names = tuple(tool.name for tool in catalogue)
            if not select_names(
                names, include=grant.capability.include, exclude=grant.capability.exclude
            ):
                issues.append(
                    AgentErrorCode.USECASE_TOOL_FILTER_MATCHES_NOTHING.issue(
                        component=grant.usecase,
                        parameter=grant.parameter,
                        server=grant.capability.server,
                    )
                )
        return issues

    async def _list_tools_once(
        self, targets: Sequence[FilterTarget], listed: dict[str, tuple[McpToolInfo, ...]]
    ) -> None:
        """List the tools of every session a declared filter applies to, once per session."""
        for target in targets:
            session = self._sessions.get(target.key)
            if session is None or target.key in listed:
                continue
            listed[target.key] = await session.list_tools()

    def _build_engines(self) -> None:
        """Build one engine per plan, exactly once per worker."""
        for name, plan in self._plans.items():
            engine = self._engine_plugin.create_engine(
                plan, deps=self._deps, container=self._container
            )
            self._slots[name] = _AgentSlot(plan=plan, engine=engine)

    def _start_health_probe(self, stack: AsyncExitStack) -> None:
        """Start the single owned probe refreshing the declared health cache."""
        probe = asyncio.create_task(self._probe_forever(), name="loom-agent-health-probe")
        stack.push_async_callback(cancel_task, probe)

    async def _probe_forever(self) -> None:
        """Refresh the health cache forever; only cancellation ends this task.

        An agent whose probe fails is recorded as ``unavailable``.
        """
        period = max(self._config.health_cache_ttl_ms, 1) / 1000
        while True:
            for name in tuple(self._slots):
                self._health[name] = await self._probe_or_unavailable(name)
            await asyncio.sleep(period)

    async def _probe_or_unavailable(self, name: str) -> AgentHealth:
        """Probe one agent, reporting a failing probe as ``unavailable``."""
        try:
            return await self._probe(name)
        except Exception:
            _logger.exception(AiEvent.HEALTH_PROBE_FAILED, extra={AiKey.AGENT: name})
            return AgentHealth(status=HealthState.UNAVAILABLE, detail=_PROBE_FAILED)

    async def _probe(self, name: str) -> AgentHealth:
        slot = self._slots[name]
        engine_status = await slot.engine.health()
        checks: dict[str, HealthState] = {"model": engine_status.status}
        for capability in slot.plan.capabilities:
            key = capability.dependency_key
            if key is not None:
                checks[key] = HealthState.OK if key in self._live else HealthState.UNAVAILABLE
        return AgentHealth(
            status=HealthState.worst(checks.values()), checks=MappingProxyType(checks)
        )

    @asynccontextmanager
    async def _run_stream(
        self,
        name: str,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation_id: str | None,
        output_type: type[Any] | None = None,
        state: object | None = None,
    ) -> AsyncIterator[AsyncIterator[AgentEvent]]:
        slot = self._require_slot(name)
        _check_conversation_id(conversation_id)
        resolved_state = _resolve_state(name, slot.plan.state, state)
        async with self._chain_bound(name), self._admitted(name):
            with ExecutionScope(identity):
                callbacks = TurnCallbacks(
                    plan=slot.plan,
                    identity=identity,
                    interaction_id=uuid4().hex,
                    conversation_id=conversation_id,
                    invoker=self._invoker,
                )
                conversation = await callbacks.conversation()
                engine_stream = _open_engine_stream(
                    slot.engine,
                    prompt,
                    identity=identity,
                    conversation=conversation,
                    output_type=output_type,
                    state=resolved_state,
                )
                async with engine_stream as events:
                    supervised = supervised_events(events, slot.plan.policies)
                    hooked = callbacks.events(supervised)
                    try:
                        yield hooked
                    finally:
                        await hooked.aclose()
                        await supervised.aclose()

    @asynccontextmanager
    async def _chain_bound(self, name: str) -> AsyncIterator[None]:
        """Enter *name* on the call chain for the body, and always leave it after."""
        prior_chain = enter_agent_call(name, max_depth=self._config.max_agent_depth)
        try:
            yield
        finally:
            exit_agent_call(prior_chain)

    @asynccontextmanager
    async def _admitted(self, name: str) -> AsyncIterator[None]:
        """Take a run slot for the body, and always release it after."""
        await self._admit(name)
        try:
            yield
        finally:
            self._runs.release()

    async def _admit(self, name: str) -> None:
        """Take a run slot, refusing instead of queueing when none is free."""
        if self._runs.locked():
            raise AgentRunError(
                AgentRunErrorCode.TOO_MANY_RUNS,
                (
                    f"agent {name!r}: this worker already serves its "
                    f"max_concurrent_runs ({self._config.max_concurrent_runs})"
                ),
            )
        await self._runs.acquire()

    def state_shape(self, name: str) -> StateShape | None:
        """Return one agent's declared state shape.

        Read by :func:`~loom.ai.fastapi.endpoints._decode_state` to parse a
        request's raw ``state`` bytes against the artefact's own shape, and available
        before this host is entered — a compiled plan already carries its shape,
        unlike a grant, which is resolved only once the host opens its clients.

        Args:
            name: Agent whose declared shape is read.

        Returns:
            The plan's :class:`~loom.ai.abc.StateShape`, or ``None`` for an
            artefact declaring neither ``deps_type`` nor ``deps_schema``.

        Raises:
            KeyError: When no agent is named *name*.
        """
        return self._require_plan(name).state

    def output_type(self, name: str) -> LoomType:
        """Return one agent's compiled boundary type for its answer.

        Read at every wire surface — ``/run``, the SSE ``final`` frame and
        both A2A paths — so the answer is projected to builtins by the
        transport helper, never decoded again: the engine already validated
        and built it once at run time.

        Args:
            name: Agent whose output type is read.

        Returns:
            The plan's :class:`~loom.core.model.LoomType` for the answer.

        Raises:
            KeyError: When no agent is named *name*.
        """
        return self._require_plan(name).output.loom_type

    def grants(self, name: str) -> AgentGrants:
        """Return one agent's own resolved grants, built once at start-up.

        Args:
            name: Agent whose grants are read.

        Returns:
            The immutable :class:`~loom.ai.execution._grants.AgentGrants` this
            host resolved for *name* when it was entered.

        Raises:
            KeyError: When no agent is named *name*.
        """
        self._require_plan(name)
        return self._grants[name]

    def native(self, name: str, *, identity: Identity, state: object | None = None) -> object:
        """Return one agent's engine-native objects, outside this host's supervision.

        Serves :meth:`~loom.ai.abc.AgentHandle.native`; see it for what a run
        driven through the returned objects keeps and what it loses.

        Args:
            name: Agent whose engine is asked for its native form.
            identity: Verified caller the dependency bundle is built for;
                informational.
            state: This run's state, resolved against *name*'s declared shape
                exactly as :meth:`run` resolves it.

        Returns:
            The engine's own objects, in the engine's own types.

        Raises:
            KeyError: When no agent is named *name*.
            RuntimeError: When this runtime was never entered.
            AgentRunError: With ``STATE_UNDECLARED`` or ``STATE_REQUIRED``
                when *state* does not match *name*'s declared shape.
            NotImplementedError: When the engine serving *name* declares no
                ``native``.
        """
        slot = self._require_slot(name)
        resolved_state = _resolve_state(name, slot.plan.state, state)
        if not isinstance(slot.engine, NativeCapableEngine):
            raise NotImplementedError(
                f"{type(slot.engine).__name__} publishes no native form: it declares no 'native'"
            )
        return slot.engine.native(
            identity=identity, state=resolved_state, guard=self._native_guard(name)
        )

    def _native_guard(self, name: str) -> Callable[[], AbstractAsyncContextManager[None]]:
        """Build the guard a hand-driven run of *name* enters to rejoin this host's bounds.

        The guard enters the call chain and takes an admission slot in
        :meth:`_run_stream`'s order, then runs its body as the identity of the
        execution in progress where it is entered.

        Args:
            name: Agent the hand-driven run belongs to.

        Returns:
            A zero-argument factory of the guard.

        Raises:
            ExecutionRequired: When the guard is entered outside any execution.
            Unauthenticated: When it is entered with no caller.
            DelegationExpired: When it is entered past the caller's deadline.
            AgentRunError: With ``UNAUTHORIZED`` when the identity acting is
                not authenticated.
        """

        @asynccontextmanager
        async def _guard() -> AsyncIterator[None]:
            require_authenticated(f"agent {name!r}")
            async with self._chain_bound(name), self._admitted(name):
                yield

        return _guard

    def _build_grants(self) -> dict[str, AgentGrants]:
        """Resolve every plan's grants once, right after its engine is built."""
        return {name: self._plan_grants(plan) for name, plan in self._plans.items()}

    def _build_use_case_grants(self) -> dict[str, McpGrant | None]:
        """Resolve the one shared substrate grant per server ``Mcp()`` markers declared.

        Each grant carries the live session and the server's full, unfiltered
        catalogue, its own capability built through :func:`mcp_connection`
        (``include``/``exclude`` empty); every binding applies its own filter.

        Returns:
            The grant of each declared server by name, ``None`` for a server
            tolerated unreachable.
        """
        grants: dict[str, McpGrant | None] = {}
        for entry in self._use_case_mcp:
            server = entry.capability.server
            if server not in grants:
                grants[server] = self._mcp_grant(mcp_connection(entry.capability))
        return grants

    def use_case_mcp_grant(self, server: str) -> McpGrant | None:
        """Return the shared substrate grant of one server ``Mcp()`` markers declared.

        The grant carries the live session and the full, unfiltered catalogue
        every binding on *server* shares; a binding's own filter is its own.

        Args:
            server: MCP server name a marker declared.

        Returns:
            The shared grant, or ``None`` when the server was tolerated
            unreachable under ``ai.remote_clients: optional``.

        Raises:
            RuntimeError: When the host was never entered.
            AgentRunError: With ``MCP_SERVER_NOT_GRANTED`` when no ``Mcp()``
                declaration reached at startup names *server*.
        """
        if self._stack is None:
            raise InvalidUse(
                "AgentHost must be entered before use: wrap it in "
                "'async with host:' to open its clients and build its engines"
            )
        try:
            return self._use_case_grants[server]
        except KeyError:
            raise AgentRunError(
                AgentRunErrorCode.MCP_SERVER_NOT_GRANTED,
                f"mcp server {server!r} is not granted to any use case: no Mcp() "
                "declaration reached at startup names it",
            ) from None

    def _plan_grants(self, plan: AgentPlan) -> AgentGrants:
        """Resolve one plan's ``mcp``/``sql`` grants against the clients start-up opened."""
        mcp: dict[str, McpGrant] = {}
        sql: dict[str, CompiledSqlCapability] = {}
        mcp_names: list[str] = []
        for capability in plan.capabilities:
            if type(capability) is CompiledMcpCapability:
                mcp_names.append(capability.server)
                grant = self._mcp_grant(capability)
                if grant is not None:
                    mcp[capability.server] = grant
            elif type(capability) is CompiledSqlCapability:
                sql[capability.connection] = capability
        hook = plan.on_output
        return AgentGrants(
            mcp=MappingProxyType(mcp),
            sql=MappingProxyType(sql),
            mcp_names=tuple(mcp_names),
            tool_timeout_s=plan.policies.tool_timeout_ms / 1000,
            output_shape_bound=hook is not None and HOOK_OUTPUT_FIELD in hook.accepted,
        )

    def _mcp_grant(self, capability: CompiledMcpCapability) -> McpGrant | None:
        """Pair one compiled ``mcp`` capability with its live session, if one opened.

        ``None`` under ``ai.remote_clients: optional`` for a server whose
        connection was tolerated as unreachable.
        """
        key = capability.dependency_key
        session = self._sessions.get(key)
        if session is None:
            return None
        return McpGrant(
            capability=capability, session=session, catalogue=self._tool_catalog.get(key, ())
        )

    def _require_plan(self, name: str) -> AgentPlan:
        plan = self._plans.get(name)
        if plan is None:
            raise UnknownKey("the host declares no agent with that name")
        return plan

    def _require_slot(self, name: str) -> _AgentSlot:
        self._require_plan(name)
        slot = self._slots.get(name)
        if slot is None:
            raise InvalidUse(
                "AgentHost must be entered before use: wrap it in "
                "'async with host:' to open its clients and build its engines"
            )
        return slot


def _open_engine_stream(
    engine: AgentEngine,
    prompt: Prompt,
    *,
    identity: Identity,
    conversation: Conversation | None,
    output_type: type[Any] | None,
    state: Mapping[str, Any] | None,
) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
    """Open the engine's event stream, shaped for this call when asked.

    :class:`~loom.ai.abc.AgentEngine` itself takes no ``output_type``: its
    ``run``/``run_stream`` signature is pinned to exactly ``prompt``,
    ``identity``, ``conversation`` and ``state`` (see the public-surface test
    that enforces it). An engine that wants to serve
    :meth:`~loom.ai.abc.AgentHandle.run`'s ``expect`` and ``run_text`` opts in
    through the separate, optional ``run_stream_shaped`` method instead, read
    with ``getattr`` the way :data:`~loom.ai.abc.NativeToolSupport` is.

    Raises:
        NotImplementedError: When *output_type* is given and the engine
            declares no ``run_stream_shaped``. Not expected in a deployment
            running the pydantic-ai engine, which implements it.
    """
    if output_type is None:
        return engine.run_stream(prompt, identity=identity, conversation=conversation, state=state)
    shaped = getattr(engine, "run_stream_shaped", None)
    if shaped is None:
        raise NotImplementedError(
            f"{type(engine).__name__} does not support a per-run output shape: "
            "it declares no 'run_stream_shaped'"
        )
    return shaped(  # type: ignore[no-any-return]
        prompt,
        identity=identity,
        conversation=conversation,
        output_type=output_type,
        state=state,
    )


def _resolve_state(
    name: str, shape: StateShape | None, state: object | None
) -> Mapping[str, Any] | None:
    """Resolve one run's ``state`` against *shape*, before any engine call.

    Args:
        name: Agent this run targets, named in a raised error.
        shape: The agent's declared state shape, or ``None``.
        state: Caller-supplied state, already decoded and normalised, or ``None``.

    Returns:
        :meth:`StateShape.resolve`'s state; ``None`` with no shape and no state.

    Raises:
        AgentRunError: ``STATE_UNDECLARED`` when *state* is given and *shape*
            is ``None``; ``STATE_REQUIRED`` as :meth:`StateShape.resolve` raises it.
    """
    if shape is not None:
        return shape.resolve(name, state)
    if state is not None:
        raise AgentRunError(
            AgentRunErrorCode.STATE_UNDECLARED,
            f"agent {name!r} declares no state (no 'deps_type' or 'deps_schema'); "
            "remove 'state' from this call or declare a state shape on the artefact",
        )
    return None


def _check_conversation_id(conversation_id: str | None) -> None:
    """Refuse an out-of-bound ``conversation_id``: a programming error, not a run failure."""
    if conversation_id is None:
        return
    if not 1 <= len(conversation_id) <= CONVERSATION_ID_MAX_LENGTH:
        raise InvalidValue(
            f"conversation_id must be between 1 and {CONVERSATION_ID_MAX_LENGTH} characters long"
        )


def _log_tolerated(issues: Iterable[AgentCompilationIssue]) -> None:
    """Report the clients start-up went on without, without leaking their address.

    Only the stable code and the registered name reach WARNING: the issue's
    message carries a reason built from ``str(exc)`` of an arbitrary transport,
    which can name a URL, and under ``ai.remote_clients: optional`` it would
    otherwise land in routine logs on every boot. The reason stays available at
    DEBUG, where an operator asks for it deliberately.

    Args:
        issues: The tolerated connection failures, one per client.
    """
    for issue in issues:
        _logger.warning(
            AiEvent.REMOTE_CLIENT_SKIPPED,
            extra={AiKey.COMPONENT: issue.component, LogField.ERROR_CODE: str(issue.code)},
        )


def _remote_capabilities(
    plans: Iterable[AgentPlan],
) -> tuple[tuple[CompiledMcpCapability, ...], tuple[CompiledA2ACapability, ...]]:
    """Return one MCP and one A2A capability per registered name across every plan.

    Clients are shared per worker, not per agent and never per call,
    so two agents naming the same server open a single connection. The name is
    the unit of sharing, not the URL: two entries of ``ai.mcp_servers`` may
    legitimately share a host while differing in credential reference or
    deadline.

    Args:
        plans: Compiled plans of this worker.

    Returns:
        The de-duplicated MCP capabilities and A2A capabilities.
    """
    mcp: dict[str, CompiledMcpCapability] = {}
    a2a: dict[str, CompiledA2ACapability] = {}
    for plan in plans:
        for capability in plan.capabilities:
            if type(capability) is CompiledMcpCapability:
                mcp.setdefault(capability.server, capability)
            elif type(capability) is CompiledA2ACapability:
                a2a.setdefault(capability.agent, capability)
    return tuple(mcp.values()), tuple(a2a.values())


def _merge_use_case_mcp(
    mcp: tuple[CompiledMcpCapability, ...], use_case_mcp: Sequence[UseCaseMcpGrant]
) -> tuple[CompiledMcpCapability, ...]:
    """Fold each declaring use case's own server into the set the host opens.

    One capability per server name; both sources open the same client.

    Args:
        mcp: MCP capabilities every compiled agent plan declares, de-duplicated.
        use_case_mcp: Every verified ``Mcp()`` marker binding.

    Returns:
        *mcp* with one capability appended per use-case-only server name.
    """
    merged: dict[str, CompiledMcpCapability] = {capability.server: capability for capability in mcp}
    for grant in use_case_mcp:
        merged.setdefault(grant.capability.server, grant.capability)
    return tuple(merged.values())
