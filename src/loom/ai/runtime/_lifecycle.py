"""One entered runtime lifecycle: shared clients opened once, engines built once.

Mirrors :class:`~loom.core.sql.clickhouse.registry.ClickHouseConnectionRegistry`:
the runtime exists only between ``__aenter__`` and ``__aexit__``, so there is no
intermediate started/stopped state. Entering opens every live client the plans
declare — concurrently — through a single
:class:`~contextlib.AsyncExitStack` owned by the entering task, validates the
declared tool filters against the tools each server really exposes, re-verifies
the read-only state of every SQL grant against live configuration, and builds
one engine per plan. Connecting and validating share a single absolute
deadline, so ``startup_timeout_ms`` bounds the whole of start-up once, whatever
the number of servers -- unless ``ai.remote_clients`` is ``optional`` and a
connection failure was tolerated, in which case the validation pass is given a
fresh budget rather than the one the unreachable server spent. Leaving closes
everything in strict reverse order.
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
    AgentEngineProvider,
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
    AgentCompilationError,
    AgentCompilationIssue,
    AgentRunError,
    AgentRunErrorCode,
    a2a_agent_unreachable,
    conversation_invoker_missing,
    mcp_server_unreachable,
    on_output_invoker_missing,
    sql_readonly_drift,
    use_case_tool_filter_matches_nothing,
)
from loom.ai.runtime._bounded import RunContext
from loom.ai.runtime._chain import enter_agent_call, exit_agent_call
from loom.ai.runtime._conversation import load_conversation
from loom.ai.runtime._grants import AgentGrants, McpGrant
from loom.ai.runtime._health import AgentHealth, worst
from loom.ai.runtime._hooks import hooked_events, no_terminal_message
from loom.ai.runtime._limits import cancel_task, supervised_events
from loom.ai.runtime._mcp import (
    FilterTarget,
    McpClientFactory,
    connection_conflicts,
    filter_issues,
    filter_targets,
    listing_timeout_issues,
    mcp_key,
    mcp_session_for,
)
from loom.core.di import LoomContainer
from loom.core.identity import ANONYMOUS, Identity
from loom.core.model import BoundaryValidationError, LoomType
from loom.core.sql.config import SqlConfig
from loom.core.use_case.invoker import ApplicationInvoker

_logger = logging.getLogger(__name__)


A2AClientFactory = Callable[[CompiledA2ACapability], AbstractAsyncContextManager[object]]
"""Builds the (not yet opened) client of one compiled A2A capability."""

_PROBING = AgentHealth(status="degraded", detail="probing")

_PROBE_FAILED = "the health probe failed; the detail is recorded server-side"
"""Detail of an agent whose engine probe raised. No exception text: the probe
reaches a model provider, so its failures carry endpoints and credential
references that an anonymous ``/health`` scrape must never receive."""

_INVOKER_UNBOUND = "the use-case invoker is not bound to a caller"
"""Probe reason when the bundle's invoker was never bound to a caller."""

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
    """One verified ``Mcp()`` marker binding, compiled into a runtime input.

    Built by :mod:`loom.rest.fastapi.auto` (``_resolve_ai``) once the marker's
    server name has already been verified against ``ai.mcp_servers``, and
    handed to :class:`AgentRuntime` so the server it names joins the set the
    runtime opens even when no agent plan declares it. ``capability`` carries
    this binding's own ``include`` — read only by :meth:`_use_case_filter_issues`,
    to check it at start-up against the server's real tool list. At call
    time the resolver never reads this ``include`` back: it receives its own
    from the resolving :class:`~loom.core.engine.plan.McpBinding`, via the
    executor, which is why one instance of this class exists per binding
    rather than per server, even though start-up and call time end up
    checking the same value on two different objects. The *shared*
    grant a server's live session and full catalogue are read from is a
    separate, server-keyed :class:`~loom.ai.runtime._grants.McpGrant` built by
    :meth:`_build_use_case_grants`, whose own capability carries no ``include``
    at all. ``usecase`` names the declaring use case, and is also carried
    into :class:`FilterTarget.agent` for the server this binding lists on its
    own (:meth:`_use_case_filter_targets`) — carried, not read: nothing on
    that path reads the field back. ``parameter`` is read in one place only:
    to name the offending signature when the marker's own ``include`` matches
    no tool the server publishes.
    """

    capability: CompiledMcpCapability
    usecase: str
    parameter: str


def _dependency_key(capability: object) -> str | None:
    """Return the health-check key of a capability with a live dependency."""
    if type(capability) is CompiledMcpCapability:
        return mcp_key(capability)
    if type(capability) is CompiledA2ACapability:
        return _a2a_key(capability)
    if type(capability) is CompiledSqlCapability:
        return f"sql:{capability.connection}"
    return None


class AgentRuntime:
    """Owns the live agents of one worker: clients, engines and their limits.

    Usable only as an async context manager, and only from the task that
    entered it: every live client is opened through one
    :class:`~contextlib.AsyncExitStack` created in ``__aenter__``, so closing
    happens in strict reverse order in the same task. That is what keeps a
    session-affine client (MCP over a framed transport) from being closed by a
    task that never opened it.

    Args:
        plans: Compiled plans this worker serves.
        config: Deployment configuration of the AI pillar.
        engine_provider: Provider building one engine per plan, exactly once.
        deps: Per-invocation dependency factory handed to every engine.
        container: Application container the engines resolve services from.
        sql_config: Live ``sql:`` configuration, re-verified at start-up
            against what the plans were compiled against (FR-046).
        mcp_client_factory: Builds the client of one MCP capability. Required
            when any plan declares an ``mcp`` capability: without it the
            declared tool filters cannot be validated, so start-up fails
            closed rather than serving unvalidated grants. A missing factory is
            a wiring bug, not an offline network, so ``ai.remote_clients:
            optional`` does not tolerate it either.
        a2a_client_factory: Builds the client of one A2A capability, with the
            same fail-closed rule.
        use_case_mcp: Every ``Mcp()`` marker binding a declaring use case
            carries, already verified and compiled — see
            :class:`UseCaseMcpGrant`. Each one's server joins the set of
            clients this runtime opens, even when no agent plan names it. A
            server named only this way stays outside the background health
            probe: this runtime never reports it as an ``/health`` check
            entry — see :meth:`_probe_forever`, which iterates the per-plan
            slots. What a caller observes when such a server is down is
            decided by
            :func:`~loom.ai.runtime._handle.mcp_marker_resolver`: a
            tolerated-unreachable server resolves to a handle whose every
            call raises ``TOOL_UNAVAILABLE``, never a bind-time failure.

    Runs emit no span of their own: the transport owns observability, because
    only it knows the route, the method and the status code a run is attributed
    to. Over HTTP that owner is
    :func:`~loom.ai.fastapi.endpoints.bind_agent_endpoints`.

    Raises:
        AgentCompilationError: From ``__aenter__``, aggregating every start-up
            failure — an unreachable server (named as the deployment
            registered it, never by URL; tolerated under
            ``ai.remote_clients: optional``), a tool filter
            matching nothing, two grants of one MCP server name describing
            different connections, or a SQL connection whose read-only state
            drifted.

    Example::

        async with AgentRuntime(
            plans=plans, config=ai_config, engine_provider=provider,
            deps=deps, container=container,
        ) as runtime:
            result = await runtime.run("analyst", prompt, identity=identity)
    """

    def __init__(
        self,
        *,
        plans: Sequence[AgentPlan],
        config: AiConfig,
        engine_provider: AgentEngineProvider,
        deps: DepsFactory,
        container: LoomContainer,
        sql_config: SqlConfig | None = None,
        mcp_client_factory: McpClientFactory | None = None,
        a2a_client_factory: A2AClientFactory | None = None,
        use_case_mcp: Sequence[UseCaseMcpGrant] = (),
    ) -> None:
        self._plans: Mapping[str, AgentPlan] = MappingProxyType({p.name: p for p in plans})
        self._config = config
        self._engine_provider = engine_provider
        self._deps = deps
        self._container = container
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
        self._use_case_grants: dict[str, McpGrant] = {}
        self._runs = asyncio.Semaphore(config.max_concurrent_runs)

    async def __aenter__(self) -> Self:
        """Open every live client, validate the plans and build the engines.

        Returns:
            The entered runtime.

        Raises:
            RuntimeError: When the runtime was already entered.
            AgentCompilationError: Aggregating every start-up failure. Every
                client opened so far is closed before the error propagates.
        """
        if self._stack is not None:
            raise RuntimeError("AgentRuntime is already entered")
        stack = AsyncExitStack()
        self._stack = stack
        self._owner = asyncio.current_task()
        deadline = self._startup_deadline()
        try:
            self._verify_sql_readonly()
            self._verify_invoker()
            self._verify_mcp_connections()
            tolerated = await self._open_clients(stack, deadline)
            # A tolerated failure spent the shared budget on a server start-up
            # is proceeding without, so the filter pass gets a fresh one.
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
            RuntimeError: When the runtime was never entered, or when the
                exiting task is not the one that entered it — closing a
                session-affine client from a foreign task is a latent
                corruption, not a detail to paper over.
        """
        stack = self._stack
        if stack is None:
            raise RuntimeError("AgentRuntime was not entered")
        if asyncio.current_task() is not self._owner:
            raise RuntimeError(
                "AgentRuntime must be exited by the task that entered it: the live "
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
        """Every compiled plan this runtime serves, in the order they were given."""
        return tuple(self._plans.values())

    def agent_names(self) -> tuple[str, ...]:
        """Return the names of every agent this runtime serves.

        Returns:
            One name per compiled plan, in the order the plans were given.
        """
        return tuple(self._plans)

    def has_agent(self, name: str) -> bool:
        """Report whether an agent with that name is served by this runtime.

        Args:
            name: Agent name to look for.

        Returns:
            ``True`` when the runtime holds a plan with that name.
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
                instead of the plan's own declared output (T304); the plan's
                own output check does not run. ``None`` — the default — runs
                the plan's declared shape exactly as before this parameter
                existed. Not part of :class:`~loom.ai.abc.AgentEngine`'s own
                ``run``/``run_stream``, whose signature is pinned to exactly
                ``prompt``, ``identity`` and ``conversation``: an engine opts
                into shape overrides through the separate, optional
                ``run_stream_shaped`` capability instead (see
                :mod:`~loom.ai.runtime._grants`).
            state: This run's state, already decoded by whichever boundary
                received it and passed as a normalised mapping, or ``None``.
                Resolved against *name*'s declared shape before this call
                reaches the engine (FR-009, FR-010): ``None`` on a stateful
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
            # Defensive only: ``hooked_events`` closes every exhausted stream
            # with a named error, so this guard cannot be reached today.
            raise AgentRunError(AgentRunErrorCode.PROVIDER_UNAVAILABLE, no_terminal_message(name))
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

    # -- lifecycle ---------------------------------------------------------

    def _verify_invoker(self) -> None:
        """Abort start-up when a hook or a loader is declared but no bundle carries an invoker.

        Probed once, before any client opens: without it a misconfigured
        deployment would fail only after every paid run.
        """
        hooked = [name for name, plan in self._plans.items() if plan.on_output is not None]
        conversational = [
            name for name, plan in self._plans.items() if plan.conversation is not None
        ]
        if not hooked and not conversational:
            return
        reason = self._invoker_reason()
        if reason is None:
            return
        issues: list[AgentCompilationIssue] = []
        if hooked:
            issues.append(on_output_invoker_missing(hooked, reason=reason))
        if conversational:
            issues.append(conversation_invoker_missing(conversational, reason=reason))
        raise AgentCompilationError(issues)

    def _invoker_reason(self) -> str | None:
        """Return why the probed bundle's invoker is unusable, or ``None`` when it is fine."""
        invoker = getattr(self._deps.build(ANONYMOUS, self._container), "invoker", None)
        if not isinstance(invoker, ApplicationInvoker):
            return INVOKER_MISSING_REASON
        # An invoker built for a caller carries that caller (``ANONYMOUS`` here);
        # one carrying ``None`` was never bound and would run every use case as nobody.
        if getattr(invoker, "identity", ANONYMOUS) is None:
            return _INVOKER_UNBOUND
        return None

    def _verify_sql_readonly(self) -> None:
        """Abort start-up when a SQL grant's read-only state drifted (FR-046)."""
        issues = [
            sql_readonly_drift(capability.connection)
            for plan in self._plans.values()
            for capability in plan.capabilities
            if type(capability) is CompiledSqlCapability and self._sql_drifted(capability)
        ]
        if issues:
            raise AgentCompilationError(issues)

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
        logged and dropped instead of aborting start-up: the runtime serves the
        agents whose other dependencies are live, and the health probe reports
        the missing key ``unavailable`` for every server an agent declares
        (``_probe_forever`` iterates compiled plans only — a server named
        solely by a use case's ``Mcp()`` marker is outside its reach; see the
        ``use_case_mcp`` parameter of :class:`AgentRuntime`). A client that was
        never wired --- no factory for a declared grant --- is a deployment
        bug rather than an offline network, so it is collected apart, at the
        point the factory is found ``None``, and stays fatal under both
        values.

        Returns:
            Whether a connection failure was tolerated, so the caller knows the
            shared budget was spent on a server start-up proceeds without.

        Raises:
            AgentCompilationError: Aggregating every fatal start-up failure.
        """
        mcp, a2a = _remote_capabilities(self._plans.values())
        mcp = _fold_use_case_mcp(mcp, self._use_case_mcp)
        if not mcp and not a2a:
            return False
        opened: list[_OpenedClient] = []
        unreachable: list[AgentCompilationIssue] = []
        unwired: list[AgentCompilationIssue] = []
        try:
            async with asyncio.timeout_at(deadline):
                async with asyncio.TaskGroup() as group:
                    for capability in mcp:
                        group.create_task(self._open_mcp(capability, opened, unreachable, unwired))
                    for remote in a2a:
                        group.create_task(self._open_a2a(remote, opened, unreachable, unwired))
        except TimeoutError:
            unreachable.extend(self._timeout_issues(mcp, a2a, opened))
        finally:
            # Registered from the entering task, in completion order, so the
            # exit stack unwinds them in strict reverse order.
            self._register_opened(stack, opened)
        tolerated = bool(unreachable) and self._tolerates_unreachable()
        if tolerated:
            _log_tolerated(unreachable)
            unreachable = []
        if unwired or unreachable:
            raise AgentCompilationError([*unwired, *unreachable])
        return tolerated

    def _verify_mcp_connections(self) -> None:
        """Refuse two grants of one server name that describe different connections.

        The worker opens a single client per name and every agent granted it
        works over that one client, so a second grant carrying another URL or
        another credential would silently run against the first agent's
        connection. Checked before any client opens.

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
                mcp_server_unreachable(capability.server, "no MCP client factory is configured")
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:  # recovery: reported as a coded start-up issue
            unreachable.append(mcp_server_unreachable(capability.server, str(exc)))
            return
        opened.append(_OpenedClient(key=mcp_key(capability), client=client, session=session))

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
                a2a_agent_unreachable(capability.agent, "no A2A client factory is configured")
            )
            return
        client = factory(capability)
        try:
            session = await client.__aenter__()
        except Exception as exc:  # recovery: reported as a coded start-up issue
            unreachable.append(a2a_agent_unreachable(capability.agent, str(exc)))
            return
        opened.append(_OpenedClient(key=_a2a_key(capability), client=client, session=session))

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
            mcp_server_unreachable(capability.server, reason)
            for capability in mcp
            if mcp_key(capability) not in live
        ]
        issues.extend(
            a2a_agent_unreachable(remote.agent, reason)
            for remote in a2a
            if _a2a_key(remote) not in live
        )
        return issues

    async def _verify_tool_filters(self, deadline: float) -> None:
        """Apply every declared tool filter to the tools really offered (FR-025).

        Tools are listed once per shared session, never once per (plan,
        capability) pair: sessions are shared per server, so two plans pointing
        at the same server would otherwise pay two serialised round trips for
        identical data. The listing outlives this pass on ``self._tool_catalog``
        (T301): a grant handle's ``mcp()``/``tools()`` read it synchronously
        later, at no extra round trip, because ``MCPToolset.list_tools`` caches
        its own result.

        This pass never fails open. Under ``ai.remote_clients: optional`` the
        waiver covers only servers that never connected --- ``_list_tools_once``
        skips a server with no session and ``filter_issues`` skips a key that
        was never listed. A server that did open still has its filters
        verified, and its listing timing out still aborts start-up, which is
        why the caller hands this pass a fresh budget rather than the one an
        unreachable server exhausted.

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

        An empty ``include``/``exclude`` pair still gets the server *listed*
        (:func:`filter_targets`'s own contract) and is then skipped by
        :func:`filter_issues`; the marker's own ``include`` is checked
        separately, in :meth:`_use_case_filter_issues`, over the catalogue
        this listing fills.  Contributed here, unconditionally, rather than
        after ``filter_targets`` might return an empty tuple: a deployment
        with zero compiled agent plans and one declaring use case must still
        list this server, or the catalogue :meth:`_mcp_grant` reads from
        stays empty and every call the resolved handle makes fails with
        ``TOOL_UNKNOWN`` against a server that opened cleanly.
        """
        return tuple(
            FilterTarget(
                agent=grant.usecase,
                server=grant.capability.server,
                key=mcp_key(grant.capability),
                include=(),
                exclude=(),
            )
            for grant in self._use_case_mcp
        )

    def _use_case_filter_issues(self) -> list[AgentCompilationIssue]:
        """One issue per marker whose own ``include`` matches no listed tool.

        Not a :func:`filter_issues` variant: that function has no field to
        carry a parameter name, and building one issue shape from inside it
        would need a discriminant on :class:`FilterTarget`. A server that was
        never listed (a tolerated, unreachable one) is skipped here too — its
        outage is reported by the connection check, not this filter pass.
        """
        issues: list[AgentCompilationIssue] = []
        for grant in self._use_case_mcp:
            catalogue = self._tool_catalog.get(mcp_key(grant.capability))
            if catalogue is None:
                continue
            names = tuple(tool.name for tool in catalogue)
            if not select_names(
                names, include=grant.capability.include, exclude=grant.capability.exclude
            ):
                issues.append(
                    use_case_tool_filter_matches_nothing(
                        grant.usecase, grant.parameter, grant.capability.server
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
        """Build one engine per plan, exactly once per worker (FR-026)."""
        for name, plan in self._plans.items():
            engine = self._engine_provider.create_engine(
                plan, deps=self._deps, container=self._container
            )
            self._slots[name] = _AgentSlot(plan=plan, engine=engine)

    def _start_health_probe(self, stack: AsyncExitStack) -> None:
        """Start the single owned probe refreshing the declared health cache."""
        probe = asyncio.create_task(self._probe_forever(), name="loom-agent-health-probe")
        stack.push_async_callback(cancel_task, probe)

    async def _probe_forever(self) -> None:
        """Refresh the health cache forever; only cancellation ends this task.

        :meth:`~loom.ai.abc.AgentEngine.health` is a public protocol a third
        party implements, so it may raise anything. An escaping failure would
        end this task for good while ``/health`` kept answering the last cached
        ``ok`` — a dead probe reported as a healthy runtime. Instead the failing
        agent is recorded as ``unavailable`` and the loop moves on to the next.
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
        except Exception:  # recovery: a failed probe is a health state, not a crash
            _logger.exception("Health probe of agent %r failed", name)
            return AgentHealth(status="unavailable", detail=_PROBE_FAILED)

    async def _probe(self, name: str) -> AgentHealth:
        slot = self._slots[name]
        engine_status = await slot.engine.health()
        checks: dict[str, str] = {"model": engine_status.status}
        for capability in slot.plan.capabilities:
            key = _dependency_key(capability)
            if key is not None:
                checks[key] = self._dependency_state(key)
        return AgentHealth(status=worst(checks.values()), checks=MappingProxyType(checks))

    def _dependency_state(self, key: str) -> HealthState:
        if key.startswith("sql:"):
            return "ok"
        return "ok" if key in self._live else "unavailable"

    # -- runs --------------------------------------------------------------

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
            run = RunContext(
                plan=slot.plan,
                identity=identity,
                interaction_id=uuid4().hex,
                conversation_id=conversation_id,
            )
            conversation = await load_conversation(run, self._deps, self._container)
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
                hooked = hooked_events(supervised, run, self._deps, self._container)
                try:
                    yield hooked
                finally:
                    await hooked.aclose()
                    await supervised.aclose()

    @asynccontextmanager
    async def _chain_bound(self, name: str) -> AsyncIterator[None]:
        """Enter *name* on the call chain for the body, and always leave it after.

        Entered before :meth:`_admitted`: a run refused for a cycle or for
        exceeding ``max_agent_depth`` must never occupy a concurrency permit
        it will just give back. The default depth of one means this push
        alone already consumes the whole budget the top-level run gets — a
        use case nested underneath (an output hook, most concretely) that
        declares its own ``Agent()`` marker finds no depth left, by design.
        """
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
        # Never suspends: the guard above proved a permit is available.
        await self._runs.acquire()

    # -- grants (T301/T303) -------------------------------------------------
    #
    # Read by ``loom.ai.runtime._handle`` alone, never by application code:
    # an ``AgentHandle`` is the only public door onto a plan's granted
    # resources. One method, returning one immutable value per agent, rather
    # than the wider method-per-fact surface this used to be: a marker-driven
    # run reads ``grants(name)`` once instead of repeating a linear scan over
    # ``plan.capabilities`` — once per grant lookup, once per policy read —
    # on every call a use case makes through its handle.

    def state_shape(self, name: str) -> StateShape | None:
        """Return one agent's declared state shape.

        Read by :func:`~loom.ai.fastapi.endpoints._decode_state` to parse a
        request's raw ``state`` bytes against the artefact's own shape
        (FR-008), and available before this runtime is entered — a compiled
        plan already carries its shape, unlike a grant, which is resolved
        only once the runtime opens its clients.

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
            The immutable :class:`~loom.ai.runtime._grants.AgentGrants` this
            runtime resolved for *name* when it was entered.

        Raises:
            KeyError: When no agent is named *name*.
        """
        self._require_plan(name)
        return self._grants[name]

    def native(self, name: str, *, identity: Identity, state: object | None = None) -> object:
        """Return one agent's engine-native objects, outside this runtime's supervision.

        Serves :meth:`~loom.ai.abc.AgentHandle.native`; see it for what a run
        driven through the returned objects keeps and what it loses.

        Args:
            name: Agent whose engine is asked for its native form.
            identity: Verified caller the returned objects are bound to.
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
        """Build the guard a hand-driven run of *name* enters to rejoin this runtime's bounds.

        Composes :meth:`_chain_bound` and :meth:`_admitted` in the same order
        :meth:`_run_stream` does, so a run entered through the returned
        factory is subject to the same cycle, depth and admission bounds as
        one this runtime drives itself. Handed to the engine's own
        ``native()`` rather than entered here: the engine pairs it with its
        own objects, and :func:`~loom.ai.engines.pydantic_ai.native_agent`
        is the one place it is entered, around the whole hand-driven run.
        """

        @asynccontextmanager
        async def _guard() -> AsyncIterator[None]:
            async with self._chain_bound(name), self._admitted(name):
                yield

        return _guard

    def _build_grants(self) -> dict[str, AgentGrants]:
        """Resolve every plan's grants once, right after its engine is built."""
        return {name: self._plan_grants(plan) for name, plan in self._plans.items()}

    def _build_use_case_grants(self) -> dict[str, McpGrant]:
        """Resolve the one shared substrate grant per server, keyed by server name.

        This grant is **not** any one binding's filtered view: it carries the
        live session and the server's full, unfiltered catalogue, which is why
        its own capability is built through :func:`mcp_connection` — the same
        helper that keys a shared MCP client by connection identity — so
        ``include``/``exclude`` come back explicitly empty rather than
        whichever binding happened to be folded in last. Two use cases naming
        the same server can declare different ``include``s without either
        one's filter leaking into the other or into this shared grant: each
        binding's own filter is checked at start-up by
        :meth:`_use_case_filter_issues` and applied at call time from its
        own :class:`~loom.core.engine.plan.McpBinding`, via the executor —
        never from the value returned here.

        Built through the existing :meth:`_mcp_grant`, so a tolerated
        unreachable server yields ``None`` here exactly as it does for an
        agent's own grant. Keyed by server name alone: this is the shared
        substrate every binding on that server reads its session and
        catalogue from, so one entry per server is exactly right, not a
        limitation to work around.
        """
        grants: dict[str, McpGrant] = {}
        for entry in self._use_case_mcp:
            server = entry.capability.server
            if server in grants:
                continue
            grant = self._mcp_grant(mcp_connection(entry.capability))
            if grant is not None:
                grants[server] = grant
        return grants

    def use_case_mcp_grant(self, server: str) -> McpGrant | None:
        """Return the shared substrate grant of one server ``Mcp()`` markers declared.

        This is the server-keyed substrate — live session, full unfiltered
        catalogue — every binding on *server* shares; it is **not** any one
        caller's filtered view, and its own capability carries an explicitly
        empty ``include``/``exclude`` (see :meth:`_build_use_case_grants`). A
        caller's own filter comes from its own
        :class:`~loom.core.engine.plan.McpBinding`, passed to the resolver
        by the executor at resolution time, never from the value this
        method returns.

        Args:
            server: MCP server name a marker declared.

        Returns:
            The resolved shared grant, or ``None`` when *server* was never
            declared by any use case, or its connection was tolerated as
            unreachable under ``ai.remote_clients: optional``.

        Raises:
            RuntimeError: When the runtime was never entered. This follows
                ``_require_slot``'s own rule rather than calling it: that
                method calls ``_require_plan`` first, which would raise
                ``KeyError`` on a server name that is not an agent name.
        """
        if self._stack is None:
            raise RuntimeError(
                "AgentRuntime must be entered before use: wrap it in "
                "'async with runtime:' to open its clients and build its engines"
            )
        return self._use_case_grants.get(server)

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
        connection was tolerated as unreachable — the same case
        ``mcp_grant`` used to signal with its own ``None`` return.
        """
        key = mcp_key(capability)
        session = self._sessions.get(key)
        if session is None:
            return None
        return McpGrant(
            capability=capability, session=session, catalogue=self._tool_catalog.get(key, ())
        )

    def _require_plan(self, name: str) -> AgentPlan:
        plan = self._plans.get(name)
        if plan is None:
            raise KeyError(name)
        return plan

    def _require_slot(self, name: str) -> _AgentSlot:
        self._require_plan(name)
        slot = self._slots.get(name)
        if slot is None:
            raise RuntimeError(
                "AgentRuntime must be entered before use: wrap it in "
                "'async with runtime:' to open its clients and build its engines"
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
    """Resolve one run's ``state`` against *shape*, before any engine call (FR-009, FR-010).

    *state* is already decoded and normalised by whichever boundary received
    it — this never parses bytes, it only checks the value against the
    artefact's declared shape.

    Args:
        name: Agent this run targets, named in a raised error.
        shape: The agent's declared state shape, or ``None``.
        state: Caller-supplied state, or ``None``.

    Returns:
        *state* unchanged when given. When *state* is ``None`` and *shape*
        declares a :class:`~loom.core.model.LoomType` whose every field has a
        default, the shape's own defaults — its decode of an empty object,
        turned to builtins — so a stateful artefact renders its declared
        defaults rather than empty markers. ``None`` when *shape* is
        ``None``, or declares the open ``deps_type: dict`` form, which has no
        defaults to supply.

    Raises:
        AgentRunError: ``STATE_UNDECLARED`` when *state* is given and *shape*
            is ``None``; ``STATE_REQUIRED`` when *state* is ``None`` and
            *shape* declares a field with no default.
    """
    if state is not None:
        if shape is None:
            raise AgentRunError(
                AgentRunErrorCode.STATE_UNDECLARED,
                f"agent {name!r} declares no state (no 'deps_type' or 'deps_schema'); "
                "remove 'state' from this call or declare a state shape on the artefact",
            )
        return cast(Mapping[str, Any], state)
    if shape is None or shape.loom_type is None:
        return None
    try:
        decoded = shape.loom_type.decode_json(b"{}")
    except BoundaryValidationError as exc:
        raise AgentRunError(
            AgentRunErrorCode.STATE_REQUIRED,
            f"agent {name!r} declares a state field with no default ({exc}); "
            "pass 'state' explicitly for this run",
        ) from exc
    return cast(Mapping[str, Any], shape.loom_type.to_builtins(decoded))


def _check_conversation_id(conversation_id: str | None) -> None:
    """Refuse an out-of-bound ``conversation_id``: a programming error, not a run failure."""
    if conversation_id is None:
        return
    if not 1 <= len(conversation_id) <= CONVERSATION_ID_MAX_LENGTH:
        raise ValueError(
            f"conversation_id must be between 1 and {CONVERSATION_ID_MAX_LENGTH} "
            f"characters long, got {len(conversation_id)}"
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
            "start-up continued without a remote client: %s (%s)", issue.code, issue.component
        )
        _logger.debug("remote client %r was not opened: %s", issue.component, issue.message)


def _a2a_key(capability: CompiledA2ACapability) -> str:
    """Return the health-check key of one A2A capability, by registered name."""
    return f"a2a:{capability.agent}"


def _remote_capabilities(
    plans: Iterable[AgentPlan],
) -> tuple[tuple[CompiledMcpCapability, ...], tuple[CompiledA2ACapability, ...]]:
    """Return one MCP and one A2A capability per registered name across every plan.

    Clients are shared per worker, not per agent and never per call (FR-026),
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


def _fold_use_case_mcp(
    mcp: tuple[CompiledMcpCapability, ...], use_case_mcp: Sequence[UseCaseMcpGrant]
) -> tuple[CompiledMcpCapability, ...]:
    """Fold each declaring use case's own server into the set the runtime opens.

    Applied above ``_open_clients``'s ``if not mcp and not a2a: return False``
    guard: with zero compiled agent plans and a server named only by a use
    case, that guard would otherwise fire on the agent-only ``mcp`` tuple and
    nothing would ever open.

    Both sources compile from the same ``ai.mcp_servers`` entry for a given
    server name and differ only in ``include``/``exclude``. Neither field
    reaches anything on this path: the client is keyed by
    ``f"mcp:{server}"`` alone (:func:`~loom.ai.runtime._mcp.mcp_key`), and
    the factory that opens it never reads a filter — the filtering is a view
    built later, per grant. So whichever of the two capabilities wins opens
    the same client, and ``setdefault`` here is a de-duplication rule, not a
    precedence one, exactly like the one :func:`_remote_capabilities` already
    applies across plans.

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
