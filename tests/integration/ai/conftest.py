"""Fakes and builders shared by the Phase 5 (US3) agent runtime tests.

Nothing here touches the network, a credential or a token: every live client
is a local stub and every engine replays a fixed script.  The module
deliberately imports no symbol from ``loom.ai.runtime`` or
``loom.ai.fastapi``: a ``conftest`` import error aborts the whole session,
while the red state of the phase must come from each test module's own
import.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from types import TracebackType
from typing import Any, ClassVar

import msgspec
import pydantic
import pytest
from pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel

from loom.ai.abc import (
    AgentEvent,
    AgentHandle,
    AgentResult,
    AgentUsage,
    Conversation,
    ErrorEvent,
    FinalEvent,
    HealthStatus,
    McpToolCallResult,
    McpToolInfo,
    StateShape,
    TextDeltaEvent,
)
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledCapability,
    CompiledConversation,
    CompiledInstruction,
    CompiledMcpCapability,
    CompiledOutput,
    CompiledOutputHook,
    CompiledSqlCapability,
)
from loom.ai.config import (
    A2AAgentConfig,
    A2AConfig,
    AgentEndpointConfig,
    AiConfig,
    McpServerConfig,
)
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import create_a2a_client, create_mcp_client
from loom.ai.errors import AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.model import loom_type, msgspec_type
from loom.core.model.redaction import Dsn
from loom.core.observability.event import LifecycleEvent
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.use_case import Agent, Caller, Input, UseCase
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.keys import use_case_key
from loom.core.use_case.registry import UseCaseRegistry

DEFAULT_USAGE = AgentUsage(
    input_tokens=11,
    output_tokens=7,
    requests=1,
    duration_ms=3,
    cache_read_tokens=4,
    cache_write_tokens=2,
    tool_calls=1,
    cost=Decimal("0.0021"),
    details={"reasoning_tokens": 5},
)
"""Fixed usage every scripted terminal event carries.

Every counter is distinct and non-zero, including one that only exists in
``details``, so a surface that drops or confuses one of them fails a test
rather than passing on a shared default of ``0``.
"""

DEFAULT_OUTPUT: Mapping[str, Any] = {"answer": "42"}
"""Fixed decoded output the default script returns."""


class RecordingObserver:
    """Lifecycle observer keeping every event a span emitted, in order.

    Attributes:
        events: Every event received, in arrival order.
    """

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one lifecycle event."""
        self.events.append(event)


# ---------------------------------------------------------------------------
# MCP stubs
# ---------------------------------------------------------------------------


class RecordingMcpSession:
    """MCP session double that records every call it serves.

    Args:
        label: Name used in the shared lifecycle log.
        tools: Tool names the server claims to expose.
        schemas: Names, among ``tools``, that publish an output schema; the
            rest report none (T301).
        results: Structured content returned per tool name; missing names
            return ``None``. A name mapped to :data:`FAILED` reports the
            server's own error flag instead (T302).
        list_delay_ms: Time one ``list_tools`` round trip costs, so a test can
            express a start-up budget spent on listing rather than connecting.
    """

    def __init__(
        self,
        *,
        label: str = "stub",
        tools: Sequence[str] = ("alpha", "beta"),
        schemas: Sequence[str] = (),
        results: Mapping[str, object] | None = None,
        list_delay_ms: int = 0,
    ) -> None:
        self.label = label
        self.tools = tuple(tools)
        self.schemas = frozenset(schemas)
        self.results = dict(results or {})
        self.list_delay_ms = list_delay_ms
        self.calls: list[tuple[str, Mapping[str, Any]]] = []
        self.listed = 0

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the stub server exposes, each with its schema flag."""
        self.listed += 1
        if self.list_delay_ms:
            await asyncio.sleep(self.list_delay_ms / 1000)
        return tuple(
            McpToolInfo(name=name, has_output_schema=name in self.schemas) for name in self.tools
        )

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Record the invocation and return the scripted protocol-level result."""
        self.calls.append((name, dict(arguments)))
        outcome = self.results.get(name)
        if outcome is FAILED:
            return McpToolCallResult(ok=False, structured=None)
        return McpToolCallResult(ok=True, structured=outcome)


FAILED = object()
"""Sentinel: :attr:`RecordingMcpSession.results` maps a tool name to this to
script an ``is_error`` result instead of a structured one (T302)."""


class InterleavingSensitiveSession:
    """Session that returns the wrong answer if two calls ever interleave.

    Each call writes its token into a single shared slot, awaits a real
    suspension point and reads the slot back.  Serialised access returns each
    caller its own token; interleaved access returns the last writer's token
    to everybody, which is exactly the poisoned-session failure T078 guards.

    Args:
        delay_ms: Suspension inside the critical section, in milliseconds.
    """

    def __init__(self, *, delay_ms: int = 10) -> None:
        self._delay_s = delay_ms / 1000
        self._slot: str = ""
        self._busy = False
        self.interleaved = False
        self.started: list[str] = []
        self.completed: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return a fixed tool name; the poisoning test never filters."""
        return (McpToolInfo(name="echo", has_output_schema=False),)

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Echo ``arguments['token']`` back, unless two calls overlapped."""
        del name
        token = str(arguments["token"])
        if self._busy:
            self.interleaved = True
        self._busy = True
        self.started.append(token)
        self._slot = token
        try:
            await asyncio.sleep(self._delay_s)
            self.completed.append(token)
            return McpToolCallResult(ok=True, structured=self._slot)
        finally:
            self._busy = False


class StubMcpClient:
    """Async context manager standing in for one live MCP client.

    Args:
        label: Name recorded in ``log`` on open and on close.
        session: Session handed to the runtime once connected.
        log: Shared lifecycle log receiving ``open:<label>``/``close:<label>``.
        connect_delay_ms: Delay before the connection is considered open.
        never_connects: When true the connection never completes, standing in
            for an unreachable server.
        connect_error: When set, the connection fails with this text instead of
            completing, standing in for a server that refuses the connection.
    """

    def __init__(
        self,
        *,
        label: str,
        session: object,
        log: list[str],
        connect_delay_ms: int = 0,
        never_connects: bool = False,
        connect_error: str | None = None,
    ) -> None:
        self.label = label
        self.session = session
        self.log = log
        self.connect_delay_ms = connect_delay_ms
        self.never_connects = never_connects
        self.connect_error = connect_error

    async def __aenter__(self) -> object:
        """Connect the stub and record the open in the shared log."""
        if self.connect_error is not None:
            raise ConnectionError(self.connect_error)
        if self.never_connects:
            await asyncio.Event().wait()
        if self.connect_delay_ms:
            await asyncio.sleep(self.connect_delay_ms / 1000)
        self.log.append(f"open:{self.label}")
        return self.session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Record the close in the shared log."""
        self.log.append(f"close:{self.label}")


def mcp_client_factory(
    clients: Mapping[str, StubMcpClient],
) -> Callable[[CompiledMcpCapability], StubMcpClient]:
    """Build an ``McpClientFactory`` resolving a stub client per registered name.

    Args:
        clients: Stub client per ``CompiledMcpCapability.server``.

    Returns:
        The factory the runtime calls once per MCP capability.
    """

    def _factory(capability: CompiledMcpCapability) -> StubMcpClient:
        return clients[capability.server]

    return _factory


@dataclass
class CountingMcpClientFactory:
    """``McpClientFactory`` recording each call it serves, by server name.

    A shared open/close log (``lifecycle_log``) proves a client opened once;
    it does not prove the *factory callable* itself was invoked only once,
    which is the number the plan's "to be measured, not assumed" note (S6)
    and T501/AC2 call for: a real call counter on the factory, not a reading
    of the log an unrelated refactor could leave stale.

    Args:
        clients: Stub client per registered server name.

    Attributes:
        calls: Server name of every invocation, in order.
    """

    clients: Mapping[str, StubMcpClient]
    calls: list[str] = field(default_factory=list)

    def __call__(self, capability: CompiledMcpCapability) -> StubMcpClient:
        """Record the call and return the stub client registered for it."""
        self.calls.append(capability.server)
        return self.clients[capability.server]


# ---------------------------------------------------------------------------
# Engine stubs
# ---------------------------------------------------------------------------


def default_script(output: object = DEFAULT_OUTPUT) -> tuple[AgentEvent, ...]:
    """Return the canonical success script: one delta plus one ``final``."""
    return (TextDeltaEvent(text="ok"), FinalEvent(output=output, usage=DEFAULT_USAGE))


class ScriptedEngine:
    """Offline :class:`~loom.ai.abc.AgentEngine` double with explicit delays.

    ``FakeAgentEngine`` replays a script instantly, which cannot express the
    timing the limit and cancellation tests need.  This double adds a declared
    per-event delay and records whether the stream observed cancellation.

    Args:
        script: Events to replay, terminal event last.
        delays_ms: Delay before each event, positionally aligned with
            ``script``; missing positions default to ``0``.
        health_status: Status reported by :meth:`health`.
        health_gate: When set, :meth:`health` waits on it before answering, so
            a test can hold the background probe open indefinitely.
    """

    def __init__(
        self,
        *,
        script: Sequence[AgentEvent] | None = None,
        delays_ms: Sequence[int] = (),
        health_status: str = "ok",
        health_gate: asyncio.Event | None = None,
    ) -> None:
        self.script: tuple[AgentEvent, ...] = tuple(
            script if script is not None else default_script()
        )
        self.delays_ms = tuple(delays_ms)
        self.health_status = health_status
        self.health_gate = health_gate
        self.emitted: list[AgentEvent] = []
        self.cancelled = False
        self.started = asyncio.Event()
        self.stream_count = 0
        self.conversations: list[Conversation | None] = []

    def _delay_for(self, position: int) -> float:
        if position < len(self.delays_ms):
            return self.delays_ms[position] / 1000
        return 0.0

    async def _iterate(self) -> AsyncGenerator[AgentEvent, None]:
        try:
            for position, event in enumerate(self.script):
                delay = self._delay_for(position)
                if delay:
                    await asyncio.sleep(delay)
                self.started.set()
                self.emitted.append(event)
                yield event
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> Any:  # AbstractAsyncContextManager[AsyncIterator[AgentEvent]]
        """Replay the script as an event stream, recording the conversation."""
        del prompt, identity, state
        self.stream_count += 1
        self.conversations.append(conversation)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            iterator = self._iterate()
            try:
                yield iterator
            finally:
                await iterator.aclose()

        return _stream()

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        """Replay the script to completion and return its terminal outcome."""
        async with self.run_stream(
            prompt, identity=identity, conversation=conversation, state=state
        ) as stream:
            last: AgentEvent | None = None
            async for event in stream:
                last = event
        if isinstance(last, FinalEvent):
            return AgentResult(output=last.output, usage=last.usage)
        raise AssertionError("scripted run did not end in a FinalEvent")

    async def health(self) -> HealthStatus:
        """Report the scripted health, optionally blocking on the gate."""
        if self.health_gate is not None:
            await self.health_gate.wait()
        return HealthStatus(status=self.health_status)  # type: ignore[arg-type]


class CountingEngineProvider:
    """Engine provider counting how often the runtime builds an engine.

    Args:
        engines: Engine per plan name.  A plan absent from the mapping gets a
            fresh default-script :class:`ScriptedEngine`.
    """

    LOOM_AI_ENGINE_API = 2

    def __init__(self, engines: Mapping[str, ScriptedEngine] | None = None) -> None:
        self.engines: dict[str, ScriptedEngine] = dict(engines or {})
        self.calls: list[str] = []

    def create_engine(self, plan: object, *, deps: object, container: object) -> ScriptedEngine:
        """Build (or return the pre-scripted) engine for one plan."""
        del deps, container
        name = str(getattr(plan, "name", plan))
        self.calls.append(name)
        return self.engines.setdefault(name, ScriptedEngine())

    def supported_capability_kinds(self) -> frozenset[str]:
        """Accept every compiled capability kind."""
        return frozenset({"usecase", "sql", "mcp", "skills", "python", "a2a"})

    # An engine that announces 'mcp' and 'a2a' must supply what serving them
    # needs: the composition root reads these off the provider rather than
    # importing an engine package, so announcing a kind without its factory is
    # now an inconsistency the runtime reports instead of one it hides.
    #
    # Kept as a staticmethod on purpose: 'engine_client_factories' reads the
    # attribute with getattr, so a third-party engine may still declare one,
    # and this double is where that shape is recorded. It is never invoked --
    # this provider builds ScriptedEngines, which serve no real MCP grant, and
    # every runtime here is handed an explicit factory. A pydantic-ai
    # deployment must instead read 'mcp_client_factory' off the very provider
    # instance that builds its engines, which 'SharedMcpToolsets.for_build'
    # now refuses to let a deployment get wrong.
    mcp_client_factory = staticmethod(create_mcp_client)
    a2a_client_factory = staticmethod(create_a2a_client)


class StubDepsFactory:
    """Per-invocation dependency factory carrying only the caller identity."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the dependency bundle for one invocation."""
        del container
        return {"identity": identity}


class FakeUow:
    """Unit of work recording its transaction calls into a shared log.

    Args:
        log: Shared sequence receiving ``begin``/``commit``/``rollback``.
    """

    transactional: ClassVar[bool] = True

    def __init__(self, log: list[str]) -> None:
        self._log = log

    async def begin(self) -> None:
        """Record the start of the transaction."""
        self._log.append("begin")

    async def commit(self) -> None:
        """Record the commit."""
        self._log.append("commit")

    async def rollback(self) -> None:
        """Record the rollback."""
        self._log.append("rollback")

    async def __aenter__(self) -> FakeUow:
        """Begin on entry, as the protocol prescribes."""
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Commit on a clean exit, roll back otherwise."""
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()


class FakeUowFactory:
    """Unit-of-work factory whose every unit records into one shared log.

    Attributes:
        log: Transaction calls in the order the executor issued them.
    """

    def __init__(self) -> None:
        self.log: list[str] = []

    def create(self) -> FakeUow:
        """Return a fresh recording unit of work."""
        return FakeUow(self.log)


@dataclass(frozen=True)
class RecordingDeps:
    """Bundle shaped as the composition root's: caller, container, bound invoker."""

    identity: Identity
    container: LoomContainer
    invoker: AppInvoker


@dataclass
class RecordingDepsFactory:
    """Deps factory running real use cases through a real executor, no database.

    Built the way the composition root builds it: the use cases are compiled
    by a :class:`UseCaseCompiler`, executed by a :class:`RuntimeExecutor` over
    a recording unit of work, resolved through a :class:`UseCaseRegistry`, and
    the invoker handed to each bundle is bound to that invocation's caller.

    Args:
        use_cases: Use-case types to compile and register.

    Attributes:
        uow: The recording unit-of-work factory, for transaction assertions.
    """

    use_cases: Sequence[type[Compilable]]
    uow: FakeUowFactory = field(default_factory=FakeUowFactory)

    def __post_init__(self) -> None:
        compiler = UseCaseCompiler()
        for use_case in self.use_cases:
            compiler.compile(use_case)
        self._registry = UseCaseRegistry.build(list(self.use_cases))
        self._executor = RuntimeExecutor(compiler, uow_factory=self.uow)

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the bundle for one invocation, its invoker bound to ``identity``."""
        invoker = AppInvoker(UseCaseFactory(container), self._executor, self._registry)
        return RecordingDeps(
            identity=identity, container=container, invoker=invoker.for_identity(identity)
        )


# ---------------------------------------------------------------------------
# Plan and configuration builders
# ---------------------------------------------------------------------------


def make_policies(
    *,
    retries: int = 0,
    tool_timeout_ms: int = 1000,
    max_iterations: int = 8,
    run_timeout_ms: int = 5000,
    max_history_bytes: int = 1048576,
) -> PolicySpec:
    """Build a :class:`PolicySpec` with every limit stated explicitly."""
    return PolicySpec(
        retries=retries,
        tool_timeout_ms=tool_timeout_ms,
        max_iterations=max_iterations,
        run_timeout_ms=run_timeout_ms,
        max_history_bytes=max_history_bytes,
    )


def make_plan(
    name: str = "analyst",
    *,
    capabilities: Sequence[CompiledCapability] = (),
    policies: PolicySpec | None = None,
    state: StateShape | None = None,
) -> AgentPlan:
    """Build a compiled plan with a decodable output and no secret material."""
    return AgentPlan(
        name=name,
        description=f"{name} test agent",
        instructions=(CompiledInstruction(text="answer"),),
        state=state,
        spec_version=1,
        inference=InferenceTarget(provider="fake", model="fake-model"),
        output=CompiledOutput(
            schema={"type": "object"},
            loom_type=msgspec_type(dict),
        ),
        capabilities=tuple(capabilities),
        policies=policies if policies is not None else make_policies(),
        metadata={},
    )


def make_state_shape() -> StateShape:
    """Build the ``deps_type: dict`` waiver shape: no schema, no boundary type."""
    return StateShape(schema=None, loom_type=None)


class AppraisalStruct(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Msgspec twin of :class:`AppraisalModel`, same shape, same builtins."""

    issuer: str
    total: float


class AppraisalModel(pydantic.BaseModel):
    """Strict pydantic twin of :class:`AppraisalStruct`, same shape, same builtins."""

    model_config = pydantic.ConfigDict(extra="forbid")

    issuer: str
    total: float


OUTPUT_TWINS: tuple[tuple[str, type[Any], object], ...] = (
    ("msgspec", AppraisalStruct, AppraisalStruct(issuer="Acme", total=42.5)),
    ("pydantic", AppraisalModel, AppraisalModel(issuer="Acme", total=42.5)),
)
"""One boundary type per library, the answer built from it, and its id."""


def plan_with_output(cls: type[Any], *, name: str = "analyst") -> AgentPlan:
    """A plan whose declared output is ``cls``, compiled through ``loom_type``."""
    return msgspec.structs.replace(
        make_plan(name),
        output=CompiledOutput(schema={"type": "object"}, loom_type=loom_type(cls)),
    )


DEFAULT_MCP_SERVER = "tools"
"""Name a plain MCP grant refers to; the deployment maps it to an address."""


def mcp_server_url(server: str) -> str:
    """Return the address ``ai.mcp_servers.<server>`` is taken to resolve to."""
    return f"https://{server}.internal/mcp"


def make_mcp_capability(
    server: str = DEFAULT_MCP_SERVER,
    *,
    include: Sequence[str] = (),
    exclude: Sequence[str] = (),
) -> CompiledMcpCapability:
    """Build an MCP capability naming a registered server and its glob filter."""
    return CompiledMcpCapability(
        server=server,
        url=mcp_server_url(server),
        include=tuple(include),
        exclude=tuple(exclude),
    )


def make_mcp_servers(*servers: str) -> dict[str, McpServerConfig]:
    """Build the ``ai.mcp_servers`` entries the named grants resolve against."""
    return {server: McpServerConfig(url=Dsn(mcp_server_url(server))) for server in servers}


def make_sql_connection(*, readonly: bool = True) -> SqlConnectionConfig:
    """Build a SQL connection configuration with an explicit read-only state."""
    return SqlConnectionConfig(
        backend="clickhouse",
        url=Dsn("clickhouse://reports.internal:8123/reporting"),
        allowed_roles=(),
        readonly=readonly,
    )


def make_sql_capability(
    connection: str = "reporting",
    *,
    readonly: bool = True,
) -> CompiledSqlCapability:
    """Build a compiled SQL capability pinning the plan's read-only belief."""
    return CompiledSqlCapability(
        connection=connection,
        config=make_sql_connection(readonly=readonly),
        max_rows=1000,
        max_result_bytes=1_000_000,
    )


def make_sql_config(connection: str = "reporting", *, readonly: bool = True) -> SqlConfig:
    """Build the live SQL configuration the runtime re-verifies against."""
    return SqlConfig(connections={connection: make_sql_connection(readonly=readonly)})


def make_ai_config(
    *,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
    a2a: A2AConfig | None = None,
    mcp_servers: Mapping[str, McpServerConfig] | None = None,
    a2a_agents: Mapping[str, A2AAgentConfig] | None = None,
    startup_timeout_ms: int = 500,
    max_concurrent_runs: int = 8,
    max_prompt_bytes: int = 65536,
    max_state_bytes: int = 65536,
    health_cache_ttl_ms: int = 20,
    remote_clients: str = "required",
    max_agent_depth: int = 1,
) -> AiConfig:
    """Build an ``AiConfig`` with test-sized budgets and no model secrets."""
    return AiConfig(
        engine="fake",
        specs=("ai/agents/*/agent.yaml",),
        models={"default": InferenceTarget(provider="fake", model="fake-model")},
        endpoints=dict(endpoints or {}),
        a2a=a2a,
        mcp_servers=dict(mcp_servers or {}),
        a2a_agents=dict(a2a_agents or {}),
        startup_timeout_ms=startup_timeout_ms,
        max_concurrent_runs=max_concurrent_runs,
        max_prompt_bytes=max_prompt_bytes,
        max_state_bytes=max_state_bytes,
        health_cache_ttl_ms=health_cache_ttl_ms,
        remote_clients=remote_clients,
        max_agent_depth=max_agent_depth,
    )


def make_endpoint(
    *,
    enabled: bool = True,
    auth: str = "external",
    allow_anonymous: bool = False,
) -> AgentEndpointConfig:
    """Build one per-agent HTTP opt-in entry."""
    return AgentEndpointConfig(enabled=enabled, auth=auth, allow_anonymous=allow_anonymous)


def error_script(
    code: AgentRunErrorCode, message: str = "scripted failure"
) -> tuple[AgentEvent, ...]:
    """Return a script whose terminal event is an ``ErrorEvent`` with ``code``."""
    return (ErrorEvent(code=code, message=message),)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def identity() -> Identity:
    """Verified caller used by every run in this suite."""
    return Identity(subject="user-1", roles=("analyst",), mechanism="test")


@pytest.fixture
def container() -> LoomContainer:
    """Empty application container; no test resolves anything from it."""
    return LoomContainer()


@pytest.fixture
def deps() -> StubDepsFactory:
    """Per-invocation dependency factory shared by every runtime under test."""
    return StubDepsFactory()


@pytest.fixture
def lifecycle_log() -> list[str]:
    """Shared open/close log every stub MCP client appends to."""
    return []


CANARY = "canary-9d41-order-payload"
"""Marker the served payload carries; no stream event may ever contain it."""


@dataclass(frozen=True)
class CapabilityDeps:
    """Bundle satisfying the capability boundary: a verified caller and a container."""

    identity: Identity
    container: LoomContainer


class CapabilityDepsFactory:
    """Per-invocation factory producing a well-formed :class:`CapabilityDeps`."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the bundle every MCP tool call is guarded against."""
        return CapabilityDeps(identity=identity, container=container)


class ScriptedToolModel:
    """A model that issues a fixed tool call, then answers.

    It records the tool surface the engine offered it, which is how a test
    observes what the grant's filter actually let through.

    Args:
        calls: ``(tool name, arguments)`` pairs to issue, in order.
    """

    def __init__(self, *, calls: Sequence[tuple[str, Mapping[str, Any]]] = ()) -> None:
        self.calls = tuple(calls)
        self.offered_tools: tuple[str, ...] = ()
        self.tool_returns: list[str] = []
        self._answer = msgspec.json.encode({"answer": "done"}).decode()
        self._step = 0
        self._seen: set[str] = set()

    def as_model(self) -> Model:
        """Return the ``FunctionModel`` the engine will drive.

        Both modes are scripted: the runtime always drives the engine through
        ``run_stream``, so a response-only ``FunctionModel`` would fail the run
        before the MCP toolset was ever consulted.
        """
        return FunctionModel(self._respond, stream_function=self._stream)

    def _respond(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        self.offered_tools = tuple(tool.name for tool in info.function_tools)
        self._collect(messages)
        if self._step < len(self.calls):
            name, arguments = self.calls[self._step]
            self._step += 1
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name=name, args=dict(arguments), tool_call_id=f"call-{self._step}"
                    )
                ]
            )
        return ModelResponse(
            parts=[ToolCallPart(tool_name=info.output_tools[0].name, args=self._answer)]
        )

    async def _stream(
        self, messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        self.offered_tools = tuple(tool.name for tool in info.function_tools)
        self._collect(messages)
        if self._step < len(self.calls):
            name, arguments = self.calls[self._step]
            self._step += 1
            yield {
                0: DeltaToolCall(
                    name=name,
                    json_args=msgspec.json.encode(dict(arguments)).decode(),
                    tool_call_id=f"call-{self._step}",
                )
            }
            return
        yield {
            0: DeltaToolCall(
                name=info.output_tools[0].name, json_args=self._answer, tool_call_id="final"
            )
        }

    def _collect(self, messages: Sequence[ModelMessage]) -> None:
        for message in messages:
            for part in getattr(message, "parts", ()):
                if isinstance(part, ToolReturnPart) and part.tool_call_id not in self._seen:
                    self._seen.add(part.tool_call_id)
                    self.tool_returns.append(str(part.content))


@pytest.fixture
def caller() -> Identity:
    """Verified caller every run in this module executes as."""
    return Identity(subject="clerk-1", roles=("clerk",), mechanism="test")


async def _until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    """Poll *predicate* until it holds, failing the test if it never does."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("the runtime never reached the expected state")


# ---------------------------------------------------------------------------
# Conversational agent doubles: the output hook and the loader, as the
# composition root would wire them, shared by the HTTP and A2A surface tests
# ---------------------------------------------------------------------------


class TurnRecorded(msgspec.Struct, frozen=True):
    """Result of recording one turn: what ``hook_result`` carries to the client."""

    triage_id: str


class TurnCommand(Command, frozen=True, kw_only=True):
    """What the hook wants from a run: the output, the id, the thread."""

    output: dict[str, Any]
    interaction_id: str
    conversation_id: str | None = None


class HistoryCommand(Command, frozen=True, kw_only=True):
    """What the loader wants from a run: the thread to continue."""

    conversation_id: str


@dataclass
class ConversationRecorder:
    """Shared observer of the hook and loader executions, resolved from the container.

    Attributes:
        commands: One entry per hook execution, in order.
        timeline: ``"hook"`` appended when a hook runs; tests append frames.
        entered: Set when the first hook execution starts.
        gate: When set, a hook execution waits on it before completing.
        failure: Exception a hook execution raises instead of completing.
        load_commands: One entry per loader execution, in order.
        load_callers: The caller of each loader execution, in order.
        load_failure: Exception a loader execution raises instead of returning.
        history: What the loader returns; ``None`` makes every turn a first one.
    """

    commands: list[TurnCommand] = field(default_factory=list)
    timeline: list[str] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    gate: asyncio.Event | None = None
    failure: Exception | None = None
    load_commands: list[HistoryCommand] = field(default_factory=list)
    load_callers: list[Identity] = field(default_factory=list)
    load_failure: Exception | None = None
    history: bytes | None = None

    async def record(self, command: TurnCommand) -> None:
        """Record one hook execution, honouring the gate and raising the failure if any."""
        self.commands.append(command)
        self.timeline.append("hook")
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        if self.failure is not None:
            raise self.failure

    async def load(self, command: HistoryCommand, caller: Identity) -> bytes | None:
        """Record one loader execution and return the scripted history."""
        self.load_commands.append(command)
        self.load_callers.append(caller)
        if self.load_failure is not None:
            raise self.load_failure
        return self.history


@use_case_key("conversations.record_turn_over_the_wire")
class RecordTurn(UseCase[Any, TurnRecorded]):
    """Records a turn once the agent has produced it."""

    def __init__(self, recorder: ConversationRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: TurnCommand = Input(), caller: Identity = Caller()
    ) -> TurnRecorded:
        del caller
        await self._recorder.record(cmd)
        return TurnRecorded(triage_id=cmd.interaction_id)


@use_case_key("conversations.load_over_the_wire")
class LoadHistory(UseCase[Any, bytes | None]):
    """Returns the prior history of a conversation."""

    def __init__(self, recorder: ConversationRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: HistoryCommand = Input(), caller: Identity = Caller()
    ) -> bytes | None:
        return await self._recorder.load(cmd, caller)


def conversational_plan(name: str, *, hook: bool = True, loader: bool = False) -> AgentPlan:
    """Build a plan whose ``on_output`` and ``conversation`` name the shared doubles.

    Args:
        name: Agent name of the plan.
        hook: Whether ``on_output`` names :class:`RecordTurn`.
        loader: Whether ``conversation`` names :class:`LoadHistory`.
    """
    plan = make_plan(name)
    if hook:
        on_output = CompiledOutputHook(
            usecase="conversations.record_turn_over_the_wire",
            use_case=RecordTurn,
            accepted=frozenset(info.name for info in msgspec.structs.fields(TurnCommand)),
        )
        plan = msgspec.structs.replace(plan, on_output=on_output)
    if loader:
        conversation = CompiledConversation(
            usecase="conversations.load_over_the_wire",
            use_case=LoadHistory,
            accepted=frozenset(info.name for info in msgspec.structs.fields(HistoryCommand)),
        )
        plan = msgspec.structs.replace(plan, conversation=conversation)
    return plan


@pytest.fixture
def conversation_recorder(container: LoomContainer) -> ConversationRecorder:
    """Recorder the hook and loader use cases resolve from the container."""
    recorder = ConversationRecorder()
    container.register_instance(ConversationRecorder, recorder)
    return recorder


@pytest.fixture
def conversation_deps() -> RecordingDepsFactory:
    """Deps factory serving the hook and the loader through a real executor."""
    return RecordingDepsFactory((RecordTurn, LoadHistory))


# ---------------------------------------------------------------------------
# Agent() marker doubles (spec 014, PR2)
#
# 'AgentRuntime' and the marker resolver it feeds
# ('loom.ai.runtime._handle.agent_marker_resolver') are deliberately absent
# from this module, for the reason the module docstring states: importing
# 'loom.ai.runtime' here would abort collection of every test in this
# package on one bad import, hiding which module actually regressed. Each
# marker test module builds its own 'AgentRuntime' and resolver, over the
# plan and use-case doubles this section provides.
# ---------------------------------------------------------------------------


class RecordingScriptedEngine(ScriptedEngine):
    """A :class:`ScriptedEngine` that also records the identity and state of every run.

    What a marker-driven run must prove is not merely that it returns an
    answer, but that the identity reaching the model is the one the executor
    bound the handle to — never a worker identity, never a value read back
    out of the caller-facing parameters. Recording it here, at the one seam
    between the handle and the "model", is what lets a test assert on it.
    ``states`` is the corresponding record for T303's own channel: what the
    runtime resolved *this* run's ``state`` to, after FR-010's check and
    FR-009's defaults.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.identities: list[Identity] = []
        self.states: list[object | None] = []

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> Any:
        self.identities.append(identity)
        self.states.append(state)
        return super().run_stream(prompt, identity=identity, conversation=conversation, state=state)


class ShapedRecordingEngine(RecordingScriptedEngine):
    """A :class:`RecordingScriptedEngine` that also serves per-run shape overrides.

    Records ``output_type`` per call to ``run_stream_shaped`` so a test can
    tell an ``AgentHandle.run(expect=...)``/``run_text`` call apart from a
    plain ``run(prompt)`` — the latter goes through ``run_stream`` alone,
    never through this method (T304).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.shaped_calls: list[type[Any]] = []

    def run_stream_shaped(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        output_type: type[Any],
        state: object | None = None,
    ) -> Any:
        self.shaped_calls.append(output_type)
        return self.run_stream(prompt, identity=identity, conversation=conversation, state=state)


MARKER_AGENT_NAME = "triage"
"""Agent name every marker use case below reaches through ``Agent()``."""


class MarkerAgentUseCase(UseCase[object, dict[str, object]]):
    """The example the spec's marker shape describes, at its simplest.

    Declares both ``Caller()`` and ``Agent()``: the handle must close over
    the same identity ``caller`` receives, both bound by the executor from
    the one identity the transport verified for this execution.
    """

    async def execute(
        self,
        caller: Identity = Caller(),
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        answer = await triage.run("assess this")
        return {"caller_subject": caller.subject, "output": answer.output}


class ShadowedIdentityParamUseCase(UseCase[object, dict[str, object]]):
    """A primitive parameter that merely *looks* like an identity.

    Regression fixture for the exact defect a reviewer found in the SQL pull
    request: a value the caller controls through ``params`` must never reach
    the model as if it were the verified identity. ``identity`` here is a
    plain ``str`` — an ordinary primitive parameter with no marker — so a
    caller may set it to anything; the handle must still run as ``caller``,
    read only from the ``Caller()``/``Agent()`` bindings.
    """

    async def execute(
        self,
        identity: str,
        caller: Identity = Caller(),
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        answer = await triage.run(identity)
        return {
            "param_identity": identity,
            "caller_subject": caller.subject,
            "output": answer.output,
        }


OUTER_MARKER_AGENT_NAME = "outer"
INNER_MARKER_AGENT_NAME = "inner"


class OuterMarkerUseCase(UseCase[object, dict[str, object]]):
    """Declares the outer agent of a nested-call scenario."""

    async def execute(
        self,
        caller: Identity = Caller(),
        outer: AgentHandle[dict[str, Any]] = Agent(OUTER_MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        del caller
        answer = await outer.run("go")
        return {"output": answer.output}


class InnerMarkerUseCase(UseCase[object, dict[str, object]]):
    """Declares the inner agent a nested call reaches from inside the outer run.

    Stands in for an output hook or a tool's own use case invoking another
    agent — the exact shape spec 011's rewritten paragraph (T601) describes.
    """

    async def execute(
        self,
        caller: Identity = Caller(),
        inner: AgentHandle[dict[str, Any]] = Agent(INNER_MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        del caller
        answer = await inner.run("go deeper")
        return {"output": answer.output}


def marker_use_case_executor(
    *use_cases: type[Compilable],
) -> tuple[UseCaseCompiler, RuntimeExecutor]:
    """Compile *use_cases* and build the real executor a marker test drives.

    Args:
        use_cases: Every use case the test invokes, including the nested
            ones an outer run's engine calls back into.

    Returns:
        The compiler (for plan inspection) and the executor, not yet bound
        to any agent resolver — the test module binds one once its own
        ``AgentRuntime`` exists.
    """
    compiler = UseCaseCompiler()
    for use_case in use_cases:
        compiler.compile(use_case)
    return compiler, RuntimeExecutor(compiler)
