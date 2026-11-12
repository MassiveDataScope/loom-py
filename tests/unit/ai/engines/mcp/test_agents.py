"""The agent path over the connection's one shared MCP session (T05).

An agent run reaches an MCP server through the toolset ``for_build`` hands the
engine. That toolset is the connection's session, so the model's own tool calls
share its renewal, its safe retry and its freeze handling with the grant views,
and a call that may have run fails the run instead of reaching the model as a
refusal. Driven through the engine's real build path (filter and call boundary)
and a real pydantic-ai agent, against a FastMCP server served in process.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

from loom.ai.compiler import CompiledPythonCapability
from loom.ai.engines.pydantic_ai import _capabilities, _mcp, _transport
from loom.ai.engines.pydantic_ai._guards import BuildContext
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets, _SessionToolset, _ToolsetSession
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._grants import INTERRUPTED_CALL_CODE
from loom.ai.runtime._handle import _BoundAgentHandle, mcp_marker_resolver
from loom.ai.runtime._lifecycle import UseCaseMcpGrant
from loom.core.di.container import LoomContainer
from loom.core.identity import Identity
from loom.core.sql.service import NullSqlQueryService
from tests.unit.ai.engines.mcp.server import InProcessServer, serving

pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

from pydantic_ai import Agent  # noqa: E402
from pydantic_ai.exceptions import ModelRetry  # noqa: E402
from pydantic_ai.messages import (  # noqa: E402
    ModelMessage,
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel  # noqa: E402
from pydantic_ai.models.test import TestModel  # noqa: E402
from pydantic_ai.tools import RunContext  # noqa: E402
from pydantic_ai.toolsets import AbstractToolset, FunctionToolset  # noqa: E402
from pydantic_ai.usage import RunUsage  # noqa: E402

_HANDSHAKE = ("server/discover", "initialize")


@dataclass
class _Deps:
    """The dependency bundle a capability call requires."""

    identity: Identity = field(default_factory=lambda: Identity(subject="tester"))
    container: LoomContainer = field(default_factory=LoomContainer)
    invoker: Any = None


class _Model:
    """Calls one tool once, then answers with what came back."""

    def __init__(self, tool: str, arguments: dict[str, Any]) -> None:
        self.tool = tool
        self.arguments = arguments
        self.calls = 0
        self.seen: list[ToolReturnPart | RetryPromptPart] = []

    def __call__(self, messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del info
        self.calls += 1
        back = [
            part
            for part in messages[-1].parts
            if isinstance(part, ToolReturnPart | RetryPromptPart)
        ]
        if not back:
            return ModelResponse(parts=[ToolCallPart(self.tool, self.arguments)])
        self.seen.extend(back)
        return ModelResponse(parts=[TextPart("done")])


@dataclass
class _World:
    """One server, the worker's shared toolsets over it, and the session the runtime opened."""

    server: InProcessServer
    shared: SharedMcpToolsets
    session: _ToolsetSession
    built: list[object]
    timeout_s: float = 5.0
    first_handed_out: object = None

    def context(self) -> BuildContext:
        """The build context of an agent granted this server."""
        return BuildContext(
            agent="agent",
            container=LoomContainer(),
            observability=None,
            timeout_s=self.timeout_s,
            mcp=self.shared,
            mcp_grants=(self.server.capability,),
        )

    def toolset(self) -> AbstractToolset[Any]:
        """The agent's MCP toolset, built as the engine builds it."""
        return _capabilities._mcp_toolset(self.server.capability, self.context())

    async def run(self, tool: str = "echo", arguments: dict[str, Any] | None = None) -> _Model:
        """Run one agent whose model calls *tool* once (``echo`` with ``"agent"`` by default)."""
        model = _Model(tool, {"text": "agent"} if arguments is None else arguments)
        agent = Agent(FunctionModel(model), toolsets=[self.toolset()], deps_type=_Deps)
        await agent.run("go", deps=_Deps())
        return model

    async def failed_run(
        self, tool: str = "echo", arguments: dict[str, Any] | None = None
    ) -> tuple[AgentRunError, _Model]:
        """Run one agent that must fail; return the error and the model."""
        model = _Model(tool, {"text": "agent"} if arguments is None else arguments)
        agent = Agent(FunctionModel(model), toolsets=[self.toolset()], deps_type=_Deps)
        with pytest.raises(AgentRunError) as raised:
            await agent.run("go", deps=_Deps())
        return raised.value, model

    def requests(self, *methods: str) -> int:
        return self.server.count(*methods)


def _counting_builds(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Every toolset built from here on, so a renewal shows up as one more."""
    built: list[object] = []
    build = _mcp.build_mcp_toolset

    def counting(*args: Any, **kwargs: Any) -> Any:
        built.append(build(*args, **kwargs))
        return built[-1]

    monkeypatch.setattr(_mcp, "build_mcp_toolset", counting)
    return built


@asynccontextmanager
async def _world(monkeypatch: pytest.MonkeyPatch, *, warm: bool = True) -> AsyncIterator[_World]:
    """Open the connection as the runtime does: entered, its tools listed, one call made."""
    built = _counting_builds(monkeypatch)
    async with serving(monkeypatch) as server:
        shared = SharedMcpToolsets()
        async with shared.open(server.capability) as session:
            await session.list_tools()
            if warm:
                await session.call_tool("echo", {"text": "warm"})
            server.log.clear()
            server.runs.clear()
            yield _World(server, shared, session, built)


@asynccontextmanager
async def _unreachable_at_start_up(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_World]:
    """A connection start-up tolerated as unreachable, whose server came back afterwards."""
    built = _counting_builds(monkeypatch)
    async with serving(monkeypatch) as server:
        shared = SharedMcpToolsets()
        server.down = True
        with pytest.raises(Exception):  # noqa: B017, PT011 - whatever the connect raises
            async with shared.open(server.capability):
                pass
        server.down = False
        server.log.clear()
        handed_out = shared.for_build(server.capability, "agent")
        session = shared.session_for(server.capability, "agent")
        assert isinstance(session, _ToolsetSession)
        yield _World(server, shared, session, built, first_handed_out=handed_out)


def _texts(parts: Sequence[ToolReturnPart | RetryPromptPart]) -> list[str]:
    """What the model got back: a tool's answer, or ``"retry"`` for a ``ModelRetry``."""
    return [
        "retry"
        if isinstance(part, RetryPromptPart) or part.retry is not None
        else part.model_response_str()
        for part in parts
    ]


class TestTheToolsetTheEngineGets:
    async def test_is_the_connections_session_never_the_raw_toolset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            toolset = world.shared.for_build(world.server.capability, "agent")

            assert toolset is not world.built[0]
            assert await toolset.for_run(None) is toolset  # type: ignore[arg-type]
            assert await toolset.for_run_step(None) is toolset  # type: ignore[arg-type]
            assert toolset.visit_and_replace(lambda leaf: world.built[0]) is toolset  # type: ignore[arg-type,return-value]

    async def test_entering_and_leaving_it_opens_and_closes_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            toolset = world.shared.for_build(world.server.capability, "agent")

            for _ in range(3):
                async with toolset:
                    pass

            assert world.server.log == []

    async def test_a_python_factory_reaches_the_same_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            assert world.shared.session_for(world.server.capability, "agent") is world.session


class TestARunAfterTheRuntimeOpened:
    async def test_the_first_mcp_user_after_start_up_is_served(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch, warm=False) as world:
            model = await world.run()

        assert _texts(model.seen) == ["agent"]
        assert world.requests("tools/call") == 1
        assert world.requests(*_HANDSHAKE, "tools/list") == 0

    async def test_five_runs_after_a_renewal_send_five_calls_and_nothing_else(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.session._current.suspect = True
            await world.session.call_tool("echo", {"text": "renews"})
            world.server.log.clear()

            for _ in range(5):
                await world.run()

        assert world.server.log == ["tools/call"] * 5
        assert len(world.built) == 2


class TestAForgottenSession:
    async def test_the_agents_call_succeeds_through_one_rebuild(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The tool the model picked was listed by the old toolset; the new one takes it (R4)."""
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "forget")

            model = await world.run()

        assert _texts(model.seen) == ["agent"]
        assert len(world.built) == 2
        assert world.server.runs["echo"] == 1

    async def test_the_tools_are_listed_once_after_the_renewal_and_never_by_a_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            await world.run()
            between = world.requests("tools/list")
            world.server.fault("echo", "forget")
            await world.run()
            renewed = world.requests("tools/list")
            await world.run()

        assert (between, renewed, world.requests("tools/list")) == (0, 1, 1)

    async def test_a_retry_that_fails_too_fails_the_run_and_runs_the_tool_at_most_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "forget", "cut")

            error, model = await world.failed_run()

        assert error.code is AgentRunErrorCode.MCP_CALL_INTERRUPTED
        assert model.seen == []
        assert world.server.runs["echo"] <= 1


class TestRenewalIsShared:
    async def test_a_renewal_a_marker_made_serves_the_next_agent_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "forget")
            await world.session.call_tool("echo", {"text": "marker"})

            await world.run()

        assert len(world.built) == 2

    async def test_a_renewal_an_agent_made_serves_the_next_marker_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "forget")
            await world.run()

            await world.session.call_tool("echo", {"text": "marker"})

        assert len(world.built) == 2


class TestACallThatMayHaveRun:
    async def test_a_cut_after_the_tool_ran_fails_the_run_and_never_reaches_the_model(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Round 3, H1: the client's own error arrives wrapped in ``ModelRetry``."""
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "cut")

            error, model = await world.failed_run()

        assert error.code is AgentRunErrorCode.MCP_CALL_INTERRUPTED
        assert isinstance(error.__cause__, ModelRetry)
        assert (model.calls, model.seen) == (1, [])
        assert world.server.runs["echo"] == 1
        assert len(world.built) == 1

    async def test_a_client_set_code_a_middleware_sent_fails_the_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", -32603)

            error, model = await world.failed_run()

        assert error.code is AgentRunErrorCode.MCP_CALL_INTERRUPTED
        assert (model.calls, model.seen) == (1, [])

    async def test_a_server_sent_error_outside_the_client_set_reaches_the_model(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", -32043)

            model = await world.run()

        assert _texts(model.seen) == ["retry"]
        assert model.calls == 2

    async def test_a_python_tool_and_the_model_report_the_same_code(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "cut", "cut")
            remote = world.shared.session_for(world.server.capability, "agent")
            with pytest.raises(AgentRunError) as from_python:
                await remote.call_tool("echo", {"text": "python"})
            world.session._current.suspect = False

            from_model, _ = await world.failed_run()

        assert from_python.value.code is from_model.code


class TestTheAgentPathAfterAFreeze:
    async def test_drops_the_stale_pool_once_and_neither_renews_nor_lists(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(_transport, "time", clock)
        drop = _transport.EvictableTransport.drop
        drops: list[None] = []

        async def counted(self: _transport.EvictableTransport) -> None:
            drops.append(None)
            await drop(self)

        monkeypatch.setattr(_transport.EvictableTransport, "drop", counted)
        async with _world(monkeypatch) as world:
            clock.now += 60.0

            model = await world.run()

        assert _texts(model.seen) == ["agent"]
        assert (len(drops), len(world.built)) == (1, 1)
        assert world.server.log == ["tools/call"]


class TestTheToolTimeout:
    async def test_bounds_the_attempt_the_renewal_and_the_retry_together(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.timeout_s = 0.3
            world.server.fault("slow", "forget")
            started = asyncio.get_running_loop().time()

            error, _ = await world.failed_run("slow", {})

            elapsed = asyncio.get_running_loop().time() - started
            world.server.release.set()

        assert error.code is AgentRunErrorCode.TOOL_TIMEOUT
        assert len(world.built) == 2
        assert elapsed < 1.0


class _Clock:
    """The wall clock alone, as ``_transport`` reads it."""

    def __init__(self) -> None:
        self.now = 1_000_000.0

    def time(self) -> float:
        return self.now


def _clock() -> str:
    """A tool of the agent's own, which needs no MCP server."""
    return "noon"


def _run_context() -> RunContext[Any]:
    return RunContext(deps=None, model=TestModel(), usage=RunUsage())


class TestListingTools:
    async def test_never_renews_so_a_run_that_calls_no_mcp_tool_succeeds_while_it_is_down(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.session._current.suspect = True
            world.server.down = True
            model = _Model("_clock", {})
            agent = Agent(
                FunctionModel(model), toolsets=[world.toolset()], tools=[_clock], deps_type=_Deps
            )

            result = await agent.run("go", deps=_Deps())

        assert result.output == "done"
        assert world.server.log == []
        assert len(world.built) == 1

    async def test_lists_the_live_toolset_after_a_renewal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _world(monkeypatch) as world:
            world.server.fault("echo", "forget")
            await world.session.call_tool("echo", {"text": "renews"})
            toolset = world.shared.for_build(world.server.capability, "agent")

            tools = await toolset.get_tools(_run_context())

        assert [tool.toolset for tool in tools.values()] == [world.built[1]] * len(tools)


class TestAConnectionUnreachableAtStartUp:
    async def test_is_served_through_one_session_like_any_other(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _unreachable_at_start_up(monkeypatch) as world:
            model = await world.run()

            assert isinstance(world.first_handed_out, _SessionToolset)
            assert world.shared.session_for(world.server.capability, "agent") is world.session
        assert _texts(model.seen) == ["agent"]

    async def test_a_cut_after_the_tool_ran_fails_the_run_and_runs_it_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _unreachable_at_start_up(monkeypatch) as world:
            world.server.fault("echo", "cut")

            error, model = await world.failed_run()

            still_connected = [toolset for toolset in world.built if toolset.is_running]  # type: ignore[attr-defined]
        assert error.code is AgentRunErrorCode.MCP_CALL_INTERRUPTED
        assert (model.calls, model.seen) == (1, [])
        assert world.server.runs["echo"] == 1
        assert still_connected == []

    async def test_a_forgotten_session_is_renewed_once_and_the_call_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with _unreachable_at_start_up(monkeypatch) as world:
            await world.run()
            world.server.fault("echo", "forget")

            model = await world.run()

            still_connected = [toolset for toolset in world.built if toolset.is_running]  # type: ignore[attr-defined]
        assert _texts(model.seen) == ["agent"]
        assert world.server.runs["echo"] == 2
        assert still_connected == []


class TestAPythonFactory:
    async def test_reaches_the_connections_own_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        remotes: list[object] = []

        def factory(context: Any) -> FunctionToolset[Any]:
            remotes.append(context.remote(context_server))
            return FunctionToolset([])

        async with _world(monkeypatch) as world:
            context_server = world.server.capability.server
            capability = CompiledPythonCapability(
                factory_ref="tests.unit.ai.engines.mcp.factory", factory=factory, params={}
            )

            _capabilities._python_toolset(capability, world.context())

        assert remotes == [world.session]


class TestTheGrantViews:
    """A use case's ``Mcp()`` marker and ``handle.mcp()`` report an interruption alike."""

    async def test_report_the_interrupted_call_code(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async with serving(monkeypatch) as server, _runtime_over(server) as runtime:
            identity = Identity(subject="tester")
            server.fault("echo", "cut", "cut")
            marker = mcp_marker_resolver(runtime, observability=None)(_SERVER, ("echo",), identity)
            handle = _BoundAgentHandle(
                name="agent",
                runtime=runtime,
                identity=identity,
                observability=None,
                sql_query_service=NullSqlQueryService(),
            ).mcp(_SERVER)

            with pytest.raises(AgentRunError) as from_marker:
                await marker.call_untyped("echo", {"text": "marker"})
            with pytest.raises(AgentRunError) as from_handle:
                await handle.call_untyped("echo", {"text": "handle"})

        assert from_marker.value.code is from_handle.value.code is INTERRUPTED_CALL_CODE
        assert INTERRUPTED_CALL_CODE is AgentRunErrorCode.MCP_CALL_INTERRUPTED  # owner, decision A
        assert server.runs["echo"] == 2


_SERVER = "orders"


@asynccontextmanager
async def _runtime_over(server: InProcessServer) -> AsyncIterator[AgentRuntime]:
    """A runtime whose agent and use case are both granted the in-process server."""
    from tests.helpers.ai import CountingEngineProvider, StubDepsFactory, make_ai_config
    from tests.integration.ai.conftest import make_mcp_capability, make_mcp_servers, make_plan

    del server  # every toolset built while it serves reaches it, whatever the URL
    grant = make_mcp_capability(_SERVER)
    runtime = AgentRuntime(
        plans=[make_plan("agent", capabilities=[grant])],
        config=make_ai_config(mcp_servers=make_mcp_servers(_SERVER)),
        engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
        deps=StubDepsFactory(),
        container=LoomContainer(),
        mcp_client_factory=SharedMcpToolsets().open,
        use_case_mcp=(
            UseCaseMcpGrant(capability=grant, usecase="orders.find", parameter="gateway"),
        ),
    )
    async with runtime:
        yield runtime
