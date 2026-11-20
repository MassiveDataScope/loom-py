"""One MCP connection per server, however many agents were granted it (FR-026).

``_remote_capabilities`` has always de-duplicated MCP clients per server, but
the run path used to build one ``MCPToolset`` per grant per agent, so a worker
held the start-up session *plus* one connection per agent — one dynamic client
registration and one credential resolution each, against servers that keep
clients in memory.

What these tests measure is therefore the **transport**, not the number of
toolset objects: ``_CountingTransport`` counts every time a client really
opened a session, which is the thing a remote server sees. The server is an
in-process ``fastmcp`` application reached over ``FastMCPTransport``, so the
real :class:`pydantic_ai.mcp.MCPToolset` and its reference-counted entry are
under test and no socket is.

Sharing the toolset must not serialise the worker: the concurrency test is the
regression guard against ever routing runs through
:class:`~loom.ai.execution.SharedMcpSession`, whose lock would queue every
agent's tool call behind every other agent's.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from loom.ai.compiler import AgentPlan, CompiledMcpCapability
from loom.ai.config import AiConfig
from loom.ai.engines.pydantic_ai import PydanticAIEnginePlugin, create_mcp_client
from loom.ai.engines.pydantic_ai import _mcp as engine_mcp
from loom.ai.errors import AgentCompilationError, AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentRuntime
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.integration.ai.conftest import (
    CapabilityDepsFactory,
    ScriptedToolModel,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_policies,
)

pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

from fastmcp import FastMCP  # noqa: E402
from fastmcp.client.transports import FastMCPTransport  # noqa: E402

_SERVER = "orders"
_OTHER_SERVER = "billing"


class _Connections:
    """How many sessions the transport opened, and how many are still open."""

    def __init__(self) -> None:
        self.opened = 0
        self.live = 0


class _CountingTransport(FastMCPTransport):
    """An in-process transport that records every session opened over it."""

    def __init__(self, server: FastMCP[Any], connections: _Connections) -> None:
        super().__init__(server)
        self._connections = connections

    @asynccontextmanager
    async def connect_session(self, **kwargs: Any) -> AsyncIterator[Any]:
        self._connections.opened += 1
        self._connections.live += 1
        try:
            async with super().connect_session(**kwargs) as session:
                yield session
        finally:
            self._connections.live -= 1


class _Server:
    """The in-process MCP application every agent in this module reaches.

    ``blocking_read`` is what makes concurrency observable: it parks on an
    event the test releases, so a second agent's call either overtakes it or
    proves the calls are serialised.
    """

    def __init__(self) -> None:
        self.released = asyncio.Event()
        self.entered = asyncio.Event()
        self.app: FastMCP[Any] = FastMCP("orders")

        @self.app.tool
        def read_orders(customer: str) -> str:
            """Read the orders of one customer."""
            return f"orders of {customer}"

        @self.app.tool
        def write_orders(customer: str) -> str:
            """Record an order for one customer."""
            return f"wrote for {customer}"

        @self.app.tool
        async def blocking_read(customer: str) -> str:
            """Answer only once the test releases it."""
            self.entered.set()
            await self.released.wait()
            return f"slow orders of {customer}"


@pytest.fixture
def server() -> _Server:
    """The MCP application, fresh per test so its events are not shared."""
    return _Server()


@pytest.fixture
def connections(server: _Server, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Connections]:
    """Route every grant to the in-process server and count its sessions.

    Only the transport is substituted: ``build_mcp_toolset`` still builds the
    real ``MCPToolset``, so what the counts measure is that toolset's own
    reference-counted entry.
    """
    counted = _Connections()

    def _client(component: str, capability: CompiledMcpCapability) -> FastMCPTransport:
        del component, capability
        return _CountingTransport(server.app, counted)

    monkeypatch.setattr(engine_mcp, "_mcp_client", _client)
    yield counted
    assert counted.live == 0, "a session outlived the runtime that opened it"


def _config(**overrides: Any) -> AiConfig:
    """Deployment configuration registering both server names."""
    return make_ai_config(
        mcp_servers=make_mcp_servers(_SERVER, _OTHER_SERVER),
        startup_timeout_ms=10000,
        **overrides,
    )


def _runtime(plans: list[AgentPlan], model: ScriptedToolModel, **overrides: Any) -> AgentRuntime:
    """Build the runtime the composition root builds, MCP factory included."""
    provider = PydanticAIEnginePlugin(model_resolver=lambda target: model.as_model())
    return AgentRuntime(
        plans=plans,
        config=_config(**overrides),
        engine_plugin=provider,
        deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
        container=LoomContainer(),
        mcp_client_factory=provider.mcp_client_factory,
    )


class TestOneConnectionPerServer:
    """A worker opens one session per server, not one per agent."""

    async def test_three_agents_naming_one_server_open_one_session(
        self, connections: _Connections, caller: Identity
    ) -> None:
        """Every agent granted the server works over the start-up connection."""
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plans = [
            make_plan(name, capabilities=(make_mcp_capability(_SERVER),))
            for name in ("clerk", "auditor", "analyst")
        ]

        async with _runtime(plans, model) as runtime:
            for plan in plans:
                await runtime.run(plan.name, "what has acme ordered?", identity=caller)

        assert connections.opened == 1

    async def test_two_servers_open_one_session_each(
        self, connections: _Connections, caller: Identity
    ) -> None:
        """Sharing is per server: a second name is a second connection."""
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plans = [
            make_plan("clerk", capabilities=(make_mcp_capability(_SERVER),)),
            make_plan("biller", capabilities=(make_mcp_capability(_OTHER_SERVER),)),
        ]

        async with _runtime(plans, model) as runtime:
            for plan in plans:
                await runtime.run(plan.name, "what has acme ordered?", identity=caller)

        assert connections.opened == 2


class TestPerAgentViewsOverOneConnection:
    """Two agents filter one shared server without seeing each other's filter."""

    async def test_each_agent_sees_only_the_tools_its_grant_admits(
        self, connections: _Connections, caller: Identity
    ) -> None:
        """The filters are wrappers over one toolset, so neither mutates it."""
        model = ScriptedToolModel(calls=(("read_orders", {"customer": "acme"}),))
        plans = [
            make_plan("reader", capabilities=(make_mcp_capability(_SERVER, include=("read_*",)),)),
            make_plan("writer", capabilities=(make_mcp_capability(_SERVER, exclude=("read_*",)),)),
        ]

        async with _runtime(plans, model) as runtime:
            await runtime.run("reader", "read", identity=caller)
            reader_first = model.offered_tools
            await runtime.run("writer", "write", identity=caller)
            writer_tools = model.offered_tools
            await runtime.run("reader", "read again", identity=caller)
            reader_again = model.offered_tools

        assert reader_first == ("read_orders",)
        assert sorted(writer_tools) == ["blocking_read", "write_orders"]
        assert reader_again == reader_first
        assert connections.opened == 1


class TestConcurrentCallsOverTheSharedToolset:
    """Sharing the toolset must not serialise the worker's tool calls."""

    async def test_a_blocked_call_does_not_hold_up_another_agent(
        self, server: _Server, connections: _Connections, caller: Identity
    ) -> None:
        """One agent's in-flight call leaves its neighbour's call free to finish.

        Red the moment runs are routed through a session that serialises on one
        lock — the second agent would then wait for the first to be released.
        """
        blocked_model = ScriptedToolModel(calls=(("blocking_read", {"customer": "acme"}),))
        quick_model = ScriptedToolModel(calls=(("read_orders", {"customer": "beta"}),))
        plans = [
            make_plan("blocked", capabilities=(make_mcp_capability(_SERVER),)),
            make_plan("quick", capabilities=(make_mcp_capability(_SERVER),)),
        ]
        # One engine is built per plan, in plan order, so the models line up
        # with ``plans`` and each agent drives its own script.
        models = iter((blocked_model, quick_model))
        provider = PydanticAIEnginePlugin(model_resolver=lambda target: next(models).as_model())
        runtime = AgentRuntime(
            plans=plans,
            config=_config(),
            engine_plugin=provider,
            deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
            container=LoomContainer(),
            mcp_client_factory=provider.mcp_client_factory,
        )

        async with runtime:
            blocked = asyncio.create_task(runtime.run("blocked", "slow", identity=caller))
            await asyncio.wait_for(server.entered.wait(), timeout=5)
            await asyncio.wait_for(runtime.run("quick", "fast", identity=caller), timeout=5)
            assert not blocked.done()
            server.released.set()
            await asyncio.wait_for(blocked, timeout=5)

        assert connections.opened == 1
        assert any("orders of beta" in returned for returned in quick_model.tool_returns)

    async def test_the_tool_timeout_still_bounds_the_agents_own_call(
        self, server: _Server, connections: _Connections, caller: Identity
    ) -> None:
        """``tool_timeout_ms`` cancels the call it belongs to, shared toolset or not."""
        model = ScriptedToolModel(calls=(("blocking_read", {"customer": "acme"}),))
        plan = make_plan(
            "impatient",
            capabilities=(make_mcp_capability(_SERVER),),
            policies=make_policies(tool_timeout_ms=100),
        )

        async with _runtime([plan], model) as runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run("impatient", "slow", identity=caller)
            server.released.set()

        assert failure.value.code is AgentRunErrorCode.TOOL_TIMEOUT
        assert connections.opened == 1


class TestTheWiringIsChecked:
    """Sharing only holds while one provider instance serves both halves."""

    async def test_start_up_refuses_a_factory_that_is_not_the_provider_s_own(
        self, connections: _Connections
    ) -> None:
        """A foreign client factory is a boot failure, not 1 + N connections.

        ``create_mcp_client`` opens a toolset of its own, so the runtime would
        validate one connection's tool filters and every agent would then run
        over another. Engines are built after the clients open, so a grant the
        shared store was never asked to open can only mean this.
        """
        model = ScriptedToolModel()
        plan = make_plan("clerk", capabilities=(make_mcp_capability(_SERVER),))
        provider = PydanticAIEnginePlugin(model_resolver=lambda target: model.as_model())
        runtime = AgentRuntime(
            plans=[plan],
            config=_config(),
            engine_plugin=provider,
            deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
            container=LoomContainer(),
            mcp_client_factory=create_mcp_client,
        )

        with pytest.raises(AgentCompilationError) as failure:
            await runtime.__aenter__()

        message = str(failure.value)
        assert _SERVER in message
        assert "clerk" in message

    async def test_start_up_refuses_a_second_provider_instance(
        self, connections: _Connections
    ) -> None:
        """Resolving the provider twice is the same hole with a friendlier face."""
        model = ScriptedToolModel()
        plan = make_plan("clerk", capabilities=(make_mcp_capability(_SERVER),))
        opener = PydanticAIEnginePlugin(model_resolver=lambda target: model.as_model())
        builder = PydanticAIEnginePlugin(model_resolver=lambda target: model.as_model())
        runtime = AgentRuntime(
            plans=[plan],
            config=_config(),
            engine_plugin=builder,
            deps=CapabilityDepsFactory(),  # type: ignore[arg-type]
            container=LoomContainer(),
            mcp_client_factory=opener.mcp_client_factory,
        )

        with pytest.raises(AgentCompilationError):
            await runtime.__aenter__()
