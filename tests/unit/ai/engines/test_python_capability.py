"""``python`` capability at build: how the engine calls the application factory.

The factory is application code the artifact names; the engine calls it once at
start-up. These tests pin the call shape — the :class:`ToolsetContext` that
arrives as the first positional, how the declared ``params`` arrive, and what
``remote()`` resolves — with recording factories, a counting stand-in for the
MCP toolset, no model and no network.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast

import anyio
import pytest
from pydantic_ai.toolsets import FunctionToolset

from loom.ai.abc import McpSession, ToolsetContext
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledCapability,
    CompiledMcpCapability,
    CompiledPythonCapability,
)
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import _capabilities, _mcp
from loom.ai.engines.pydantic_ai._guards import BuildContext, capability_call
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets, _OpenToolset, _ToolsetSession
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import (
    OPEN_OBJECT_SCHEMA,
    compiled_instructions,
    compiled_output,
)

FACTORY_REF = "myapp.tools.geo:build_geo_toolset"
"""Reference the artifact named; the compiler resolved it to the callable below."""

PARAMS: Mapping[str, Any] = {"max_results": 3, "radius_km": 25}
"""Keyword arguments the artifact declared under ``params``."""

AGENT = "geo-agent"
"""Name of the plan under build."""

SERVER = "geo-tools"
"""Server name the agent's ``mcp`` grant declares."""

MCP_GRANT = CompiledMcpCapability(server=SERVER, url="https://geo.internal/mcp")
"""The agent's own ``mcp`` grant on :data:`SERVER`."""


@dataclass
class RecordingFactory:
    """Factory that records every call it receives.

    Attributes:
        calls: ``(positional arguments, keyword arguments)`` per invocation.
    """

    calls: list[tuple[tuple[object, ...], Mapping[str, Any]]] = field(default_factory=list)

    def __call__(self, *args: object, **kwargs: Any) -> object:
        """Record the call and return an engine toolset."""
        self.calls.append((args, kwargs))
        return FunctionToolset()


@dataclass
class RemoteFactory:
    """Factory that resolves one remote in its body and keeps the session.

    Attributes:
        sessions: The session ``remote(server)`` returned, per successful build.
    """

    sessions: list[McpSession] = field(default_factory=list)

    def __call__(self, context: ToolsetContext, *, server: str) -> object:
        """Resolve ``server`` through the context, then return an engine toolset."""
        self.sessions.append(context.remote(server))
        return FunctionToolset()


class FailingFactory:
    """Factory that raises with a secret in the message, as application code might."""

    def __call__(self, context: ToolsetContext) -> object:
        """Raise before building anything."""
        del context
        raise ValueError("secret=abc")


@dataclass
class _FakeProtocolResult:
    """Stand-in for ``mcp.types.CallToolResult``: the two fields the adapter reads."""

    is_error: bool = False
    structured_content: object = None


@dataclass
class _FakeFastmcpClient:
    """Stand-in for the ``fastmcp.Client`` a real ``MCPToolset`` wraps.

    Attributes:
        calls: ``(tool name, arguments)`` per ``call_tool_mcp``.
    """

    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def call_tool_mcp(self, name: str, arguments: dict[str, Any]) -> _FakeProtocolResult:
        """Record the call the way the real client's raw method would forward it."""
        self.calls.append((name, arguments))
        return _FakeProtocolResult(structured_content={"tool": name})


class FakeMcpToolset:
    """Stand-in for the engine's ``MCPToolset``: enterable, wraps a fake client.

    Attributes:
        client: The fake client every call is forwarded to and recorded on.
    """

    def __init__(self) -> None:
        self.client = _FakeFastmcpClient()

    @property
    def calls(self) -> list[tuple[str, dict[str, Any]]]:
        """Calls the fake client recorded, exposed under the toolset for old assertions."""
        return self.client.calls

    async def __aenter__(self) -> FakeMcpToolset:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


@dataclass
class CountingToolsetBuilder:
    """Replacement for ``build_mcp_toolset`` that counts what it built.

    Attributes:
        built: Every toolset handed out, in build order.
    """

    built: list[FakeMcpToolset] = field(default_factory=list)

    def __call__(self, capability: CompiledMcpCapability, *, init_timeout: float) -> Any:
        """Build one fake toolset for the grant's connection."""
        del capability, init_timeout
        toolset = FakeMcpToolset()
        self.built.append(toolset)
        return toolset


def make_plan(*capabilities: CompiledCapability) -> AgentPlan:
    """Build a compiled plan granting ``capabilities`` and nothing else."""
    return AgentPlan(
        name=AGENT,
        description="looks up service points",
        instructions=compiled_instructions("answer the question"),
        spec_version=1,
        inference=InferenceTarget(provider="openai", model="gpt-5.2"),
        output=compiled_output(OPEN_OBJECT_SCHEMA),
        capabilities=capabilities,
        policies=PolicySpec(retries=0, tool_timeout_ms=5000),
        metadata={},
    )


@pytest.fixture
def toolset_builder(monkeypatch: pytest.MonkeyPatch) -> CountingToolsetBuilder:
    """Replace the engine's MCP toolset builder with a counting fake."""
    builder = CountingToolsetBuilder()
    monkeypatch.setattr(_mcp, "build_mcp_toolset", builder)
    return builder


async def opened(grant: CompiledMcpCapability) -> SharedMcpToolsets:
    """Return a store whose connection for ``grant`` the runtime already opened."""
    shared = SharedMcpToolsets()
    await shared.open(grant).__aenter__()
    return shared


def context_of(factory: RecordingFactory) -> ToolsetContext:
    """Return the first positional of the factory's only call."""
    assert len(factory.calls) == 1
    args, _ = factory.calls[0]
    return cast(ToolsetContext, args[0])


class TestFactoryCallShape:
    """The factory is called once with the context first and ``params`` as kwargs."""

    def test_factory_receives_the_context_and_the_params_at_build(self) -> None:
        """``factory(context, **params)``, exactly once."""
        factory = RecordingFactory()
        capability = CompiledPythonCapability(
            factory_ref=FACTORY_REF, factory=factory, params=PARAMS
        )
        container = LoomContainer()

        _capabilities.build_toolsets(make_plan(capability), container, mcp=SharedMcpToolsets())

        args, kwargs = factory.calls[0]
        assert len(args) == 1
        assert context_of(factory).container is container
        assert kwargs == PARAMS

    def test_factory_receives_only_the_context_without_params(self) -> None:
        """Without ``params`` the factory is called with the context alone."""
        factory = RecordingFactory()
        capability = CompiledPythonCapability(factory_ref=FACTORY_REF, factory=factory)
        container = LoomContainer()

        _capabilities.build_toolsets(make_plan(capability), container, mcp=SharedMcpToolsets())

        args, kwargs = factory.calls[0]
        assert len(args) == 1
        assert context_of(factory).container is container
        assert kwargs == {}

    def test_context_names_the_agent_and_carries_the_container(self) -> None:
        """AC3: ``agent`` is the plan name, ``container`` the application container."""
        factory = RecordingFactory()
        capability = CompiledPythonCapability(factory_ref=FACTORY_REF, factory=factory)
        container = LoomContainer()

        _capabilities.build_toolsets(make_plan(capability), container, mcp=SharedMcpToolsets())

        context = context_of(factory)
        assert context.agent == AGENT
        assert context.container is container
        assert callable(context.remote)


class TestRemote:
    """``remote()`` hands the factory the agent's own shared MCP session."""

    @pytest.mark.asyncio
    async def test_remote_calls_reach_the_toolset_the_mcp_grant_was_built_over(
        self, toolset_builder: CountingToolsetBuilder
    ) -> None:
        """AC4: one toolset per connection, shared by the ``mcp`` grant and the session."""
        factory = RemoteFactory()
        capability = CompiledPythonCapability(
            factory_ref=FACTORY_REF, factory=factory, params={"server": SERVER}
        )
        shared = await opened(MCP_GRANT)

        _capabilities.build_toolsets(make_plan(MCP_GRANT, capability), LoomContainer(), mcp=shared)
        await factory.sessions[0].call_tool("t", {"q": "harbour"})

        assert len(toolset_builder.built) == 1
        assert toolset_builder.built[0].calls == [("t", {"q": "harbour"})]

    @pytest.mark.asyncio
    async def test_remote_on_a_server_the_agent_was_not_granted_fails_at_build(
        self, toolset_builder: CountingToolsetBuilder
    ) -> None:
        """AC5: the worker knows the server, but this plan holds no grant on it."""
        factory = RemoteFactory()
        capability = CompiledPythonCapability(
            factory_ref=FACTORY_REF, factory=factory, params={"server": SERVER}
        )
        shared = await opened(MCP_GRANT)
        plan = make_plan(capability)
        container = LoomContainer()

        with pytest.raises(AgentCompilationError) as raised:
            _capabilities.build_toolsets(plan, container, mcp=shared)

        (issue,) = raised.value.issues
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_REMOTE_NOT_GRANTED,
            "capabilities.factory",
        )
        assert AGENT in issue.message
        assert FACTORY_REF in issue.message
        assert SERVER in issue.message
        assert factory.sessions == []
        assert len(toolset_builder.built) == 1


class TestFactoryFailure:
    """A factory that raises fails start-up with a coded issue, not a bare traceback."""

    def test_a_raising_factory_is_reported_by_class_name_without_its_message(self) -> None:
        """The issue names the agent, the factory and the class; the message stays private."""
        capability = CompiledPythonCapability(factory_ref=FACTORY_REF, factory=FailingFactory())
        plan = make_plan(capability)
        container = LoomContainer()
        mcp = SharedMcpToolsets()

        with pytest.raises(AgentCompilationError) as raised:
            _capabilities.build_toolsets(plan, container, mcp=mcp)

        (issue,) = raised.value.issues
        assert (issue.code, issue.field) == (
            AgentErrorCode.PYTHON_FACTORY_FAILED,
            "capabilities.factory",
        )
        assert AGENT in issue.message
        assert FACTORY_REF in issue.message
        assert "ValueError" in issue.message
        assert "abc" not in issue.message
        assert isinstance(raised.value.__cause__, ValueError)


class TestAbcImports:
    def test_abc_imports_on_a_fresh_interpreter(self) -> None:
        """AC3: moving ``McpSession`` into ``loom.ai.abc`` opened no import cycle."""
        completed = subprocess.run(
            [sys.executable, "-c", "import loom.ai.abc"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr


class _HungFastmcpClient:
    """Stand-in for the ``fastmcp.Client``: a call waits until the test releases it."""

    def __init__(self) -> None:
        self._release = asyncio.Event()
        self.has_entered = False

    @property
    def is_released(self) -> bool:
        """Whether the test has let the hung call finish."""
        return self._release.is_set()

    async def call_tool_mcp(self, name: str, arguments: dict[str, Any]) -> _FakeProtocolResult:
        """Wait for a release that the timeout test never sends."""
        del name, arguments
        self.has_entered = True
        await self._release.wait()
        return _FakeProtocolResult(structured_content=None)


class _HungFakeMcpToolset:
    """Stand-in for ``MCPToolset``: a real refcount behind a real lock, and an
    ``__aexit__`` that actually awaits at refcount zero -- mirroring
    ``MCPToolset`` closely enough that a cancelled call leaving the refcount
    unbalanced would show up here, not just by construction."""

    def __init__(self) -> None:
        self.client = _HungFastmcpClient()
        self._lock = anyio.Lock()
        self.running_count = 0

    async def __aenter__(self) -> _HungFakeMcpToolset:
        async with self._lock:
            self.running_count += 1
        return self

    async def __aexit__(self, *exc: object) -> None:
        async with self._lock:
            self.running_count -= 1
            if self.running_count == 0:
                await asyncio.sleep(0)


class TestKindPythonRespectsToolTimeout:
    """A ``kind: python`` factory that calls
    ``context.remote(server).call_tool(...)`` must still be cut off by the
    plan's ``tool_timeout_ms``, exactly as a ``usecase`` or ``mcp`` tool is.

    Before the fix, ``_ToolsetSession.call_tool`` shielded the whole round
    trip from cancellation, so ``capability_call``'s ``asyncio.timeout`` fired
    but the call kept running underneath it until the remote itself answered
    -- unbounded against a hung gateway. This drives the two real pieces of
    that route together: ``capability_call`` (the plan's own timeout) wrapping
    a real ``_ToolsetSession.call_tool`` (what a python factory's
    ``context.remote(server)`` returns), against a remote that never answers.
    """

    async def test_a_hung_remote_is_cut_off_by_the_plan_timeout(self) -> None:
        """The plan timeout cancels a remote that is still waiting."""
        context = BuildContext(
            agent=AGENT,
            container=LoomContainer(),
            observability=None,
            timeout_s=0.05,
            mcp=SharedMcpToolsets(),
            mcp_grants=(),
        )
        toolset = _HungFakeMcpToolset()
        session = _ToolsetSession(_OpenToolset.from_toolset(toolset))  # type: ignore[arg-type]

        async def call_until_cut_off() -> None:
            """Run the lookup until the plan timeout cancels it."""
            async with capability_call(context, "python", "lookup", Identity(subject="tester")):
                await session.call_tool("lookup", {})

        with pytest.raises(AgentRunError) as raised:
            await asyncio.wait_for(call_until_cut_off(), timeout=5)

        assert raised.value.code is AgentRunErrorCode.TOOL_TIMEOUT
        assert toolset.client.has_entered
        assert not toolset.client.is_released
        assert toolset.running_count == 0
