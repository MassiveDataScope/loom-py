"""The runtime's own wiring point decides whether a session is safe unwrapped.

``AgentRuntime._register_opened`` (``loom/ai/runtime/_lifecycle.py``) is the
one place a just-opened session becomes what a grant view calls through. It
must go through :func:`~loom.ai.runtime._mcp.mcp_session_for` to do that:
storing the session as-is would let a third-party session that declares
nothing — the deployment's own MCP client, not the engine's — run unguarded,
even though :func:`mcp_session_for` is fully tested on its own.

This test drives the real path end to end, through the public grant surface
(:meth:`~loom.ai.abc.AgentHandle.mcp`) rather than reaching into the
runtime's private state: a plain session that never declares
:class:`~loom.ai.abc.ConcurrentMcpSession` must still serialise two
concurrent calls once the runtime has opened it, because skipping
``mcp_session_for`` at start-up is indistinguishable from any other bug that
leaves a session unwrapped.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

from loom.ai.abc import McpToolCallResult, McpToolInfo
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import _AgentHandle
from loom.core.di import LoomContainer
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.stores.sql.service import NullSqlQueryService
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

_AGENT_NAME = "triage"
_MCP_SERVER = "tools"
_AUTHENTICATED = Identity(subject="ada", mechanism="test")


class _UndeclaredThirdPartySession:
    """A plain ``McpSession`` double: declares nothing, detects interleaving.

    Stands in for a deployment's own MCP client — the case ``mcp_session_for``
    exists for — the same way
    :class:`~tests.integration.ai.conftest.InterleavingSensitiveSession`
    does, but returns a mapping so it can go through
    :meth:`~loom.ai.abc.McpHandle.call_untyped` unmodified.
    """

    def __init__(self, *, delay_s: float = 0.02) -> None:
        self._delay_s = delay_s
        self._busy = False
        self.interleaved = False
        self.started: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return (McpToolInfo(name="echo", has_output_schema=False),)

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        del name
        token = str(arguments["token"])
        if self._busy:
            self.interleaved = True
        self._busy = True
        self.started.append(token)
        try:
            await asyncio.sleep(self._delay_s)
            return McpToolCallResult(ok=True, structured={"token": token})
        finally:
            self._busy = False


@pytest.fixture
def container() -> LoomContainer:
    """Empty application container; this test resolves nothing from it."""
    return LoomContainer()


class TestUndeclaredThirdPartySession:
    async def test_an_undeclared_third_party_session_stays_serialized(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A third-party session opened by the runtime still serialises its calls.

        Skipping ``mcp_session_for`` at the wiring point (storing the just-opened
        session verbatim in ``self._sessions``) would let these two calls
        interleave; this is what pins that it does not happen.
        """
        capability = make_mcp_capability(_MCP_SERVER)
        session = _UndeclaredThirdPartySession()
        client = StubMcpClient(label=_MCP_SERVER, session=session, log=[])
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME, capabilities=(capability,))],
            config=make_ai_config(mcp_servers=make_mcp_servers(_MCP_SERVER)),
            engine_provider=CountingEngineProvider(  # type: ignore[arg-type]
                engines={_AGENT_NAME: ScriptedEngine()}
            ),
            deps=deps,  # type: ignore[arg-type]
            container=container,
            mcp_client_factory=mcp_client_factory({_MCP_SERVER: client}),  # type: ignore[arg-type]
        )
        handle = _AgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with ExecutionScope(_AUTHENTICATED):
                mcp = handle.mcp(_MCP_SERVER)
                first = asyncio.create_task(mcp.call_untyped("echo", {"token": "a"}))
                while not session.started:
                    await asyncio.sleep(0)
                second = asyncio.create_task(mcp.call_untyped("echo", {"token": "b"}))
                results = await asyncio.gather(first, second)

        assert session.interleaved is False
        assert {result["token"] for result in results} == {"a", "b"}
