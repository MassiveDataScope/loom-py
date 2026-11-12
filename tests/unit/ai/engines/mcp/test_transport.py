"""The HTTP seam an ``http`` MCP server is reached through (plan 6.5).

``build_mcp_toolset`` builds the streamable-HTTP transport itself, so the
credentials sit on the transport and every connect gets a fresh client over the
generation's :class:`EvictableTransport`: the one place a freeze can drop the
pooled connections without ending the MCP session.
"""

from __future__ import annotations

import asyncio
import socket
import subprocess
import sys
import urllib.request
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx2
import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.engines.pydantic_ai import _mcp
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets, _ToolsetSession, build_mcp_toolset
from loom.ai.engines.pydantic_ai._transport import EvictableTransport, PooledHttpTransport
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.config.errors import ConfigError
from loom.core.model.redaction import Secret
from tests.unit.ai.engines.mcp.server import serving

pytest.importorskip(
    "pydantic_ai.mcp",
    reason="the MCP client is not installed: uv sync --group mcp-tests",
    exc_type=ImportError,
)

from fastmcp.client.auth import OAuth  # noqa: E402
from fastmcp.client.transports import StreamableHttpTransport  # noqa: E402

from loom.ai.compiler import CompiledRemoteAuth  # noqa: E402

_URL = "https://orders.example.com/mcp"


class _Bearer(httpx2.Auth):
    """A static credential, as a strategy hands it over."""

    def auth_flow(self, request: httpx2.Request) -> Any:
        request.headers["Authorization"] = "Bearer abc"
        yield request


def _capability(**overrides: Any) -> CompiledMcpCapability:
    return CompiledMcpCapability(server="orders", url=_URL, timeout_ms=20000, **overrides)


def _with_strategy(monkeypatch: pytest.MonkeyPatch, auth: object) -> CompiledMcpCapability:
    monkeypatch.setattr(_mcp, "shared_mcp_auth", lambda server, declared: auth)
    return _capability(auth=CompiledRemoteAuth(kind="token-service", settings=()))


class _Inner(httpx2.AsyncBaseTransport):
    """An inner transport that only records whether it was closed."""

    def __init__(self) -> None:
        self.closed = 0

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, request=request)

    async def aclose(self) -> None:
        self.closed += 1


class TestTheHttpTransportCarriesTheCredentials:
    def test_http_is_a_streamable_transport_with_the_headers_and_the_auth(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bearer = _Bearer()
        capability = _with_strategy(monkeypatch, bearer)
        capability = CompiledMcpCapability(
            server=capability.server,
            url=capability.url,
            timeout_ms=capability.timeout_ms,
            auth=capability.auth,
            headers_ref=Secret("X-Key=abc123"),
        )

        toolset = build_mcp_toolset(capability, init_timeout=10.0)

        transport = toolset.client.transport
        assert isinstance(transport, StreamableHttpTransport)
        assert transport.headers == {"X-Key": "abc123"}
        assert transport.auth is bearer

    def test_the_toolset_gets_neither_so_it_never_resolves_them_per_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toolset = build_mcp_toolset(_with_strategy(monkeypatch, _Bearer()), init_timeout=10.0)

        assert (toolset._static_headers, toolset._static_auth) == (None, None)
        assert toolset._resolves_per_run is False

    def test_the_pool_is_the_toolsets_own_never_a_borrowed_http_client(self) -> None:
        """``MCPToolset(http_client=...)`` drops the per-toolset headers and timeout."""
        toolset = build_mcp_toolset(_capability(), init_timeout=10.0)

        transport = toolset.client.transport
        assert isinstance(transport, PooledHttpTransport)
        assert transport.httpx_client_factory == transport.pool.client_factory

    def test_the_session_holds_the_pool_the_builder_made(self) -> None:
        toolset = build_mcp_toolset(_capability(), init_timeout=10.0)

        session = _ToolsetSession(toolset)

        assert session._current.pool is toolset.client.transport.pool

    def test_a_strategy_that_did_not_reach_the_transport_still_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        capability = _with_strategy(monkeypatch, _Bearer())
        monkeypatch.setattr(_mcp, "_client_auth", lambda sdk, auth: None)

        with pytest.raises(AgentCompilationError) as raised:
            build_mcp_toolset(capability, init_timeout=10.0)

        assert raised.value.issues[0].code is AgentErrorCode.MCP_AUTH_STRATEGY_INVALID

    def test_a_per_run_auth_callable_is_refused_naming_the_server(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        capability = _with_strategy(monkeypatch, lambda ctx: "per-run")
        monkeypatch.setattr(_mcp, "_client_auth", lambda sdk, auth: auth)

        with pytest.raises(ConfigError, match="'orders'"):
            build_mcp_toolset(capability, init_timeout=10.0)

    def test_the_oauth_sentinel_builds_over_the_same_factory(self) -> None:
        evictable = EvictableTransport()

        transport = StreamableHttpTransport(
            _URL, auth="oauth", httpx_client_factory=evictable.client_factory
        )

        assert isinstance(transport.auth, OAuth)
        assert transport.auth.httpx_client_factory == evictable.client_factory


class TestEachConnectGetsAFreshClient:
    async def test_three_connects_leave_one_session_id_hook_on_each_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clients: list[httpx2.AsyncClient] = []
        build = EvictableTransport.client_factory

        def recording(self: EvictableTransport, **kwargs: Any) -> httpx2.AsyncClient:
            clients.append(build(self, **kwargs))
            return clients[-1]

        monkeypatch.setattr(EvictableTransport, "client_factory", recording)
        async with serving(monkeypatch) as server:
            toolset = build_mcp_toolset(server.capability, init_timeout=10.0)
            for _ in range(3):
                async with toolset:
                    await toolset.client.call_tool_mcp("echo", {"text": "hi"})

        assert len({id(client) for client in clients}) == 3
        assert [len(client.event_hooks["response"]) for client in clients] == [1, 1, 1]
        assert server.runs["echo"] == 3


class TestEvictableTransport:
    async def test_closing_the_client_does_not_close_the_pool(self) -> None:
        inner = _Inner()
        transport = EvictableTransport(inner=lambda: inner)

        async with httpx2.AsyncClient(transport=transport):
            pass
        await transport.aclose()

        assert inner.closed == 0

    async def test_drop_swaps_in_a_new_pool_and_closes_the_old_one(self) -> None:
        old, new = _Inner(), _Inner()
        pools = [old, new]
        transport = EvictableTransport(inner=lambda: pools.pop(0))

        await transport.drop()
        response = await transport.handle_async_request(httpx2.Request("GET", _URL))

        assert (old.closed, new.closed) == (1, 0)
        assert response.status_code == 200
        assert pools == []

    async def test_release_closes_the_pool_for_good(self) -> None:
        inner = _Inner()
        transport = EvictableTransport(inner=lambda: inner)

        await transport.release()

        assert inner.closed == 1


class TestTheClientBehavesAsBefore:
    async def test_it_does_not_follow_a_redirect_even_when_fastmcp_asks(self) -> None:
        sent: list[str] = []

        def answer(request: httpx2.Request) -> httpx2.Response:
            sent.append(str(request.url))
            return httpx2.Response(307, headers={"Location": _URL + "/"})

        pool = EvictableTransport(inner=lambda: httpx2.MockTransport(answer))
        async with pool.client_factory(follow_redirects=True) as client:
            response = await client.post(_URL)

        assert (response.status_code, sent) == (307, [_URL])


class TestBehindAProxy:
    """Behind a proxy the toolset keeps the stock transport, as before: httpx2 routes it."""

    @pytest.mark.parametrize("variable", ["HTTPS_PROXY", "ALL_PROXY", "https_proxy"])
    def test_the_toolset_is_built_as_before_with_its_credentials(
        self, monkeypatch: pytest.MonkeyPatch, variable: str
    ) -> None:
        monkeypatch.setenv(variable, "http://proxy.internal:3128")
        bearer = _Bearer()
        capability = _with_strategy(monkeypatch, bearer)

        toolset = build_mcp_toolset(capability, init_timeout=10.0)

        transport = toolset.client.transport
        assert type(transport) is StreamableHttpTransport
        assert transport.httpx_client_factory is None
        assert transport.auth is bearer

    def test_static_headers_reach_the_stock_transport(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HTTPS_PROXY", "http://proxy.internal:3128")

        toolset = build_mcp_toolset(
            _capability(headers_ref=Secret("X-Key=abc123")), init_timeout=10.0
        )

        assert type(toolset.client.transport) is StreamableHttpTransport
        assert toolset.client.transport.headers == {"X-Key": "abc123"}

    def test_a_system_proxy_counts_too(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(urllib.request, "getproxies", lambda: {"https": "system:8080"})

        transport = build_mcp_toolset(_capability(), init_timeout=10.0).client.transport

        assert type(transport) is StreamableHttpTransport

    def test_a_proxy_for_the_other_scheme_keeps_the_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HTTP_PROXY", "http://proxy.internal:3128")

        transport = build_mcp_toolset(_capability(), init_timeout=10.0).client.transport

        assert isinstance(transport, PooledHttpTransport)

    def test_without_a_proxy_the_toolset_gets_the_pool(self) -> None:
        transport = build_mcp_toolset(_capability(), init_timeout=10.0).client.transport

        assert isinstance(transport, PooledHttpTransport)

    async def test_it_starts_through_a_proxy_named_without_a_scheme(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The proxy here is the server itself: a host that never resolves is reached through it."""
        async with _proxy_that_serves() as (address, requests):
            monkeypatch.setenv("HTTP_PROXY", address)
            capability = CompiledMcpCapability(
                server="orders", url="http://orders.invalid/mcp", timeout_ms=5000
            )
            shared = SharedMcpToolsets()

            async with shared.open(capability) as session:
                result = await session.call_tool("echo", {"text": "through"})

        assert result.structured == {"result": "through"}
        assert requests


@asynccontextmanager
async def _proxy_that_serves() -> AsyncIterator[tuple[str, list[str]]]:
    """A real MCP server on a local port, named as the proxy as ``host:port``, no scheme."""
    import uvicorn
    from fastmcp import FastMCP

    mcp = FastMCP("orders")

    @mcp.tool
    async def echo(text: str) -> str:
        return text

    inner = mcp.http_app(path="/mcp")
    requests: list[str] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            requests.append(scope["path"])
        await inner(scope, receive, send)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", lifespan="on")
    )
    serving = asyncio.create_task(server.serve())
    try:
        while not server.started:
            await asyncio.sleep(0.01)
        yield f"127.0.0.1:{port}", requests
    finally:
        server.should_exit = True
        await serving


def test_loading_the_engine_does_not_load_the_http_pool() -> None:
    """The pool module and fastmcp load with the first ``http`` toolset, not with the engine.

    httpx2 itself is already loaded by pydantic-ai's own dependencies
    (``genai_prices``), so the claim is about what this package adds.
    """
    script = (
        "import sys, loom.ai.engines.pydantic_ai\n"
        "print(sorted(m for m in ('loom.ai.engines.pydantic_ai._transport', 'fastmcp')"
        " if m in sys.modules))"
    )
    loaded = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert loaded.stdout.strip() == "[]"
