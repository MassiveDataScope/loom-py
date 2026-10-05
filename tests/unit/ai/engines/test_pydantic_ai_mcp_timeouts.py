"""The two MCP deadlines reach the engine (spec 016, T601, AC-011).

``init_timeout`` (the handshake budget, derived from ``ai.startup_timeout_ms``)
and ``read_timeout`` (the per-call deadline, derived from
``McpServerConfig.timeout_ms``) both reach ``pydantic_ai.mcp.MCPToolset``.
The MCP client is a real object here, not a double: the exact values are
asserted on ``toolset.client``, following plan.md's rule that the engine
boundary is inspected, not inferred.

Under the SSE transport, ``read_timeout`` doubles as ``sse_read_timeout`` —
the idle-stream deadline, not the per-call one — and only when the toolset
builds its transport explicitly, which happens exactly when the server
declares ``headers_ref`` or ``auth``. A server with neither keeps FastMCP's
own inferred transport and is unaffected. Both shapes are covered below,
deliberately.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.engines.pydantic_ai import _mcp
from loom.ai.engines.pydantic_ai._mcp import (
    DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS,
    SharedMcpToolsets,
    build_mcp_toolset,
)

pytest.importorskip(
    "pydantic_ai.mcp",
    reason="the MCP client is not installed: uv sync --group mcp-tests",
    exc_type=ImportError,
)

from fastmcp.client.transports.base import ClientTransport  # noqa: E402
from mcp.types import Implementation, InitializeResult, ServerCapabilities  # noqa: E402

_STREAMABLE_CAPABILITY = CompiledMcpCapability(
    server="orders", url="https://orders.example.com/mcp", timeout_ms=20000
)
_SSE_NO_CREDENTIAL = CompiledMcpCapability(
    server="orders", url="https://orders.example.com/sse", timeout_ms=20000
)
_SSE_WITH_HEADERS = CompiledMcpCapability(
    server="orders",
    url="https://orders.example.com/sse",
    headers_ref="X-Key=abc123",
    timeout_ms=20000,
)


class TestBuildMcpToolset:
    """``build_mcp_toolset`` passes both deadlines to the real MCP client."""

    def test_init_timeout_reaches_the_client(self) -> None:
        toolset = build_mcp_toolset(_STREAMABLE_CAPABILITY, init_timeout=7.0)

        assert toolset.client._init_timeout == 7.0

    def test_read_timeout_reaches_the_client_derived_from_timeout_ms(self) -> None:
        capability = CompiledMcpCapability(
            server="orders", url="https://orders.example.com/mcp", timeout_ms=45000
        )

        toolset = build_mcp_toolset(capability, init_timeout=10.0)

        session_kwargs = cast("dict[str, Any]", toolset.client._session_kwargs)
        assert session_kwargs["read_timeout_seconds"] == 45.0

    def test_streamable_http_transport_is_unaffected_by_read_timeout(self) -> None:
        """The per-call deadline governs the streamable-http transport alone."""
        toolset = build_mcp_toolset(_STREAMABLE_CAPABILITY, init_timeout=10.0)

        assert not hasattr(toolset.client.transport, "sse_read_timeout")


class TestSseReadTimeoutCoupling:
    """``read_timeout`` also becomes ``sse_read_timeout``, only when explicit."""

    def test_sse_read_timeout_equals_timeout_ms_when_the_server_declares_a_credential(
        self,
    ) -> None:
        toolset = build_mcp_toolset(_SSE_WITH_HEADERS, init_timeout=10.0)

        assert toolset.client.transport.sse_read_timeout.total_seconds() == 20.0

    def test_sse_read_timeout_is_untouched_when_the_server_declares_no_credential(self) -> None:
        """FastMCP's own inferred transport is used, and our ``read_timeout`` never reaches it."""
        toolset = build_mcp_toolset(_SSE_NO_CREDENTIAL, init_timeout=10.0)

        assert toolset.client.transport.sse_read_timeout is None


def test_the_default_connect_timeout_is_pinned_to_ten_seconds() -> None:
    """The constant itself, pinned against a literal.

    Catches a regression in *this* constant alone, including one that also
    changes ``AiConfig.startup_timeout_ms`` to match: the cross-check below
    would stay green in that case, because both sides would have moved
    together. This literal is what catches that day.
    """
    assert DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS == 10.0


def test_the_default_connect_timeout_still_matches_ai_configs_own_default() -> None:
    """The two defaults are meant to agree; this catches the day only one of them drifts.

    Kept *beside* the literal pin above, not instead of it: this is exactly
    the comparison the literal pin cannot make, because it never reads
    ``AiConfig`` at all — and a regression that moves the two defaults apart
    (never together) is precisely the case this cross-check exists to catch.
    """
    from loom.ai.config import AiConfig

    config = AiConfig(engine="pydantic-ai", specs=(), models={})
    assert config.startup_timeout_ms / 1000 == DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS


class TestSharedMcpToolsets:
    """The connect deadline is per-instance and reaches every toolset it builds."""

    def test_default_connect_timeout_matches_the_published_constant(self) -> None:
        shared = SharedMcpToolsets()

        toolset = shared._toolset(_STREAMABLE_CAPABILITY)

        assert toolset.client._init_timeout == DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS

    def test_a_custom_connect_timeout_reaches_every_toolset_it_builds(self) -> None:
        shared = SharedMcpToolsets()
        shared.set_connect_timeout(3.0)

        toolset = shared._toolset(_STREAMABLE_CAPABILITY)

        assert toolset.client._init_timeout == 3.0

    def test_set_connect_timeout_changes_the_deadline_of_the_next_toolset_built(self) -> None:
        shared = SharedMcpToolsets()
        shared.set_connect_timeout(3.0)

        shared.set_connect_timeout(9.0)
        toolset = shared._toolset(_STREAMABLE_CAPABILITY)

        assert toolset.client._init_timeout == 9.0


class _SlowInitializeSession:
    """Stand-in for ``mcp.ClientSession`` whose ``initialize()`` takes ``delay`` seconds.

    Everything else about a real handshake — the transport connecting, tools
    being listed — is irrelevant to what
    :paramref:`~pydantic_ai.mcp.MCPToolset.init_timeout` bounds: "the initial
    connection and the ``initialize`` handshake", so only the one RPC the
    deadline actually wraps is scripted.
    """

    def __init__(self, delay: float) -> None:
        self._delay = delay
        self.protocol_version = None

    async def initialize(self) -> InitializeResult:
        await asyncio.sleep(self._delay)
        # The SDK's ``InitializeResult`` aliases these two fields to
        # camelCase; pydantic's runtime constructor accepts the alias, which
        # pyright's stub does not model, so the call goes through ``Any``.
        build_result = cast("Any", InitializeResult)
        return cast(
            InitializeResult,
            build_result(
                protocolVersion="2025-06-18",
                capabilities=ServerCapabilities(),
                serverInfo=Implementation(name="fake", version="0"),
            ),
        )


class _SlowInitializeTransport(ClientTransport):
    """Fake transport connecting instantly, then stalling in ``initialize()``."""

    legacy_only = True

    def __init__(self, delay: float) -> None:
        self._delay = delay

    def connect_session(  # type: ignore[override]
        self, *, transport_options: Any = None, **session_kwargs: Any
    ) -> Any:
        del transport_options, session_kwargs
        return self._connect(self._delay)

    @staticmethod
    @contextlib.asynccontextmanager
    async def _connect(delay: float) -> AsyncIterator[_SlowInitializeSession]:
        yield _SlowInitializeSession(delay)


class TestHandshakeDeadline:
    """AC-011: the handshake survives under a generous budget, dies under a tight one.

    ``_mcp_client`` — the private helper that resolves a capability's
    transport — is the one seam substituted; :func:`build_mcp_toolset` itself
    runs unmodified, so this exercises the real wiring end to end. The delay
    is scaled down from the acceptance text's illustrative "seven seconds" so
    the suite stays fast; the claim is identical at any scale: a handshake
    slower than the engine's own undocumented five seconds and faster than
    ``ai.startup_timeout_ms`` now succeeds, and one slower than the deadline
    still fails.
    """

    @staticmethod
    def _capability_taking(delay: float) -> tuple[CompiledMcpCapability, float]:
        return (
            CompiledMcpCapability(server="orders", url="https://orders.example.com/mcp"),
            delay,
        )

    async def test_boots_when_the_handshake_fits_the_configured_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        capability, delay = self._capability_taking(0.2)
        fake_client = lambda component, cap: _SlowInitializeTransport(delay)  # noqa: E731
        monkeypatch.setattr(_mcp, "_mcp_client", fake_client)

        toolset = build_mcp_toolset(capability, init_timeout=5.0)
        async with toolset:
            pass

    async def test_fails_when_the_handshake_exceeds_the_configured_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        capability, delay = self._capability_taking(0.2)
        fake_client = lambda component, cap: _SlowInitializeTransport(delay)  # noqa: E731
        monkeypatch.setattr(_mcp, "_mcp_client", fake_client)

        toolset = build_mcp_toolset(capability, init_timeout=0.05)
        # The engine's own connection failure, not the scripted timeout
        # itself: a bare ``Exception`` here would also pass on a mis-signed
        # monkeypatch raising ``TypeError``, which proves nothing about the
        # deadline.
        with pytest.raises(RuntimeError, match="failed to connect"):
            async with toolset:
                pass
