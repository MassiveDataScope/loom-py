"""A real FastMCP streamable-HTTP server served in process, with no socket.

Every pool an :class:`EvictableTransport` makes is an ASGI transport onto the
same app, so the whole client stack (pydantic-ai, fastmcp, mcp, httpx2) runs
unchanged while the server records each JSON-RPC method it receives.
"""

from __future__ import annotations

import asyncio
import functools
import json
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx2
import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.engines.pydantic_ai import _transport
from loom.ai.engines.pydantic_ai._transport import EvictableTransport

_Scope = MutableMapping[str, Any]
_Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
_Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]


@dataclass
class InProcessServer:
    """What the server saw, in order, and the knobs a test turns.

    Attributes:
        capability: A grant whose toolset reaches this server.
        log: JSON-RPC methods received, interleaved with what tests append.
        runs: How often each tool ran.
        release: Set to let a ``slow`` call finish.
    """

    capability: CompiledMcpCapability
    log: list[str] = field(default_factory=list)
    runs: Counter[str] = field(default_factory=Counter)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    def count(self, *methods: str) -> int:
        """Return how many requests carried one of *methods*."""
        return sum(entry in methods for entry in self.log)


def _app(server: InProcessServer) -> Any:
    from fastmcp import FastMCP

    mcp = FastMCP("orders")

    @mcp.tool
    async def echo(text: str) -> str:
        server.runs["echo"] += 1
        return text

    @mcp.tool
    async def slow() -> str:
        server.runs["slow"] += 1
        await server.release.wait()
        return "done"

    return mcp.http_app(path="/mcp")


def _logged(app: Any, log: list[str]) -> Callable[[_Scope, _Receive, _Send], Awaitable[None]]:
    async def call(scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] != "http":
            await app(scope, receive, send)
            return
        body = bytearray()

        async def peek() -> MutableMapping[str, Any]:
            message = await receive()
            body.extend(message.get("body", b""))
            if message["type"] == "http.request" and not message.get("more_body"):
                log.append(json.loads(body).get("method", "") if body else scope["method"])
            return message

        await app(scope, peek, send)

    return call


@asynccontextmanager
async def serving(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[InProcessServer]:
    """Serve one app for the block; every toolset ``build_mcp_toolset`` builds reaches it."""
    server = InProcessServer(
        capability=CompiledMcpCapability(
            server="orders", url="http://orders.test/mcp", timeout_ms=5000
        )
    )
    app = _app(server)
    asgi = _logged(app, server.log)

    def pool() -> httpx2.AsyncBaseTransport:
        return httpx2.ASGITransport(app=asgi)

    monkeypatch.setattr(
        _transport, "EvictableTransport", functools.partial(EvictableTransport, inner=pool)
    )
    async with app.router.lifespan_context(app):
        yield server
