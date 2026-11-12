"""A real FastMCP streamable-HTTP server served in process, with no socket.

Every pool an :class:`EvictableTransport` makes is an ASGI transport onto the
same app, so the whole client stack (pydantic-ai, fastmcp, mcp, httpx2) runs
unchanged while the server records each JSON-RPC method it receives.

A test scripts a fault for a tool's next calls (:attr:`InProcessServer.faults`):

* ``"forget"`` -- the server no longer knows the session: a 404 carrying the
  JSON-RPC error ``-32600 Session not found``, as the mcp server SDK sends it,
  and nothing ran;
* ``"cut"`` -- the tool runs, then the connection drops before the response;
* an ``int`` -- a JSON-RPC error with that code, as a middleware would send.
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

Fault = str | int
"""``"forget"``, ``"cut"`` or a JSON-RPC error code; see the module docstring."""

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
        faults: Per tool, the faults its next calls meet, one per call.
        down: While set, no connection reaches the server at all.
    """

    capability: CompiledMcpCapability
    log: list[str] = field(default_factory=list)
    runs: Counter[str] = field(default_factory=Counter)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    faults: dict[str, list[Fault]] = field(default_factory=dict)
    down: bool = False

    def fault(self, tool: str, *faults: Fault) -> None:
        """Make the next calls to *tool* meet *faults*, in order."""
        self.faults.setdefault(tool, []).extend(faults)

    def _next_fault(self, request: dict[str, Any]) -> Fault | None:
        if request.get("method") != "tools/call":
            return None
        queued = self.faults.get(request.get("params", {}).get("name", ""))
        return queued.pop(0) if queued else None

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


def _logged(
    app: Any, server: InProcessServer
) -> Callable[[_Scope, _Receive, _Send], Awaitable[None]]:
    async def call(scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] != "http":
            await app(scope, receive, send)
            return
        if server.down:
            raise httpx2.ConnectError("All connection attempts failed")
        messages = [await receive()]
        while messages[-1]["type"] == "http.request" and messages[-1].get("more_body"):
            messages.append(await receive())
        body = b"".join(message.get("body", b"") for message in messages)
        request = json.loads(body) if body else {}
        server.log.append(request.get("method", "") if body else scope["method"])

        async def replay() -> MutableMapping[str, Any]:
            return messages.pop(0) if messages else await receive()

        fault = server._next_fault(request)
        if fault is None:
            await app(scope, replay, send)
        elif fault == "forget":
            await _error(send, 404, request, -32600, "Session not found")
        elif fault == "cut":
            await app(scope, replay, _discarded)
            raise httpx2.RemoteProtocolError("Server disconnected without sending a response.")
        else:
            await _error(send, 200, request, int(fault), "sent by a middleware")

    return call


async def _error(
    send: _Send, status: int, request: dict[str, Any], code: int, message: str
) -> None:
    reply = {"jsonrpc": "2.0", "id": request.get("id"), "error": {"code": code, "message": message}}
    headers = [(b"content-type", b"application/json")]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": json.dumps(reply).encode()})


async def _discarded(message: MutableMapping[str, Any]) -> None:
    del message


@asynccontextmanager
async def serving(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[InProcessServer]:
    """Serve one app for the block; every toolset ``build_mcp_toolset`` builds reaches it."""
    server = InProcessServer(
        capability=CompiledMcpCapability(
            server="orders", url="http://orders.test/mcp", timeout_ms=5000
        )
    )
    app = _app(server)
    asgi = _logged(app, server)

    def pool() -> httpx2.AsyncBaseTransport:
        return httpx2.ASGITransport(app=asgi)

    monkeypatch.setattr(
        _transport, "EvictableTransport", functools.partial(EvictableTransport, inner=pool)
    )
    async with app.router.lifespan_context(app):
        yield server
