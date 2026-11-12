"""A real streamable-HTTP FastMCP server on a local port, behind a TCP proxy the test controls.

The server and the proxy run on an event loop of their own, in a thread, so a
Lambda handler -- which runs its application on a loop of its own -- reaches
them over real sockets. What a test can do to them:

* read the request log (each request's JSON-RPC method, with the TCP
  connection it came on and the ``Authorization`` header it carried), how often
  each tool ran, and the TCP connects the proxy accepted;
* :meth:`LiveMcpServer.stale` -- every open connection goes silently dead: the
  server side is closed, the client is told nothing, and the client's next
  write on it is answered with an RST, as a connection a frozen Lambda kept
  past the server's idle timeout;
* :meth:`LiveMcpServer.forget` -- the server no longer knows any session it
  issued, so the mcp server SDK itself answers ``404 Session not found``
  (handshake era only; a modern-era client holds no session);
* :meth:`LiveMcpServer.cut_next_call` -- the next ``tools/call`` reaches the
  server and its tool runs, then the connection is reset before any response
  byte reaches the client.

The era is chosen by the server: ``"modern"`` is FastMCP 4's own, with which
fastmcp's client negotiates a stateless session; ``"legacy"`` rejects
``server/discover`` so the unmodified client falls back to the
``initialize`` handshake, a session id and a GET event stream.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import struct
import threading
from collections import Counter
from collections.abc import Awaitable, Callable, Iterator, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Literal

import uvicorn

Era = Literal["modern", "legacy"]

_Scope = MutableMapping[str, Any]
_Message = MutableMapping[str, Any]
_Receive = Callable[[], Awaitable[_Message]]
_Send = Callable[[_Message], Awaitable[None]]
_SESSION_HEADER = b"mcp-session-id"
_START_BUDGET_S = 10.0
_CUT_BUDGET_S = 5.0


@dataclass(frozen=True)
class Request:
    """One HTTP request the server received.

    Attributes:
        connection: The proxy's upstream port, one per client TCP connection.
        verb: The HTTP method.
        method: The JSON-RPC method, or ``""`` for a request without a body.
        authorization: The ``Authorization`` header sent, if any.
    """

    connection: int
    verb: str
    method: str
    authorization: str | None


@dataclass
class _Connection:
    client: asyncio.StreamWriter
    upstream: asyncio.StreamWriter
    stale: bool = False
    cut: bool = False


def _reset(writer: asyncio.StreamWriter) -> None:
    """Close *writer*'s socket with an RST rather than a FIN."""
    sock = writer.get_extra_info("socket")
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    writer.transport.abort()


class _Proxy:
    """A TCP proxy in front of the server, counting connects and injecting faults."""

    def __init__(self, upstream_port: int, tool_ran: asyncio.Event) -> None:
        self._upstream_port = upstream_port
        self._tool_ran = tool_ran
        self._open: list[_Connection] = []
        self._server: asyncio.Server | None = None
        self.accepted = 0
        self.cut_armed = False
        self.port = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.accepted += 1
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self._upstream_port)
        connection = _Connection(writer, up_writer)
        self._open.append(connection)
        try:
            await asyncio.gather(
                self._to_server(connection, reader), self._to_client(connection, up_reader)
            )
        finally:
            self._open.remove(connection)
            for side in (writer, up_writer):
                side.close()
                with contextlib.suppress(OSError):
                    await side.wait_closed()

    async def _to_server(self, connection: _Connection, reader: asyncio.StreamReader) -> None:
        with contextlib.suppress(OSError):
            while data := await reader.read(65536):
                if connection.stale:
                    _reset(connection.client)
                    return
                cutting = self.cut_armed and b'"tools/call"' in data
                if cutting:
                    self.cut_armed = False
                    connection.cut = True
                    self._tool_ran.clear()
                connection.upstream.write(data)
                await connection.upstream.drain()
                if cutting:
                    await asyncio.wait_for(self._tool_ran.wait(), _CUT_BUDGET_S)
                    _reset(connection.client)
                    return
        with contextlib.suppress(OSError):
            connection.upstream.close()

    async def _to_client(self, connection: _Connection, reader: asyncio.StreamReader) -> None:
        with contextlib.suppress(OSError):
            while data := await reader.read(65536):
                if connection.stale or connection.cut:
                    return
                connection.client.write(data)
                await connection.client.drain()
        if not connection.stale:
            with contextlib.suppress(OSError):
                connection.client.close()

    def stale(self) -> None:
        for connection in self._open:
            connection.stale = True
            connection.upstream.close()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
        for connection in list(self._open):
            connection.client.transport.abort()
            connection.upstream.transport.abort()


@dataclass
class LiveMcpServer:
    """What the server saw and the faults a test injects; see the module docstring.

    Attributes:
        era: The protocol era the server makes the client speak.
        log: Every request received, in order.
        runs: How often each tool ran.
        release: Set by a test to let a ``slow`` call answer.
        started: Set once a ``slow`` call is running on the server.
    """

    era: Era
    log: list[Request] = field(default_factory=list)
    runs: Counter[str] = field(default_factory=Counter)
    _issued: set[bytes] = field(default_factory=set)
    _forgotten: set[bytes] = field(default_factory=set)
    release: threading.Event = field(default_factory=threading.Event)
    started: threading.Event = field(default_factory=threading.Event)
    _loop: asyncio.AbstractEventLoop | None = None
    _proxy: _Proxy | None = None

    @property
    def url(self) -> str:
        """The URL a client reaches the server at, through the proxy."""
        assert self._proxy is not None
        return f"http://127.0.0.1:{self._proxy.port}/mcp"

    @property
    def connects(self) -> int:
        """TCP connections the client opened so far."""
        assert self._proxy is not None
        return self._proxy.accepted

    def stale(self) -> None:
        """Make every open connection RST on the client's next write; see the module docstring."""
        self._in_loop(self._proxied().stale)

    def forget(self) -> None:
        """Make the server answer every session it issued so far as unknown."""
        self._forgotten |= self._issued

    def cut_next_call(self) -> None:
        """Let the next ``tools/call`` run its tool, then reset the connection before a reply."""
        self._proxied().cut_armed = True

    def _proxied(self) -> _Proxy:
        assert self._proxy is not None
        return self._proxy

    def _in_loop(self, action: Callable[[], None]) -> None:
        assert self._loop is not None

        async def run() -> None:
            action()

        asyncio.run_coroutine_threadsafe(run(), self._loop).result(_START_BUDGET_S)

    def _app(self, tool_ran: asyncio.Event) -> Any:
        from fastmcp import FastMCP

        mcp = FastMCP("orders")

        @mcp.tool
        async def echo(text: str) -> str:
            """Return *text*."""
            self.runs["echo"] += 1
            return text

        @mcp.tool
        async def book(text: str) -> str:
            """Record *text*; the call a test cuts."""
            self.runs["book"] += 1
            tool_ran.set()
            return text

        @mcp.tool
        async def slow(text: str) -> str:
            """Answer *text* once the test sets :attr:`release`; the call left in flight."""
            self.runs["slow"] += 1
            self.started.set()
            while not self.release.is_set():
                await asyncio.sleep(0.01)
            return text

        return self._logged(mcp.http_app(path="/mcp"))

    def _logged(self, app: Any) -> Callable[[_Scope, _Receive, _Send], Awaitable[None]]:
        async def call(scope: _Scope, receive: _Receive, send: _Send) -> None:
            if scope["type"] != "http":
                await app(scope, receive, send)
                return
            messages = [await receive()]
            while messages[-1]["type"] == "http.request" and messages[-1].get("more_body"):
                messages.append(await receive())
            body = b"".join(message.get("body", b"") for message in messages)
            request = json.loads(body) if body else {}
            headers = dict(scope["headers"])
            authorization = headers.get(b"authorization")
            self.log.append(
                Request(
                    connection=scope["client"][1],
                    verb=scope["method"],
                    method=request.get("method", "") if isinstance(request, dict) else "",
                    authorization=authorization.decode() if authorization else None,
                )
            )
            if self.era == "legacy" and request.get("method") == "server/discover":
                await _method_not_found(send, request)
                return
            if headers.get(_SESSION_HEADER) in self._forgotten:
                scope["headers"] = [
                    (name, b"forgotten" if name == _SESSION_HEADER else value)
                    for name, value in scope["headers"]
                ]

            async def replay() -> _Message:
                return messages.pop(0) if messages else await receive()

            async def issued(message: _Message) -> None:
                if message["type"] == "http.response.start":
                    for name, value in message.get("headers", ()):
                        if name.lower() == _SESSION_HEADER:
                            self._issued.add(value)
                await send(message)

            await app(scope, replay, issued)

        return call


async def _method_not_found(send: _Send, request: dict[str, Any]) -> None:
    """Answer ``server/discover`` as a handshake-era server does: an unknown method."""
    reply = {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "error": {"code": -32601, "message": "Method not found"},
    }
    headers = [(b"content-type", b"application/json")]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": json.dumps(reply).encode()})


@contextlib.contextmanager
def serving(era: Era) -> Iterator[LiveMcpServer]:
    """Run the server and its proxy on a loop in a thread of their own, for the block."""
    server = LiveMcpServer(era=era)
    ready = threading.Event()
    stop: asyncio.Event | None = None
    failure: list[BaseException] = []

    async def main() -> None:
        nonlocal stop
        stop = asyncio.Event()
        tool_ran = asyncio.Event()
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        config = uvicorn.Config(
            server._app(tool_ran), log_level="warning", lifespan="on", timeout_keep_alive=60
        )
        http = uvicorn.Server(config)
        serve = asyncio.create_task(http.serve(sockets=[sock]))
        while not http.started:
            await asyncio.sleep(0.01)
        proxy = _Proxy(sock.getsockname()[1], tool_ran)
        await proxy.start()
        server._loop, server._proxy = asyncio.get_running_loop(), proxy
        ready.set()
        await stop.wait()
        await proxy.stop()
        http.should_exit = True
        await serve
        sock.close()

    def run() -> None:
        try:
            asyncio.run(main())
        except BaseException as exc:  # noqa: BLE001 - reported to the test thread
            failure.append(exc)
            ready.set()

    thread = threading.Thread(target=run, name="loom-test-mcp", daemon=True)
    thread.start()
    if not ready.wait(_START_BUDGET_S) or failure:
        raise AssertionError(f"the test MCP server did not start: {failure}")
    try:
        yield server
    finally:
        assert server._loop is not None and stop is not None
        server._loop.call_soon_threadsafe(stop.set)
        thread.join(_START_BUDGET_S)
