"""The HTTP pool of one MCP toolset, which a thawed process drops (plan 6.5).

A frozen Lambda keeps its pooled keep-alive connections while the server closes
them, and httpcore2 only expires an idle connection by a monotonic clock that did
not move during the freeze, so the first request after a thaw would be written
to a dead socket. :class:`EvictableTransport` is the pool of one toolset. Every
caller of the toolset goes through it -- a grant view, the model's own tool
calls, a ``kind: python`` factory -- so it is the one place that sees all the
traffic: before each request it reads the wall clock, and after a gap longer
than httpcore2's own keep-alive expiry, with no request of its own in flight, it
swaps in a fresh pool. The MCP session is untouched.

fastmcp builds a fresh ``httpx2.AsyncClient`` over the pool on every connect
(:meth:`EvictableTransport.client_factory`); closing that client closes nothing.
:class:`PooledHttpTransport` is fastmcp's streamable-HTTP transport carrying its
pool, so the code that builds a toolset hands the pool on with it.

A client handed its own transport ignores the environment's proxies, so this pool
serves only a server reached directly: behind a proxy the toolset keeps
fastmcp's stock transport, which applies httpx2's own proxy rules, and a
thawed call relies on the session's safe retry and renewal instead.

This module imports fastmcp and httpx2, so it is loaded with the first ``http``
toolset, never with the engine.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from typing import Any, Final, cast

import httpx2
from fastmcp.client.transports import StreamableHttpTransport

KEEPALIVE_EXPIRY_SECONDS: Final[float] = 5.0
"""httpcore2's default keep-alive expiry (``httpx2._config``): a gap longer than
this means every pooled connection is one httpcore2 would not reuse either."""

_MCP_DEFAULT_TIMEOUT: Final = httpx2.Timeout(30.0, read=300.0)
"""What ``mcp.shared._httpx_utils.create_mcp_http_client`` uses when given no timeout."""


class _Tracked(httpx2.AsyncByteStream):
    """A response body that reports, once, when it is closed."""

    def __init__(self, stream: httpx2.AsyncByteStream, done: Callable[[], None]) -> None:
        self._stream = stream
        self._done: Callable[[], None] | None = done

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._stream:
            yield chunk

    async def aclose(self) -> None:
        try:
            await self._stream.aclose()
        finally:
            done, self._done = self._done, None
            if done is not None:
                done()


class EvictableTransport(httpx2.AsyncBaseTransport):
    """A pool that outlives the clients built over it, and drops itself after a freeze.

    A request is in flight from the moment it is sent until its response body
    is closed. A ``GET`` is not counted: in the handshake era it is the
    server's event stream, open for the session's whole life, and counting it
    would keep a stale pool forever. A drop cuts that stream; the mcp client
    reopens it on its own.

    Args:
        inner: Builds a fresh pool; the default is httpx2's own.

    Attributes:
        last_use: Wall-clock time a request last started or finished.
    """

    def __init__(self, inner: Callable[[], httpx2.AsyncBaseTransport] | None = None) -> None:
        self._new_pool = inner or httpx2.AsyncHTTPTransport
        self._pool = self._new_pool()
        self._in_flight = 0
        self.last_use = time.time()

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        """Send *request*, after dropping the pool if a freeze left it stale."""
        now = time.time()
        gap, self.last_use = now - self.last_use, now
        if gap > KEEPALIVE_EXPIRY_SECONDS and not self._in_flight:
            await self.drop()
        if request.method == "GET":
            return await self._pool.handle_async_request(request)
        self._in_flight += 1
        try:
            response = await self._pool.handle_async_request(request)
        except BaseException:
            self._finished()
            raise
        if response.is_closed:  # a body read already, as a mock or ASGI pool may hand over
            self._finished()
        else:
            response.stream = _Tracked(
                cast("httpx2.AsyncByteStream", response.stream), self._finished
            )
        return response

    def _finished(self) -> None:
        """Count a request out and stamp the clock, so a long call does not read as a gap."""
        self._in_flight -= 1
        self.last_use = time.time()

    async def aclose(self) -> None:
        """Do nothing: fastmcp closes its client at every session exit, never the pool."""

    async def drop(self) -> None:
        """Swap in a new pool, then close the old one and every connection in it.

        The swap happens before the first await, so no request can reach the
        old pool once this started.
        """
        old, self._pool = self._pool, self._new_pool()
        await old.aclose()

    async def release(self) -> None:
        """Close the pool for good, when its toolset closes."""
        await self._pool.aclose()

    def client_factory(
        self,
        headers: dict[str, str] | None = None,
        timeout: httpx2.Timeout | None = None,
        auth: httpx2.Auth | None = None,
        **kwargs: Any,
    ) -> httpx2.AsyncClient:
        """Build the client fastmcp asks for on each connect, over this pool.

        fastmcp asks for ``follow_redirects=True``; it is ignored, as the mcp
        client's own factory does: the transport follows a redirect within the
        server's origin itself, and nothing else. Taking ``**kwargs`` also
        serves the ``"oauth"`` sentinel, whose flow calls this with nothing.
        """
        kwargs.pop("follow_redirects", None)
        return httpx2.AsyncClient(
            headers=headers,
            timeout=timeout or _MCP_DEFAULT_TIMEOUT,
            auth=auth,
            transport=self,
            **kwargs,
        )


class PooledHttpTransport(StreamableHttpTransport):
    """fastmcp's streamable-HTTP transport over an :class:`EvictableTransport` it carries.

    Args:
        url: The server's validated URL.
        headers: Static headers sent on every request.
        auth: Static auth, or the ``"oauth"`` sentinel.

    Attributes:
        pool: The pool every client of this transport goes through.
    """

    def __init__(self, url: str, *, headers: dict[str, str] | None, auth: object) -> None:
        self.pool = EvictableTransport()
        super().__init__(
            url,
            headers=headers,
            auth=cast("Any", auth),
            httpx_client_factory=self.pool.client_factory,
        )
