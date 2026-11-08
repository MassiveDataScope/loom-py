"""The MCP client (``mcp`` extra): pydantic-ai's toolset, fastmcp's stdio transport,
and the ``httpx2`` adapter that presents a loom request hook as client auth."""

from __future__ import annotations

from collections.abc import Callable, Generator

import httpx2
from fastmcp.client.transports import StdioTransport
from pydantic_ai.mcp import MCPToolset

__all__ = ["MCPToolset", "RequestHookAuth", "StdioTransport"]


class RequestHookAuth(httpx2.Auth):
    """A loom request hook (``request -> request``) as an ``httpx2.Auth``.

    ``loom.ai.remote_auth`` strategies return a plain callable, the one shape
    both ``httpx`` and ``httpx2`` accept.  ``MCPToolset`` from pydantic-ai 2.66
    reads any callable ``auth`` as a function of the run context instead, so the
    MCP boundary hands it this object: an ``Auth`` instance is not callable and
    reaches the transport as the static auth it is on every release.

    Args:
        hook: The strategy's request hook; it sets the credentials on the request.
    """

    def __init__(self, hook: Callable[[httpx2.Request], httpx2.Request]) -> None:
        self._hook = hook

    @property
    def hook(self) -> Callable[[httpx2.Request], httpx2.Request]:
        """The strategy's request hook this object presents."""
        return self._hook

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response]:
        """Send the request with the credentials the hook sets."""
        yield self._hook(request)

    def __repr__(self) -> str:
        # The credentials live inside the hook's closure; the repr names none of them.
        return f"{type(self).__name__}()"
