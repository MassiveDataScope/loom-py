"""Outbound MCP transport: this agent calling a remote tool server.

One ``MCPToolset`` per connection serves the whole worker, and
:class:`SharedMcpToolsets` is what holds them:

* :meth:`SharedMcpToolsets.open` is the
  :data:`~loom.ai.runtime.McpClientFactory` the runtime opens at start-up.
  Entering it connects the server and hands back a session, so an unreachable
  server fails start-up as ``MCP_SERVER_UNREACHABLE`` and the declared tool
  filters are validated against the tools the server really exposes (FR-025)
  instead of being taken on trust.
* :meth:`SharedMcpToolsets.toolset` is what the engine puts behind the
  capability boundary for the run itself — the *same object*, so the start-up
  connection is the run's connection and a worker holds one channel per server
  rather than one per agent (FR-026).

What is shared is the toolset, and :class:`_ToolsetSession` — what a grant
view (:meth:`~loom.ai.abc.AgentHandle.mcp`, a use case's ``Mcp()``) calls
through — declares itself with
:class:`~loom.ai.abc.ConcurrentMcpSession`, so the runtime leaves it
unwrapped too: it goes straight to the same reference-counted ``MCPToolset``
the model's own tool calls use, so a grant view's calls run concurrently with
the model's and with each other, never queued behind
:class:`~loom.ai.runtime.SharedMcpSession`'s single lock.

:func:`build_mcp_toolset` holds the connection rules of one grant — its
validated URL and its credential — once, so start-up cannot validate a server
the run would not reach.  The credential itself is resolved by
:mod:`loom.ai.remote_auth`, which is where the deployment's own strategy plugs in;
this module only carries the result to the client.

The MCP client ships as an optional ``pydantic-ai-slim`` dependency, so it is
imported inside the function that needs it: importing it at module load would
break every deployment that declares no ``mcp`` grant.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

from loom.ai.abc import ConcurrentMcpSession, McpToolCallResult, McpToolInfo
from loom.ai.compiler import CompiledMcpCapability, mcp_connection
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.registry import require_provider_sdk
from loom.ai.remote_auth import headers_from_ref, shared_mcp_auth

if TYPE_CHECKING:
    from fastmcp.client.transports import ClientTransport, StdioTransport
    from pydantic_ai.mcp import MCPToolset

_logger = logging.getLogger(__name__)

_ISLAND = "loom.ai.engines.pydantic_ai.extras.mcp"
_EXTRA = "mcp"


class _McpSdk(Protocol):
    """The two names the MCP island publishes."""

    MCPToolset: type[MCPToolset[Any]]
    StdioTransport: type[StdioTransport]


def _sdk() -> _McpSdk:
    """Load the island; the cast records the contract every island keeps.

    Raises:
        AgentCompilationError: With ``PROVIDER_NOT_INSTALLED`` naming the
            ``mcp`` extra when the client is not installed.
    """
    return cast(_McpSdk, require_provider_sdk("mcp", _ISLAND, _EXTRA))


DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS: Final[float] = 10.0
"""Fallback handshake deadline, seconds, matching
:attr:`~loom.ai.config.AiConfig.startup_timeout_ms`'s own default. Production
never relies on this: the composition root always hands
:class:`SharedMcpToolsets` the deployment's real ``ai.startup_timeout_ms``.
Used only by a direct construction — a test, a diagnostic — so it still
carries a deadline rather than falling through to
:class:`~pydantic_ai.mcp.MCPToolset`'s own ``5``-second default."""


class _ToolsetSession(ConcurrentMcpSession):
    """Adapts a connected ``MCPToolset`` to the runtime's session contract.

    The runtime asks a session for two things only — the tool names a server
    exposes, and one tool call — so the adapter is the narrowing of the
    toolset's much wider surface down to
    :class:`~loom.ai.runtime.McpSession`. It subclasses
    :class:`~loom.ai.abc.ConcurrentMcpSession` because the underlying
    ``fastmcp`` client multiplexes concurrent calls by request id rather
    than writing straight through a single unmatched stream, so the runtime
    does not need to serialise calls made through this adapter with a second
    lock. ``MCPToolset`` itself only reference-counts ``__aenter__`` /
    ``__aexit__`` to decide when the connection opens and closes; that is a
    connection-lifecycle concern, not what makes concurrent calls safe.

    A session that died after it opened is replaced by the next call. The
    ``fastmcp`` client still reports itself connected once its transport is
    gone, so the evidence is the call: a tool's own error is a result flagged
    ``is_error``, and an exception out of the round trip is the transport
    failing. That call is raised and never repeated -- it may have run on the
    server -- and the one after it opens a fresh toolset first.

    Args:
        toolset: Already-entered toolset speaking to one server.
        rebuild: Builds a new, not yet entered toolset for the same
            connection; ``None`` never replaces the one given.
    """

    def __init__(
        self,
        toolset: MCPToolset[Any],
        *,
        rebuild: Callable[[], MCPToolset[Any]] | None = None,
    ) -> None:
        self._toolset = toolset
        self._rebuild = rebuild
        self._suspect: MCPToolset[Any] | None = None
        self._renewing = asyncio.Lock()
        self._renewed: list[MCPToolset[Any]] = []

    async def _live(self) -> MCPToolset[Any]:
        """Return the toolset to speak through, replacing a suspect one at most once.

        A renewal that cannot connect raises and leaves the suspect in place,
        so the next call tries again.
        """
        toolset = self._toolset
        if self._rebuild is None or self._suspect is not toolset:
            return toolset
        async with self._renewing:
            if self._toolset is not toolset:
                return self._toolset
            fresh = self._rebuild()
            await fresh.__aenter__()
            self._renewed.append(fresh)
            self._toolset = fresh
            _logger.warning("mcp session renewed after a failed call")
            return fresh

    async def aclose(self) -> None:
        """Close the toolsets this session opened itself; the first belongs to its opener."""
        while self._renewed:
            toolset = self._renewed.pop()
            try:
                await toolset.__aexit__(None, None, None)
            except Exception:
                _logger.warning("a renewed mcp session did not close cleanly", exc_info=True)

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the server exposes, each with its output-schema flag.

        ``MCPToolset.list_tools`` caches its result (``cache_tools=True`` by
        default), so a second call from
        :class:`~loom.ai.runtime._grants.McpGrantView` costs no extra round
        trip beyond the one start-up already pays.
        """
        return tuple(
            McpToolInfo(name=tool.name, has_output_schema=tool.output_schema is not None)
            for tool in await (await self._live()).list_tools()
        )

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Invoke one tool and return its protocol-level result.

        Goes straight to the underlying client's ``call_tool_mcp`` rather than
        ``MCPToolset.direct_call_tool``: the latter is written for the model's
        own retry loop and turns an ``is_error`` result into a raised
        ``ModelRetry``/``ToolFailed`` before the caller ever sees the flag.
        This adapter's caller is
        :class:`~loom.ai.runtime._grants.McpGrantView`, which must inspect
        ``ok`` itself — before deciding whether to decode — so the raw
        protocol result is what it needs, with neither prose-fallback mapping
        nor an engine-facing exception in the way.

        Nothing here shields the round trip from the caller's own
        cancellation: this adapter holds no lock over the shared
        ``MCPToolset``, so cancelling a caller here never desynchronises a
        neighbour the way it would on a locked, single-framed session — the
        JSON-RPC client keeps every in-flight call's response matched to its
        own request id regardless. A cancelled caller — including the plan's
        own ``tool_timeout_ms``, enforced by
        :func:`~loom.ai.engines.pydantic_ai._guards.capability_call` around
        this call on the ``kind: python`` and ``kind: mcp`` routes — is
        therefore free to return the moment it fires.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The server's own error flag and structured content, verbatim.
        """
        toolset = await self._live()
        try:
            async with toolset:
                result = await toolset.client.call_tool_mcp(name, dict(arguments))
        except Exception:
            self._suspect = toolset
            raise
        return McpToolCallResult(ok=not result.is_error, structured=result.structured_content)


def build_mcp_toolset(capability: CompiledMcpCapability, *, init_timeout: float) -> MCPToolset[Any]:
    """Build the unfiltered toolset of one grant, applying its connection rules.

    The credential is applied exactly as the deployment declared it: fixed
    headers from ``headers_ref``, or the object the named strategy builds —
    one instance per server, shared by every agent granted it.  A server that
    declares neither is connected exactly as before, which is what lets one
    artifact move between environments unchanged.

    ``read_timeout``, derived from ``capability.timeout_ms``, governs one
    call's deadline for the HTTP-streamable transport; under the SSE
    transport, whenever this call builds the transport explicitly (the
    server declares ``headers_ref`` or ``auth``), the same value also
    becomes the server's idle-event-stream deadline. See "The handshake
    deadline vs. the call deadline" in ``docs/ai/mcp.md``.

    Args:
        capability: Compiled grant carrying the validated address of its
            transport and the credential resolved for it.
        init_timeout: Seconds to wait for the connection and the
            ``initialize`` handshake, derived from ``ai.startup_timeout_ms``.

    Returns:
        The unfiltered, not yet connected toolset; the caller applies the
        grant's tool filter and the capability boundary.

    Raises:
        AgentCompilationError: When the MCP client is not installed, the
            grant's transport is not one this engine serves, the
            ``headers_ref`` payload is not one ``Name=value`` pair, or the
            named strategy cannot be built.

    Example::

        toolset = build_mcp_toolset(capability, init_timeout=10.0)
    """
    sdk = _sdk()
    component = f"mcp server '{capability.server}'"
    client = _mcp_client(component, capability)
    headers = headers_from_ref(component, capability.headers_ref) or None
    auth = shared_mcp_auth(capability.server, capability.auth)
    read_timeout = capability.timeout_ms / 1000
    # ``MCPToolset`` annotates auth as ``httpx.Auth | Literal['oauth'] | str | None``,
    # which admits no callable, while what really consumes it is fastmcp's HTTP
    # transport: its ``_set_auth`` special-cases only ``"oauth"``, ``OAuth``, the
    # OAuth providers and ``str``, and hands anything else to its ``httpx2`` client
    # untouched — including the callable loom's own strategies return.
    toolset: MCPToolset[Any] = sdk.MCPToolset(
        client,
        headers=headers,
        auth=cast("Any", auth),
        init_timeout=init_timeout,
        read_timeout=read_timeout,
    )
    return toolset


def _mcp_client(component: str, capability: CompiledMcpCapability) -> str | ClientTransport:
    """Return what ``MCPToolset`` connects to for the grant's transport.

    Under ``http`` that is the validated server URL.  Under ``stdio`` it is a
    transport that spawns the declared command, receives only the declared
    environment and dies with the context that opened it, so no server outlives
    the toolset that owns it.

    Raises:
        AgentCompilationError: When the transport is not one this engine serves,
            or the stdio client library is missing.
    """
    if capability.transport == "http" and capability.url is not None:
        return capability.url
    if capability.transport == "stdio" and capability.command is not None:
        return _stdio_transport(capability.command, capability)
    reason = f"transport {capability.transport!r} is not served by the pydantic-ai engine"
    raise AgentCompilationError(
        [
            AgentErrorCode.MCP_TRANSPORT_INVALID.issue(
                component=component, field="transport", reason=reason
            )
        ]
    )


def _stdio_transport(command: str, capability: CompiledMcpCapability) -> ClientTransport:
    """Build the stdio transport of one grant, tied to its owner's lifetime.

    Raises:
        AgentCompilationError: When the stdio client library is not installed.
    """
    return _sdk().StdioTransport(
        command=command,
        args=list(capability.args),
        env=dict(capability.env) or None,
        keep_alive=False,
    )


class SharedMcpToolsets:
    """The MCP toolsets of one worker: one per distinct connection.

    Every agent granted a server works over the same ``MCPToolset`` instance,
    which is what makes a worker hold one channel per server instead of one
    per agent. ``MCPToolset`` reference-counts ``__aenter__``, so the runtime
    entering it at start-up and each run entering it again open a single
    session, and the connection closes when the last holder leaves.

    Sharing the toolset — rather than a
    :class:`~loom.ai.runtime.SharedMcpSession` over it — is what keeps the
    runs concurrent: the session wrapper serialises every call on one lock,
    so one agent's ``tool_timeout_ms`` would stop bounding anything for its
    neighbours, queued behind it. :class:`_ToolsetSession` holds no such
    lock, so a caller cancelled mid-call — the plan's own
    ``tool_timeout_ms`` firing, in particular — returns immediately instead
    of waiting for the call to drain; the underlying JSON-RPC client still
    matches every in-flight response to its own request id, so an abandoned
    call never desynchronises a neighbour sharing the connection.

    All of that holds only while the runtime's ``mcp_client_factory`` and the
    engine build read the *same* instance. :meth:`open` records every
    connection it was asked for and :meth:`for_build` refuses one it was never
    asked for, which is why the two are separate methods: a shared
    build-and-cache helper would have made :meth:`open` satisfy its own check
    and left the wiring unverified. The engine build runs strictly after the
    runtime opened its clients, so a grant missing from that record can only
    mean the runtime was handed some other factory — and the run would then
    hold a second connection whose tool filters nothing validated (FR-025).

    State is per instance and owned by the engine provider; nothing here is
    module-level. One instance serves one runtime lifecycle: the toolsets are
    never evicted, and an ``MCPToolset`` re-entered from a second event loop
    would reuse a lock bound to the first.

    This per-server deadline never fires first in production — see "The
    per-server handshake deadline never fires first" in ``docs/ai/mcp.md``.

    Every toolset built by an instance waits
    :data:`DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS` for its connection and
    ``initialize`` handshake unless :meth:`set_connect_timeout` replaces that
    deadline; production is handed the deployment's real
    ``ai.startup_timeout_ms`` that way.

    Example::

        shared = SharedMcpToolsets()
        shared.set_connect_timeout(10.0)
        async with shared.open(capability) as session:
            names = await session.list_tools()
    """

    def __init__(self) -> None:
        self._init_timeout = DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS
        self._toolsets: dict[CompiledMcpCapability, MCPToolset[Any]] = {}
        self._opened: set[CompiledMcpCapability] = set()

    def set_connect_timeout(self, seconds: float) -> None:
        """Replace the handshake deadline every toolset built after this call waits for.

        Has no effect on a connection whose toolset :meth:`_toolset` already
        built: the deadline is read once, at build, not on every connect.
        Callers that need the deployment's real ``ai.startup_timeout_ms``
        must call this before the runtime opens any client.

        Args:
            seconds: New handshake deadline, in seconds.
        """
        self._init_timeout = seconds

    def for_build(self, capability: CompiledMcpCapability, agent: str) -> MCPToolset[Any]:
        """Return the shared toolset an agent's ``mcp`` grant must run over.

        The toolset is keyed by connection alone
        (:func:`~loom.ai.compiler.mcp_connection`), so two agents that filter
        one server differently share it; each applies its own filter on top,
        which changes neither the shared object nor the other agent's view.

        Args:
            capability: Compiled grant naming the connection.
            agent: Plan being built, named in the wiring failure.

        Returns:
            The worker's toolset for that connection, already opened by the
            runtime.

        Raises:
            AgentCompilationError: When the runtime never opened this
                connection through :meth:`open` — the deployment wired some
                other MCP client factory — or when the toolset cannot be built.
        """
        connection = mcp_connection(capability)
        if connection not in self._opened:
            raise AgentCompilationError(
                [
                    AgentErrorCode.MCP_CONNECTION_NOT_OPENED.issue(
                        component=agent, server=capability.server
                    )
                ]
            )
        return self._toolset(connection)

    def _toolset(self, connection: CompiledMcpCapability) -> MCPToolset[Any]:
        """Return the connection's toolset, building it at most once."""
        existing = self._toolsets.get(connection)
        if existing is not None:
            return existing
        built = build_mcp_toolset(connection, init_timeout=self._init_timeout)
        self._toolsets[connection] = built
        return built

    @asynccontextmanager
    async def open(self, capability: CompiledMcpCapability) -> AsyncIterator[_ToolsetSession]:
        """Open the worker's session for one grant, connected and ready to list.

        Satisfies :data:`~loom.ai.runtime.McpClientFactory`: nothing happens
        until the context is entered, so the runtime's start-up deadline bounds
        the whole of it and a failure is reported as a coded start-up issue
        rather than as an exception escaping ``create_app``. Holding the
        context open is what keeps the shared connection alive for the runs.

        Args:
            capability: Compiled grant carrying the validated address of its
                transport.

        Yields:
            The connected session; leaving releases this holder's reference and
            closes the transport once no run holds one either.

        Raises:
            AgentCompilationError: When the MCP client is not installed, or the
                grant's credential cannot be resolved.
        """
        connection = mcp_connection(capability)
        self._opened.add(connection)
        toolset = self._toolset(connection)
        async with toolset:
            session = _ToolsetSession(
                toolset,
                rebuild=lambda: build_mcp_toolset(connection, init_timeout=self._init_timeout),
            )
            try:
                yield session
            finally:
                await session.aclose()


@asynccontextmanager
async def create_mcp_client(
    capability: CompiledMcpCapability,
) -> AsyncIterator[_ToolsetSession]:
    """Open one throw-away session against an MCP server, outside any sharing.

    Kept for a caller that needs a session of its own — a diagnostic, a probe,
    a test. A composition root must **not** wire this as the runtime's
    ``mcp_client_factory``: it builds a toolset the engine's run path does not
    know about, so the worker would hold that connection plus one per agent.
    Read ``mcp_client_factory`` off the engine provider instead, which is what
    ``create_app`` does.

    Args:
        capability: Compiled grant carrying the validated address of its transport.

    Yields:
        The connected session, closed with its transport on exit.

    Raises:
        AgentCompilationError: When the MCP client is not installed, or the
            grant's credential cannot be resolved.

    Example::

        async with create_mcp_client(capability) as session:
            names = await session.list_tools()
    """
    toolset = build_mcp_toolset(capability, init_timeout=DEFAULT_MCP_CONNECT_TIMEOUT_SECONDS)
    async with toolset:
        yield _ToolsetSession(toolset)
