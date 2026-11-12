"""Outbound MCP transport: this agent calling a remote tool server.

One ``MCPToolset`` per connection serves the whole worker, and
:class:`SharedMcpToolsets` is what holds them:

* :meth:`SharedMcpToolsets.open` is the
  :data:`~loom.ai.runtime.McpClientFactory` the runtime opens at start-up.
  Entering it connects the server and hands back a session, so an unreachable
  server fails start-up as ``MCP_SERVER_UNREACHABLE`` and the declared tool
  filters are validated against the tools the server really exposes (FR-025)
  instead of being taken on trust.
* :meth:`SharedMcpToolsets.for_build` is what the engine puts behind the
  capability boundary for the run itself — the session :meth:`open` made, as a
  toolset (:class:`_SessionToolset`) — so the start-up connection is the run's
  connection and a worker holds one channel per server rather than one per
  agent (FR-026).

What is shared is the session. :class:`_ToolsetSession` — what a grant view
(:meth:`~loom.ai.abc.AgentHandle.mcp`, a use case's ``Mcp()``), a ``python``
factory's ``remote()`` and the model's own tool calls all go through —
declares itself with :class:`~loom.ai.abc.ConcurrentMcpSession`, so the
runtime leaves it unwrapped: every caller reaches the same reference-counted
``MCPToolset`` concurrently, never queued behind
:class:`~loom.ai.runtime.SharedMcpSession`'s single lock, and a renewal any of
them triggers serves them all.

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
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, NoReturn, Protocol, TypeVar, cast

from pydantic_ai.toolsets import AbstractToolset, ToolsetTool

from loom.ai.abc import ConcurrentMcpSession, McpSession, McpToolCallResult, McpToolInfo
from loom.ai.compiler import CompiledMcpCapability, mcp_connection
from loom.ai.engines.pydantic_ai._failures import McpCallInterrupted, Outcome, classify
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.registry import require_provider_sdk
from loom.ai.remote_auth import headers_from_ref, shared_mcp_auth
from loom.core.config.errors import ConfigError
from loom.core.model.redaction import Secret

if TYPE_CHECKING:
    from fastmcp.client.transports import ClientTransport, StdioTransport
    from pydantic_ai.mcp import MCPToolset
    from pydantic_ai.tools import RunContext

    from loom.ai.engines.pydantic_ai._transport import EvictableTransport

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_ISLAND = "loom.ai.engines.pydantic_ai.extras.mcp"
_EXTRA = "mcp"


class _McpSdk(Protocol):
    """The names the MCP island publishes."""

    MCPToolset: type[MCPToolset[Any]]
    StdioTransport: type[StdioTransport]
    RequestHookAuth: Callable[[Callable[[Any], Any]], Any]


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


@dataclass(eq=False)
class _OpenToolset:
    """One toolset a session speaks through, with what its calls need to know about it.

    Attributes:
        toolset: The toolset.
        pool: Its HTTP pool, or ``None`` for a transport that has none.
        held: Whether the session holds an entry of it, keeping it connected
            between uses; one it does not hold is entered by each use alone.
        in_flight: Calls sent through it that have not returned yet.
        suspect: Whether a call failed on it, so the next call renews it first.
    """

    toolset: MCPToolset[Any]
    pool: EvictableTransport | None
    held: bool = True
    in_flight: int = 0
    suspect: bool = False

    @classmethod
    def of(cls, toolset: MCPToolset[Any], *, held: bool = True) -> _OpenToolset:
        """Hold *toolset* with the pool its builder put on its transport, if any.

        ``build_mcp_toolset`` gives an ``http`` toolset a
        :class:`~._transport.PooledHttpTransport`, which carries the pool;
        ``stdio`` and SSE transports carry none. ``Client.transport`` is
        fastmcp's public attribute.
        """
        transport = getattr(getattr(toolset, "client", None), "transport", None)
        return cls(toolset, getattr(transport, "pool", None), held=held)


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

    A failed call is classified (:func:`~._failures.classify`). One the
    server answered passes through unchanged. One that provably never ran — the
    server no longer knows the session — is sent once more on a renewed
    toolset. Any other may have run on the server: it is raised as
    :class:`~._failures.McpCallInterrupted`, never repeated, and the next call
    renews first.

    A renewal retires the toolset it replaces rather than closing it under a
    call still in flight there; the last such call to return hands its close
    off, so the call's own result and deadline are never touched by it. At
    most two toolsets are open: a renewal that finds an earlier one still
    draining closes it first, cutting the calls left on it. Every close runs in
    a task of its own, so a caller cancelled while waiting for one never
    leaves it half done, and :meth:`aclose` waits for all of them. A closed
    session refuses every later call. The freeze check lives in the toolset's
    pool (:class:`~._transport.EvictableTransport`), which sees every caller.

    A session that does not hold its toolset -- one over a connection start-up
    could not open -- keeps nothing connected: each use enters the toolset and
    leaves it, so its first use connects, and a renewal swaps in a toolset it
    does not hold either.

    Args:
        toolset: The toolset speaking to one server. When *held*, it is already
            entered and the session holds that entry until :meth:`aclose`;
            otherwise it is not entered, and each use enters it alone.
        rebuild: Builds a new, not yet entered toolset for the same
            connection; ``None`` never replaces the one given.
        held: Whether *toolset* is entered and held by the session.
    """

    def __init__(
        self,
        toolset: MCPToolset[Any],
        *,
        rebuild: Callable[[], MCPToolset[Any]] | None = None,
        held: bool = True,
    ) -> None:
        self._current = _OpenToolset.of(toolset, held=held)
        self._retired: _OpenToolset | None = None
        self._rebuild = rebuild
        self._renewing = asyncio.Lock()
        self._closing: set[asyncio.Task[None]] = set()
        self._closed = False

    async def _live(self) -> _OpenToolset:
        """Return the toolset to speak through, replacing a suspect one at most once.

        A renewal that cannot connect raises and leaves the suspect in place,
        so the next call tries again. The session may close while a call waits
        for the renewal, so it is checked again before the toolset is handed out.

        No ``client.is_connected()`` check runs first: over streamable HTTP it
        stays true through a server restart, so it would never catch the dead
        connection it is meant for; the pool's freeze drop and the safe retry do.

        Raises:
            RuntimeError: When the session is closed.
        """
        held = self._current
        if not self._closed and (self._rebuild is None or not held.suspect):
            return held
        async with self._renewing:
            if not self._closed and self._current is held and self._rebuild is not None:
                await self._renew(held, self._rebuild())
            if self._closed:
                raise RuntimeError("the mcp session is closed")
            return self._current

    async def _renew(self, held: _OpenToolset, toolset: MCPToolset[Any]) -> None:
        """Put *toolset* in place of *held*: connected and its tools listed, if held.

        The tools are listed before anyone is served, once: ``MCPToolset``
        caches the list, so no run lists them again. A toolset the session
        does not hold is connected by its first use instead. A session closed
        meanwhile closes *toolset*. An earlier toolset still draining is closed
        first, cutting its calls, and every close still running is waited for,
        so at most two are ever open, however slow a close is.
        """
        retired, self._retired = self._retired, None
        if retired is not None:
            self._close(retired)
        await asyncio.shield(asyncio.gather(*self._closing))
        fresh = await self._listed(toolset) if held.held else _OpenToolset.of(toolset, held=False)
        if self._closed:
            await asyncio.shield(self._close(fresh))
            return
        self._current = fresh
        if held.in_flight:
            self._retired = held
        else:
            await asyncio.shield(self._close(held))
        _logger.warning("mcp session renewed after a failed call")

    async def _listed(self, toolset: MCPToolset[Any]) -> _OpenToolset:
        """Enter *toolset* and list its tools; one that cannot is closed again."""
        fresh = await _entered_toolset(toolset)
        try:
            await fresh.toolset.list_tools()
        except BaseException:
            await asyncio.shield(self._close(fresh))
            raise
        return fresh

    def _close(self, held: _OpenToolset) -> asyncio.Task[None]:
        """Close *held* in a task of its own, tracked until it is done."""
        task = asyncio.create_task(_shut(held))
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)
        return task

    async def aclose(self) -> None:
        """Close every toolset this session holds, the one it was given included; refuse calls."""
        self._closed = True
        retired, self._retired = self._retired, None
        if retired is not None:
            self._close(retired)
        self._close(self._current)
        await asyncio.shield(asyncio.gather(*self._closing))

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the server exposes, each with its output-schema flag.

        ``MCPToolset.list_tools`` caches its result (``cache_tools=True`` by
        default), so a second call from
        :class:`~loom.ai.runtime._grants.McpGrantView` costs no extra round
        trip beyond the one start-up already pays.
        """
        tools = await self.invoke(None, lambda toolset: toolset.list_tools())
        return tuple(
            McpToolInfo(name=tool.name, has_output_schema=tool.output_schema is not None)
            for tool in tools
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

        Raises:
            McpCallInterrupted: When the call may have run on the server, or a
                renewal before it could not connect.
        """
        result = await self.invoke(
            name, lambda toolset: toolset.client.call_tool_mcp(name, dict(arguments))
        )
        return McpToolCallResult(ok=not result.is_error, structured=result.structured_content)

    async def invoke(
        self, name: str | None, operation: Callable[[MCPToolset[Any]], Awaitable[_T]]
    ) -> _T:
        """Run *operation* on the live toolset; send one that provably never ran once more.

        Every caller goes through here -- a grant view's call, a listing, the
        model's own tool call -- so they share one renewal, one in-flight count
        and one classification (:func:`~._failures.classify`). A failure the
        server answered passes through unchanged; one that never ran is sent
        once more on a renewed toolset; any other may have run, and is raised
        as :class:`~._failures.McpCallInterrupted`.

        Args:
            name: The tool called, named in an interruption; ``None`` for a listing.
            operation: What to do with the toolset.

        Returns:
            What *operation* returned.

        Raises:
            McpCallInterrupted: When the call may have run on the server, or was
                never sent.
        """
        held = await self._ready(name)
        try:
            return await self._send(held, operation)
        except Exception as exc:
            outcome = classify(exc)
            if self._rebuild is None or outcome is not Outcome.SAFE:
                _raise_failure(held, exc, name, outcome)
            held.suspect = True
        held = await self._ready(name)
        try:
            return await self._send(held, operation)
        except Exception as exc:
            _raise_failure(held, exc, name, classify(exc))

    async def tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        """The live toolset's tools for an agent run; a listing never renews.

        ``MCPToolset`` caches the list, so a toolset the session holds answers
        without a request, suspect or not: a run that calls no tool of this
        server is never held up by its renewal, which the next call makes,
        bounded by its tool timeout. A toolset the session does not hold
        connects to list.

        Raises:
            McpCallInterrupted: When the session is closed.
        """
        if self._closed:
            raise McpCallInterrupted(None, sent=False)
        held = self._current
        async with self._using(held):
            return await held.toolset.get_tools(ctx)

    async def _ready(self, name: str | None) -> _OpenToolset:
        """Return the toolset to send *name* through; a renewal that cannot connect sent nothing."""
        try:
            return await self._live()
        except Exception as exc:
            raise McpCallInterrupted(name, sent=False) from exc

    async def _send(
        self, held: _OpenToolset, operation: Callable[[MCPToolset[Any]], Awaitable[_T]]
    ) -> _T:
        """Run *operation* on *held*, counted in flight there until it returns."""
        async with self._using(held):
            return await operation(held.toolset)

    @asynccontextmanager
    async def _using(self, held: _OpenToolset) -> AsyncIterator[None]:
        """Hold *held* in use; the last use of a retired toolset hands off its close."""
        held.in_flight += 1
        try:
            async with held.toolset:
                yield
        finally:
            held.in_flight -= 1
            if held is self._retired and not held.in_flight:
                self._retired = None
                self._close(held)


class _SessionToolset(AbstractToolset[Any]):
    """What the engine puts behind an agent's ``mcp`` grant: the connection's one session.

    The toolset a session speaks through changes at every renewal, so this
    wraps the session, not a toolset: the model's own calls and their listing
    go through :meth:`_ToolsetSession.invoke`, sharing its renewal, its safe
    retry and its in-flight count with the grant views. The session holds the
    connection open, so entering and leaving this does nothing, and it is the
    same object for every run and every step.

    Args:
        session: The connection's session, opened by the runtime.
        named_after: The connection's first toolset, whose id and label this
            reports.
    """

    def __init__(self, session: _ToolsetSession, named_after: AbstractToolset[Any]) -> None:
        self._session = session
        self._named_after = named_after

    @property
    def id(self) -> str | None:
        return self._named_after.id

    @property
    def label(self) -> str:
        return self._named_after.label

    def visit_and_replace(
        self, visitor: Callable[[AbstractToolset[Any]], AbstractToolset[Any]]
    ) -> AbstractToolset[Any]:
        """Stay this toolset: a visitor that swapped it out would bypass the session."""
        del visitor
        return self

    # get_instructions is not forwarded: build_mcp_toolset leaves include_instructions
    # off; turning it on would need forwarding to the session's live toolset.

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        """List the live toolset's tools, never renewing it (:meth:`_ToolsetSession.tools`)."""
        return await self._session.tools(ctx)

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext[Any], tool: ToolsetTool[Any]
    ) -> Any:
        """Call one tool on the live toolset, which takes a tool any generation listed.

        Raises:
            McpCallInterrupted: When the call may have run, so the run fails
                rather than handing the model a refusal it would act on.
        """
        return await self._session.invoke(
            name, lambda toolset: toolset.call_tool(name, tool_args, ctx, tool)
        )


def _raise_failure(
    held: _OpenToolset, exc: Exception, name: str | None, outcome: Outcome
) -> NoReturn:
    """Raise what a failed call ends as: the server's own error, or an interruption.

    An interruption marks the toolset suspect, so the next call renews it first.
    """
    if outcome is Outcome.COMPLETED:
        raise exc
    held.suspect = True
    raise McpCallInterrupted(name) from exc


async def _entered_toolset(toolset: MCPToolset[Any]) -> _OpenToolset:
    """Enter a freshly built toolset; one that cannot connect releases the pool it made."""
    held = _OpenToolset.of(toolset)
    try:
        await toolset.__aenter__()
    except BaseException:
        await _released(held)
        raise
    return held


async def _shut(held: _OpenToolset) -> None:
    """Release a session's entry of a toolset and its pool; calls still in flight are cut.

    A close never raises: a toolset whose transport died raises from its
    force-close or its exit, and neither stops the steps after it.
    """
    try:
        try:
            if held.in_flight:
                await held.toolset.client.close()  # type: ignore[no-untyped-call]  # fastmcp
        finally:
            if held.held:
                await held.toolset.__aexit__(None, None, None)
    except Exception:
        _logger.warning("an mcp session did not close cleanly", exc_info=True)
    await _released(held)


async def _released(held: _OpenToolset) -> None:
    if held.pool is None:
        return
    try:
        await held.pool.release()
    except Exception:
        _logger.warning("an mcp connection pool did not close cleanly", exc_info=True)


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
    headers = headers_from_ref(component, capability.headers_ref) or None
    auth = _client_auth(sdk, shared_mcp_auth(capability.server, capability.auth))
    _refuse_per_run(capability.server, headers, auth)
    client = _http_transport(_mcp_client(component, capability), headers=headers, auth=auth)
    # A transport built here carries the credentials itself; MCPToolset refuses
    # them twice. Only an SSE URL still hands them to MCPToolset.
    credentials: dict[str, Any] = (
        {"headers": headers, "auth": auth} if isinstance(client, str) else {}
    )
    toolset: MCPToolset[Any] = sdk.MCPToolset(
        client,
        **credentials,
        init_timeout=init_timeout,
        read_timeout=capability.timeout_ms / 1000,
    )
    _require_static_auth(toolset, capability)
    return toolset


def _refuse_per_run(server: str, headers: object, auth: object) -> None:
    """Refuse credentials a toolset would resolve per run.

    ``MCPToolset`` copies itself for each run whose ``auth`` or ``headers`` is
    a callable, which would split the session every caller shares.

    Raises:
        ConfigError: Naming the server.
    """
    if callable(headers) or callable(auth):
        raise ConfigError(
            f"mcp server {server!r}: per-run auth or headers are not supported; "
            "the server's credentials must be static"
        )


def _client_auth(sdk: _McpSdk, auth: object) -> object:
    """Put a strategy's result in the shape the MCP client keeps as static auth.

    A strategy's request hook is a plain callable, and ``MCPToolset`` from
    pydantic-ai 2.66 reads any callable as a function of the run context, which
    would drop the credentials; the hook therefore reaches it as an
    ``httpx2.Auth``.  An ``Auth`` instance and the ``"oauth"`` sentinel are not
    callable and pass through unchanged.
    """
    if callable(auth):
        return sdk.RequestHookAuth(auth)
    return auth


def _require_static_auth(toolset: MCPToolset[Any], capability: CompiledMcpCapability) -> None:
    """Fail closed when a declared strategy did not reach the transport.

    Raises:
        AgentCompilationError: With ``MCP_AUTH_STRATEGY_INVALID`` when the grant
            declares ``auth`` and the client's transport carries none, so an
            upstream change can never turn an authenticated server into one
            called without credentials.
    """
    if capability.auth is None:
        return
    transport = getattr(toolset.client, "transport", None)
    if getattr(transport, "auth", None) is not None:
        return
    raise AgentCompilationError(
        [
            AgentErrorCode.MCP_AUTH_STRATEGY_INVALID.issue(
                kind=capability.auth.kind,
                reason="the MCP client's transport received no auth from it",
            )
        ]
    )


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


def _http_transport(
    client: str | ClientTransport, *, headers: dict[str, str] | None, auth: object
) -> str | ClientTransport:
    """Turn a streamable-HTTP URL into a transport with the credentials and a pool of its own.

    A transport already built (``stdio``), an SSE URL and a URL reached through
    a proxy pass through, so ``MCPToolset`` builds its stock transport as it
    always did. A client given its own transport ignores the environment's
    proxies, and only httpx2 applies their rules -- ``NO_PROXY`` included -- so
    behind any proxy there is no freeze eviction: a stale connection is left
    to the safe retry and the renewal. The MCP extra is already loaded by the
    caller.
    """
    from fastmcp.mcp_config import infer_transport_type_from_url

    if not isinstance(client, str) or infer_transport_type_from_url(client) == "sse":
        return client
    if _proxied(client):
        return client

    from loom.ai.engines.pydantic_ai._transport import PooledHttpTransport

    return PooledHttpTransport(client, headers=headers, auth=auth)


def _proxied(url: str) -> bool:
    """Whether the environment or the system names a proxy for *url*'s scheme, or for all."""
    proxies = urllib.request.getproxies()
    return bool(proxies.get(urllib.parse.urlsplit(url).scheme) or proxies.get("all"))


def _stdio_transport(command: str, capability: CompiledMcpCapability) -> ClientTransport:
    """Build the stdio transport of one grant, tied to its owner's lifetime.

    Raises:
        AgentCompilationError: When the stdio client library is not installed.
    """
    return _sdk().StdioTransport(
        command=command,
        args=list(capability.args),
        env={
            key: value.reveal() if isinstance(value, Secret) else value
            for key, value in capability.env
        }
        or None,
        keep_alive=False,
    )


class SharedMcpToolsets:
    """The MCP toolsets of one worker: one per distinct connection.

    Every agent granted a server works over the same connection, which is what
    makes a worker hold one channel per server instead of one per agent. The
    runtime opens it at start-up (:meth:`open`) as a :class:`_ToolsetSession`,
    kept here per connection: :meth:`for_build` hands an agent's ``mcp`` grant
    that session (:class:`_SessionToolset`), and :meth:`session_for` hands it to
    a ``python`` factory, so a grant view, the model's own calls and a python
    tool share one renewal and one in-flight count. A connection start-up did
    not open -- tolerated as unreachable -- gets a session too, created on
    first use, which holds no connection: each use enters and leaves its
    toolset, so it connects then.

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
        self._sessions: dict[CompiledMcpCapability, _ToolsetSession] = {}

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

    def for_build(self, capability: CompiledMcpCapability, agent: str) -> AbstractToolset[Any]:
        """Return the toolset an agent's ``mcp`` grant must run over.

        The connection is keyed alone (:func:`~loom.ai.compiler.mcp_connection`),
        so two agents that filter one server differently share it; each applies
        its own filter on top, which changes neither the shared object nor the
        other agent's view. An opened connection is handed out as its session,
        never as the toolset it started with, which a renewal replaces.

        Args:
            capability: Compiled grant naming the connection.
            agent: Plan being built, named in the wiring failure.

        Returns:
            The connection's session as a toolset, whether or not start-up
            connected it.

        Raises:
            AgentCompilationError: When the runtime never opened this
                connection through :meth:`open` -- the deployment wired some
                other MCP client factory -- or when the toolset cannot be built.
        """
        connection = self._opened_connection(capability, agent)
        return _SessionToolset(self._session(connection), named_after=self._toolset(connection))

    def session_for(self, capability: CompiledMcpCapability, agent: str) -> McpSession:
        """Return the session a ``python`` factory's ``remote()`` reaches the grant through.

        The same session a grant view and the agent's ``mcp`` grant use; when
        start-up could not connect it, one that holds no connection.

        Raises:
            AgentCompilationError: As :meth:`for_build`.
        """
        return self._session(self._opened_connection(capability, agent))

    def _session(self, connection: CompiledMcpCapability) -> _ToolsetSession:
        """Return the connection's session; one start-up could not open connects on use.

        That session holds nothing open: each use enters its toolset and leaves
        it, so nothing outlives the runtime that never opened the connection.
        It starts from a toolset of its own, as the one start-up tried released
        its pool.
        """
        session = self._sessions.get(connection)
        if session is None:
            rebuild = self._builder(connection)
            session = _ToolsetSession(rebuild(), rebuild=rebuild, held=False)
            self._sessions[connection] = session
        return session

    def _builder(self, connection: CompiledMcpCapability) -> Callable[[], MCPToolset[Any]]:
        return lambda: build_mcp_toolset(connection, init_timeout=self._init_timeout)

    def _opened_connection(
        self, capability: CompiledMcpCapability, agent: str
    ) -> CompiledMcpCapability:
        """Return the grant's connection, refusing one :meth:`open` was never asked for."""
        connection = mcp_connection(capability)
        if connection not in self._opened:
            raise AgentCompilationError(
                [
                    AgentErrorCode.MCP_CONNECTION_NOT_OPENED.issue(
                        component=agent, server=capability.server
                    )
                ]
            )
        return connection

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
        async with _entered(
            self._toolset(connection), rebuild=self._builder(connection)
        ) as session:
            self._sessions[connection] = session
            try:
                yield session
            finally:
                del self._sessions[connection]


@asynccontextmanager
async def _entered(
    toolset: MCPToolset[Any], *, rebuild: Callable[[], MCPToolset[Any]] | None = None
) -> AsyncIterator[_ToolsetSession]:
    """Enter *toolset* and hand that entry to a session, which closes all it holds on exit."""
    await _entered_toolset(toolset)
    session = _ToolsetSession(toolset, rebuild=rebuild)
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
    async with _entered(toolset) as session:
        yield session
