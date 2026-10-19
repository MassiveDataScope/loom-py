"""MCP sessions shared per server, and the tool filters checked against them.

A JSON-RPC session gets one shared lock serialising every concurrent call by
default, unless it declares itself already safe for concurrent calls (see
:func:`mcp_session_for` and :class:`~loom.ai.abc.ConcurrentMcpSession`). The
declared tool filters are validated here too (FR-025): they are checked
against the tools a server really lists, which is a property of the session,
not of the runtime lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, TypeVar

from loom.ai._concurrency import shield_and_drain
from loom.ai._filters import select_names
from loom.ai.abc import ConcurrentMcpSession, McpSession, McpToolCallResult, McpToolInfo
from loom.ai.compiler import AgentPlan, CompiledMcpCapability, mcp_connection
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")


McpClientFactory = Callable[[CompiledMcpCapability], AbstractAsyncContextManager[McpSession]]
"""Builds the (not yet opened) client of one compiled MCP capability."""


class SharedMcpSession:
    """Serialises every call to one MCP session shared by concurrent runs.

    A JSON-RPC session is a single framed stream: two overlapping calls
    interleave their frames, and a caller cancelled mid-frame leaves the
    session desynchronised for its neighbours. Both are prevented here — one
    lock per session, and the in-flight call shielded and drained to
    completion before the lock is released, after which the cancellation is
    re-raised to the caller that asked for it.

    Args:
        session: The live session to guard.
        label: Human-readable name used in log messages.
    """

    def __init__(self, session: McpSession, *, label: str) -> None:
        self._session = session
        self._label = label
        self._lock = asyncio.Lock()

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        """Return the tools the server exposes, serialised with every other call.

        Returns:
            Every tool the underlying session advertises.
        """
        return await self._serialised(self._session.list_tools)

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        """Invoke one tool, serialised with every other call on this session.

        Args:
            name: Tool name as the server exposes it.
            arguments: Arguments to pass to the tool.

        Returns:
            The tool's result.

        Raises:
            asyncio.CancelledError: When the caller is cancelled. The in-flight
                call still runs to completion, so the session stays usable.
        """
        return await self._serialised(lambda: self._session.call_tool(name, arguments))

    async def _serialised(self, make_call: Callable[[], Coroutine[Any, Any, _T]]) -> _T:
        """Run *make_call* behind this session's lock, building its coroutine only once held.

        *make_call* is a callable, not an already-built coroutine: building
        the coroutine before the lock is acquired would let a caller
        cancelled while still queued for its turn abandon one nobody ever
        awaits.
        """
        async with self._lock:
            return await shield_and_drain(make_call(), label=self._label)


def mcp_session_for(session: McpSession, *, label: str) -> McpSession:
    """Return the session a grant view calls through, serialised unless declared safe.

    ``session`` already carries the answer: one that subclasses
    :class:`~loom.ai.abc.ConcurrentMcpSession` already guards its own frames
    — the engine's own session does, because the underlying JSON-RPC client
    multiplexes concurrent calls by request id rather than writing straight
    through a single unmatched stream — so wrapping it again would only add
    a second, redundant lock, queuing every grant view behind its own
    neighbours. A session that declares nothing is serialised behind
    :class:`SharedMcpSession`, the safe default for a third-party session
    this runtime does not control.

    Args:
        session: The just-opened session of one MCP connection.
        label: Human-readable name used in the wrapper's log messages.

    Returns:
        ``session`` unchanged when it declares itself concurrency-safe;
        otherwise a new :class:`SharedMcpSession` guarding it.
    """
    if isinstance(session, ConcurrentMcpSession):
        return session
    return SharedMcpSession(session, label=label)


def _filtered_tools(
    tools: Sequence[McpToolInfo], *, include: Sequence[str], exclude: Sequence[str]
) -> tuple[str, ...]:
    """Apply the glob ``include`` then ``exclude`` to the tools a server offers."""
    return select_names(tuple(tool.name for tool in tools), include=include, exclude=exclude)


@dataclass(frozen=True, slots=True)
class FilterTarget:
    """One declared MCP grant and the shared session its tools are listed from.

    ``include``/``exclude`` are empty when the grant declares no filter; such
    a target is still listed (see :func:`filter_targets`), just never checked
    by :func:`filter_issues`.

    Attributes:
        agent: The agent declaring this grant, or the registered use-case key
            when the target comes from an ``Mcp()`` marker instead of a
            compiled agent plan (see ``AgentRuntime._use_case_filter_targets``).
    """

    agent: str
    server: str
    key: str
    include: tuple[str, ...]
    exclude: tuple[str, ...]


def filter_targets(plans: Iterable[AgentPlan]) -> tuple[FilterTarget, ...]:
    """Return one target per declared MCP capability, in plan then declaration order.

    Every grant is listed once at start-up, not only the ones that declare a
    filter: :class:`~loom.ai.runtime._grants.McpGrantView` reads this same
    catalogue synchronously later, so a grant an ``AgentHandle`` may reach
    through :meth:`~loom.ai.abc.AgentHandle.mcp` must have its tools listed
    here regardless of whether it also narrows them.
    """
    return tuple(
        FilterTarget(
            agent=plan.name,
            server=capability.server,
            key=mcp_key(capability),
            include=capability.include,
            exclude=capability.exclude,
        )
        for plan in plans
        for capability in plan.capabilities
        if type(capability) is CompiledMcpCapability
    )


def filter_issues(
    targets: Iterable[FilterTarget], listed: Mapping[str, tuple[McpToolInfo, ...]]
) -> list[AgentCompilationIssue]:
    """Return one issue per declared filter that selects none of its server's tools.

    A target with no declared filter is skipped here: an empty ``include``
    already selects everything :func:`_filtered_tools` is given, so checking
    it would only catch a server publishing zero tools at all — a different
    failure than a filter matching nothing.
    """
    return [
        AgentErrorCode.TOOL_FILTER_MATCHES_NOTHING.issue(
            component=target.agent, target=target.server
        )
        for target in targets
        if (target.include or target.exclude)
        and target.key in listed
        and not _filtered_tools(listed[target.key], include=target.include, exclude=target.exclude)
    ]


def listing_timeout_issues(
    targets: Iterable[FilterTarget], listed: Mapping[str, tuple[McpToolInfo, ...]]
) -> list[AgentCompilationIssue]:
    """Name every server whose tool listing did not complete inside the budget."""
    pending: dict[str, str] = {
        target.key: target.server for target in targets if target.key not in listed
    }
    return [
        AgentErrorCode.MCP_SERVER_UNREACHABLE.issue(
            server=server, reason="listing its tools timed out"
        )
        for server in pending.values()
    ]


def connection_conflicts(plans: Iterable[AgentPlan]) -> list[AgentCompilationIssue]:
    """Return one issue per agent whose grant contradicts the server's first one.

    This is what makes two different keyings agree. Start-up de-duplicates its
    clients by server *name* (``_remote_capabilities``), while the engine keys
    the toolset it shares by *connection*
    (:func:`~loom.ai.compiler.mcp_connection`). One name resolving to two
    connections would therefore open one client at start-up and still build a
    second toolset at run time — a connection whose tool filters nothing
    validated, for an agent that believes it was checked. Refused before any
    client opens, so it is a boot failure and not a run-time surprise.

    Plans compiled together can never disagree, because every connection fact
    comes from one ``ai.mcp_servers`` mapping; nothing forces a deployment to
    compile its plans together.

    Args:
        plans: Compiled plans of this worker, in the order they were given.

    Returns:
        One issue per disagreeing grant, naming the server and both agents.

    Example::

        issues = connection_conflicts(plans)
    """
    first: dict[str, tuple[CompiledMcpCapability, str]] = {}
    issues: list[AgentCompilationIssue] = []
    for plan in plans:
        for capability in plan.capabilities:
            if type(capability) is not CompiledMcpCapability:
                continue
            connection = mcp_connection(capability)
            seen = first.setdefault(capability.server, (connection, plan.name))
            if seen[0] != connection:
                issues.append(
                    AgentErrorCode.MCP_CONNECTION_CONFLICT.issue(
                        server=capability.server, agents=(seen[1], plan.name)
                    )
                )
    return issues


def mcp_key(capability: CompiledMcpCapability) -> str:
    """Return the health-check key of one MCP capability, by registered name."""
    return f"mcp:{capability.server}"
