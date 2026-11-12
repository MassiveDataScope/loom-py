"""What a failed MCP call proves about the server, and the error for one that may have run.

A failed call is sent again only when it provably never ran: the server answered
that it does not know the session. Everything else that is not the server's own
answer may have run, so it is raised as :class:`McpCallInterrupted` and never
repeated (plan 6.5; fastmcp 4.1.6, mcp 2.3.0).

The mcp client raises one class, ``MCPError``, both for what it makes itself and
for what a server sends; only the code tells them apart. On a ``tools/call`` the
client makes the codes in :data:`CLIENT_MADE_CODES` and no other, so a code
outside the set is an answer the server sent. A code inside it is never proof of
an answer, even when a server sent it.
"""

from __future__ import annotations

from enum import Enum
from typing import Final

from loom.ai.errors import AgentRunError, AgentRunErrorCode

CLIENT_MADE_CODES: Final[frozenset[int]] = frozenset(
    {
        -32000,  # CONNECTION_CLOSED
        -32001,  # REQUEST_TIMEOUT
        -32600,  # INVALID_REQUEST
        -32601,  # METHOD_NOT_FOUND
        -32603,  # INTERNAL_ERROR
        -32700,  # PARSE_ERROR
    }
)
"""JSON-RPC codes the mcp 2.3.0 client makes on a ``tools/call``; an mcp upgrade rechecks them."""

_INVALID_REQUEST: Final = -32600
_SESSION_UNKNOWN: Final = frozenset({"Session not found", "Session terminated"})
"""The two messages of a 404 on a session the server no longer knows: nothing ran."""


class Outcome(Enum):
    """What a failed call proves."""

    SAFE = "safe"
    """The call never ran; it may be sent once more on a new session."""
    UNSAFE = "unsafe"
    """The call may have run; it is never sent again."""
    COMPLETED = "completed"
    """The server answered with an error; it passes through unchanged."""


class McpCallInterrupted(AgentRunError):
    """An MCP call failed in a way that leaves unknown whether it ran on the server.

    It is never repeated, and the session renews before the next call. Being a
    run error with its own code is what lets an engine-neutral caller recognise
    it: a marker's grant view turns it into ``TOOL_UNAVAILABLE``. A call refused
    before it was sent -- the session is closed, or a renewal could not
    connect -- carries the same code, and its message says it was not sent.

    Args:
        tool: The tool the interrupted call named.
        sent: Whether the call reached the transport, so it may have run.
    """

    def __init__(self, tool: str, *, sent: bool = True) -> None:
        fate = "was interrupted and may have run" if sent else "was not sent"
        super().__init__(
            AgentRunErrorCode.MCP_CALL_INTERRUPTED,
            f"the call to mcp tool {tool!r} {fate}; it is not repeated",
        )


def classify(exc: Exception) -> Outcome:
    """Return what a call's failure proves about the server.

    Args:
        exc: What the call raised.

    Returns:
        ``SAFE`` for a session the server does not know, ``COMPLETED`` for a
        JSON-RPC error outside the client's own codes, ``UNSAFE`` for anything
        else.
    """
    from mcp import MCPError

    if not isinstance(exc, MCPError):
        return Outcome.UNSAFE
    if exc.code not in CLIENT_MADE_CODES:
        return Outcome.COMPLETED
    if exc.code == _INVALID_REQUEST and exc.message in _SESSION_UNKNOWN:
        return Outcome.SAFE
    return Outcome.UNSAFE
