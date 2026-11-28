"""What the application's packages share with the freeze suite."""

from __future__ import annotations

import threading

from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.command import Command

SERVER = "orders"
AGENT = "clerk"
TOOLS = ("echo", "book", "slow")
GAP_TAKEN = threading.Event()
"""Set by a test once it moved the wall clock while a ``slow`` call is in flight."""
RAISED: list[AgentRunErrorCode] = []
"""The code of every ``AgentRunError`` a use case or job saw, noted before it propagated."""


class Say(Command, frozen=True):
    """Which tool to call, and with what."""

    tool: str = "echo"
    text: str = "marker"


def noted(error: AgentRunError) -> AgentRunError:
    RAISED.append(error.code)
    return error
