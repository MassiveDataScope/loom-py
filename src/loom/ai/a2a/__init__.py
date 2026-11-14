"""Pure A2A projection of a compiled agent.

Holds the card projection and the event projection, both derived from the
compiled :class:`~loom.ai.compiler.AgentPlan` alone. Neither imports an
A2A SDK nor a web framework, so importing this package on a base installation
succeeds and the redaction guarantee is unit-testable. The
transport server lives behind the ``ai-a2a`` extra and is not re-exported here.
"""

from __future__ import annotations

from loom.ai.a2a.card import (
    DEFAULT_A2A_PREFIX,
    PROTOCOL_VERSION,
    SKILL_TAGS,
    agent_url,
    build_agent_card,
    card_path,
)
from loom.ai.a2a.events import A2AEventProjector

__all__ = [
    "DEFAULT_A2A_PREFIX",
    "PROTOCOL_VERSION",
    "SKILL_TAGS",
    "A2AEventProjector",
    "agent_url",
    "build_agent_card",
    "card_path",
]
