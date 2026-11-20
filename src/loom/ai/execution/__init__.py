"""Live agent runtime: one entered lifecycle, shared clients, bounded runs.

The lifecycle lives in :mod:`~loom.ai.execution._lifecycle`, the shared MCP
sessions and their tool-filter validation in :mod:`~loom.ai.execution._mcp` and
the per-run limits in :mod:`~loom.ai.execution._limits`. This package is the
whole public surface.

Nothing here imports FastAPI or Starlette: the HTTP surface lives in
:mod:`loom.ai.fastapi` and this package stays usable from any transport.

The classes here are experimental and may change within a major line; the
artifact format they run is not.  See :mod:`loom.ai` for the distinction.
"""

from __future__ import annotations

from loom.ai.abc import ConcurrentMcpSession as ConcurrentMcpSession
from loom.ai.abc import HealthState as HealthState
from loom.ai.abc import McpSession as McpSession
from loom.ai.errors import (
    AgentRunError as AgentRunError,
)
from loom.ai.execution._lifecycle import (
    A2AClientFactory,
    AgentHealth,
    AgentHost,
    UseCaseMcpGrant,
)
from loom.ai.execution._mcp import McpClientFactory, SharedMcpSession

__all__ = [
    "A2AClientFactory",
    "AgentHealth",
    "AgentHost",
    "ConcurrentMcpSession",
    "McpClientFactory",
    "McpSession",
    "SharedMcpSession",
    "UseCaseMcpGrant",
]
