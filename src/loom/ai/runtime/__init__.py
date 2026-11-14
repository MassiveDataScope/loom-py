"""Live agent runtime: one entered lifecycle, shared clients, bounded runs.

The lifecycle lives in :mod:`~loom.ai.runtime._lifecycle`, the shared MCP
sessions and their tool-filter validation in :mod:`~loom.ai.runtime._mcp`, the
per-run limits in :mod:`~loom.ai.runtime._limits` and the health vocabulary in
:mod:`~loom.ai.runtime._health`. This package is the whole public surface.

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
from loom.ai.runtime._health import AgentHealth
from loom.ai.runtime._lifecycle import A2AClientFactory, AgentRuntime, UseCaseMcpGrant
from loom.ai.runtime._mcp import McpClientFactory, SharedMcpSession

__all__ = [
    "A2AClientFactory",
    "AgentHealth",
    "AgentRuntime",
    "ConcurrentMcpSession",
    "McpClientFactory",
    "McpSession",
    "SharedMcpSession",
    "UseCaseMcpGrant",
]
