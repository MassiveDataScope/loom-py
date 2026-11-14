"""The AI execution kinds: ``Agent()`` and ``Mcp()`` handles.

Declared here, not in the AI pillar, so the engine dispatches them without
importing it. The AI pillar binds their resolvers through
:meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver` and
:meth:`~loom.core.engine.executor.RuntimeExecutor.bind_mcp_resolver`.
"""

from __future__ import annotations

from loom.core.execution import ExecutionKinds, Kind
from loom.core.use_case.markers import _AgentMarker, _McpMarker

AGENT = Kind("agent", spelling=_AgentMarker.spelling)
MCP = Kind("mcp", spelling=_McpMarker.spelling)


def declare_ai_kinds(kinds: ExecutionKinds) -> None:
    """Declare ``Agent()`` as :data:`AGENT` and ``Mcp()`` as :data:`MCP` on ``kinds``."""
    kinds.declare_type(_AgentMarker, AGENT)
    kinds.declare_type(_McpMarker, MCP)
