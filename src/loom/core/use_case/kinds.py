"""The use-case execution kinds: ``Caller()``, ``Agent()``, ``Mcp()``, and the refused inputs."""

from __future__ import annotations

from loom.core.execution import ExecutionKinds, ExecutionResolver, Kind, execution_identity
from loom.core.identity import Identity
from loom.core.use_case.markers import (
    _AgentMarker,
    _CallerMarker,
    _DeliveryMarker,
    _ExistsMarker,
    _InputMarker,
    _LoadByIdMarker,
    _LoadMarker,
    _McpMarker,
    _QueryMarker,
)

CALLER = Kind("caller", singleton_safe=False, spelling=_CallerMarker.spelling)
AGENT = Kind("agent", spelling=_AgentMarker.spelling)
MCP = Kind("mcp", spelling=_McpMarker.spelling)

_INPUT_MARKERS = (
    _InputMarker,
    _QueryMarker,
    _DeliveryMarker,
    _LoadByIdMarker,
    _LoadMarker,
    _ExistsMarker,
)


class CallerResolver(ExecutionResolver):
    """Resolves ``Caller()`` to the identity of the execution in progress."""

    __slots__ = ()

    def resolve(self, declaration: object, annotation: object) -> Identity:
        """Return :func:`~loom.core.execution.execution_identity`, failing closed."""
        return execution_identity()


def declare_kinds(kinds: ExecutionKinds) -> None:
    """Declare ``Caller()``, ``Agent()`` and ``Mcp()`` on *kinds*, bind ``Caller()``, refuse inputs.

    ``Caller()`` is :data:`CALLER`, resolved by :class:`CallerResolver`;
    ``Agent()`` is :data:`AGENT` and ``Mcp()`` is :data:`MCP`, whose resolvers
    the AI runtime binds. A constructor declaring an input marker is refused.

    Args:
        kinds: The registry to declare on.

    Raises:
        RuntimeError: When *kinds* already has a ``Caller()`` resolver.
    """
    kinds.declare_type(_CallerMarker, CALLER)
    kinds.declare_type(_AgentMarker, AGENT)
    kinds.declare_type(_McpMarker, MCP)
    for marker in _INPUT_MARKERS:
        kinds.refuse(marker, f"{marker.spelling} declares an input of execute(), not a dependency")
    kinds.bind(CALLER, CallerResolver())
