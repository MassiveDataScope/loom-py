"""Fan-out node that expands one event into typed sub-collections and routes each to its Process."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from loom.core.errors import InvalidValue
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._decompose import PayloadExpander

if TYPE_CHECKING:
    from loom.streaming.graph._flow import Process
else:

    class Process:
        @classmethod
        def __class_getitem__(cls, item: object) -> type[Process]:
            del item
            return cls


InT = TypeVar("InT", bound=StreamPayload)


class ExpandRoutes(Generic[InT]):
    """Decompose one event into typed sub-collections and route each to its own Process.

    Unlike Broadcast (which copies the same message to every route), ExpandRoutes
    calls expander.expand() exactly once per event and routes each output type's
    rows to the matching Process. Zero redundant reprocessing.

    ExpandRoutes is terminal — no process nodes may follow it in the parent Process.

    Args:
        expander: PayloadExpander subclass that produces typed sub-collections.
        routes: Mapping from output type to the Process that handles those rows.
        default: Optional fallback Process for types not declared in routes.

    Raises:
        InvalidValue: If routes is empty and default is None.
    """

    __slots__ = ("_default", "_expander", "_routes")

    def __init__(
        self,
        *,
        expander: type[PayloadExpander[InT]],
        routes: dict[type, Process[Any, Any]],
        default: Process[Any, Any] | None = None,
    ) -> None:
        if not routes and default is None:
            raise InvalidValue("ExpandRoutes requires at least one route or a default Process.")
        self._expander = expander
        self._routes: Mapping[type, Process[Any, Any]] = MappingProxyType(dict(routes))
        self._default = default

    @property
    def expander(self) -> type[PayloadExpander[InT]]:
        """PayloadExpander subclass used to decompose each event."""
        return self._expander

    @property
    def routes(self) -> Mapping[type, Process[Any, Any]]:
        """Mapping from output type to its handling Process."""
        return self._routes

    @property
    def default(self) -> Process[Any, Any] | None:
        """Fallback Process for types not declared in routes."""
        return self._default


__all__ = ["ExpandRoutes"]
