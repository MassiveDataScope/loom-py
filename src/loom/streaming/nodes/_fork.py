"""Graph-level fork declarations for terminal branching flows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from loom.core.errors import InvalidValue
from loom.core.model import LoomFrozenStruct
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._expr_eval import (
    PredicateSpec,
    SelectorSpec,
    evaluate_predicate,
    select_value,
)

if TYPE_CHECKING:
    from loom.streaming.graph._flow import Process
else:

    class Process:
        @classmethod
        def __class_getitem__(cls, item: object) -> type[Process]:
            del item
            return cls


InT = TypeVar("InT", bound=StreamPayload)


class ForkKind(StrEnum):
    """Routing family for a terminal fork declaration."""

    KEYED = "keyed"
    PREDICATE = "predicate"


class ForkRoute(LoomFrozenStruct, Generic[InT], frozen=True):
    """One predicate branch of a :class:`Fork` node.

    Pattern:
        Branch declaration.

    Args:
        when: Predicate expression or custom predicate.
        process: Terminal process executed when the predicate matches.
    """

    when: PredicateSpec[InT]
    process: Process[InT, Any]


class Fork(Generic[InT]):
    """Terminal graph-level routing node that physically splits the stream.

    Pattern:
        Terminal branching.

    Unlike :class:`loom.streaming.nodes.Router`, branches are full
    independent sub-graphs. Each branch may contain any process node,
    including ``With`` and ``WithAsync``. ``Fork`` is terminal in the
    parent process: no nodes may follow it.
    """

    __slots__ = ("_default", "_kind", "_predicate_routes", "_routes", "_selector")

    def __init__(
        self,
        *,
        kind: ForkKind,
        selector: SelectorSpec[InT] | None = None,
        routes: Mapping[object, Process[InT, Any]] | None = None,
        predicate_routes: Sequence[ForkRoute[InT]] = (),
        default: Process[InT, Any] | None = None,
    ) -> None:
        _reject_foreign_routes(kind, routes, predicate_routes)
        self._kind = kind
        self._selector = selector
        self._routes = dict(routes or {})
        self._predicate_routes = tuple(predicate_routes)
        self._default = default

    @classmethod
    def by(
        cls,
        selector: SelectorSpec[InT],
        branches: Mapping[object, Process[InT, Any]],
        *,
        default: Process[InT, Any] | None = None,
    ) -> Fork[InT]:
        """Declare key-based terminal branching.

        Args:
            selector: Path expression or custom selector.
            branches: Terminal processes keyed by selector result.
            default: Optional fallback process.

        Returns:
            Fork declaration.
        """
        if not branches:
            raise InvalidValue("Fork.by requires at least one keyed route.")
        return cls(kind=ForkKind.KEYED, selector=selector, routes=branches, default=default)

    @classmethod
    def when(
        cls,
        routes: Sequence[ForkRoute[InT]],
        *,
        default: Process[InT, Any] | None = None,
    ) -> Fork[InT]:
        """Declare ordered first-match terminal branching.

        Args:
            routes: Ordered predicate routes.
            default: Optional fallback process.

        Returns:
            Fork declaration.
        """
        if not routes:
            raise InvalidValue("Fork.when requires at least one predicate route.")
        return cls(
            kind=ForkKind.PREDICATE,
            predicate_routes=routes,
            default=default,
        )

    @property
    def kind(self) -> ForkKind:
        """Routing family for this fork."""
        return self._kind

    @property
    def selector(self) -> SelectorSpec[InT] | None:
        """Key selector for ``Fork.by`` branches."""
        return self._selector

    @property
    def routes(self) -> Mapping[object, Process[InT, Any]]:
        """Keyed branches for ``Fork.by``."""
        return dict(self._routes)

    @property
    def predicate_routes(self) -> tuple[ForkRoute[InT], ...]:
        """Ordered predicate routes for ``Fork.when``."""
        return self._predicate_routes

    @property
    def default(self) -> Process[InT, Any] | None:
        """Fallback process."""
        return self._default


def _reject_foreign_routes(
    kind: ForkKind,
    routes: Mapping[object, Process[Any, Any]] | None,
    predicate_routes: Sequence[ForkRoute[Any]],
) -> None:
    """Reject routes that belong to a dispatch family other than *kind*.

    Only the declared family is enumerated, so a route of the other family
    would never be indexed, validated or wired.
    """
    if kind is ForkKind.KEYED and predicate_routes:
        raise InvalidValue("Fork(kind=keyed) does not dispatch predicate_routes.")
    if kind is ForkKind.PREDICATE and routes:
        raise InvalidValue("Fork(kind=predicate) does not dispatch keyed routes.")


__all__ = ["Fork", "ForkKind", "ForkRoute", "evaluate_predicate", "select_value"]
