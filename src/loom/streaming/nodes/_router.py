"""Streaming router declarations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

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
OutT = TypeVar("OutT", bound=StreamPayload)


class Route(LoomFrozenStruct, Generic[InT, OutT], frozen=True):
    """One predicate route branch.

    Pattern:
        Route declaration.

    Args:
        when: Predicate expression or custom predicate.
        process: Process executed when the predicate matches.
    """

    when: PredicateSpec[InT]
    process: Process[InT, OutT]


class Router(Generic[InT, OutT]):
    """Declarative streaming router.

    Pattern:
        In-place routing.

    Use :meth:`by` for key-based dispatch and :meth:`when` for ordered
    predicate dispatch. The router only declares graph shape; runtime
    execution belongs to backend adapters.
    """

    __slots__ = ("_selector", "_routes", "_predicate_routes", "_default")

    def __init__(
        self,
        *,
        selector: SelectorSpec[InT] | None = None,
        routes: Mapping[object, Process[InT, OutT]] | None = None,
        predicate_routes: Sequence[Route[InT, OutT]] = (),
        default: Process[InT, OutT] | None = None,
    ) -> None:
        if selector is None and not predicate_routes:
            raise ValueError("Router requires a selector or at least one predicate route.")
        if selector is not None and not routes:
            raise ValueError("Router.by requires at least one keyed route.")
        self._selector = selector
        self._routes = dict(routes or {})
        self._predicate_routes = tuple(predicate_routes)
        self._default = default

    @classmethod
    def by(
        cls,
        selector: SelectorSpec[InT],
        routes: Mapping[object, Process[InT, OutT]],
        *,
        default: Process[InT, OutT] | None = None,
    ) -> Router[InT, OutT]:
        """Declare key-based routing.

        Args:
            selector: Path expression or custom selector.
            routes: Processes keyed by selector result.
            default: Optional fallback process.

        Returns:
            Router declaration.
        """

        return cls(selector=selector, routes=routes, default=default)

    @classmethod
    def when(
        cls,
        routes: Sequence[Route[InT, OutT]],
        *,
        default: Process[InT, OutT] | None = None,
    ) -> Router[InT, OutT]:
        """Declare ordered first-match predicate routing.

        Args:
            routes: Ordered predicate routes.
            default: Optional fallback process.

        Returns:
            Router declaration.
        """

        return cls(predicate_routes=routes, default=default)

    @property
    def selector(self) -> SelectorSpec[InT] | None:
        """Key selector for ``Router.by`` routes."""
        return self._selector

    @property
    def routes(self) -> Mapping[object, Process[InT, OutT]]:
        """Keyed routes for ``Router.by``."""
        return dict(self._routes)

    @property
    def predicate_routes(self) -> tuple[Route[InT, OutT], ...]:
        """Ordered predicate routes for ``Router.when``."""
        return self._predicate_routes

    @property
    def default(self) -> Process[InT, OutT] | None:
        """Fallback process."""
        return self._default


__all__ = ["Route", "Router", "evaluate_predicate", "select_value"]
