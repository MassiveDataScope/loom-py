"""Broadcast node — inclusive fan-out to multiple independent branches."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from loom.core.errors import InvalidValue
from loom.core.model import LoomFrozenStruct
from loom.streaming.core._message import StreamPayload
from loom.streaming.nodes._boundary import IntoTopic

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


class BroadcastRoute(LoomFrozenStruct, Generic[InT, OutT], frozen=True):
    """One branch of a :class:`Broadcast` node.

    Pattern:
        Fan-out branch.

    Args:
        process: Transformation nodes applied to every incoming message on
            this branch.
        output: Optional terminal Kafka topic that receives the transformed
            messages. When omitted, the branch acts as a discard branch after
            its inner process completes.

    Example::

        BroadcastRoute(
            process=Process(RecordStep(to_analytics_event)),
            output=IntoTopic("events.analytics", payload=AnalyticsEvent),
        )
    """

    process: Process[InT, OutT]
    output: IntoTopic[OutT] | None = None


class Broadcast(Generic[InT]):
    """Terminal fan-out node that delivers every message to all branches simultaneously.

    Pattern:
        Inclusive fan-out.

    Unlike :class:`~loom.streaming.Fork`, which routes each message to exactly
    one branch, ``Broadcast`` copies the message to every declared branch.
    All branches are independent: they may apply different transformations and
    write to different output topics.

    ``Broadcast`` is terminal — no process nodes may follow it.

    Args:
        *routes: One or more :class:`BroadcastRoute` declarations.

    Raises:
        InvalidValue: If no routes are provided.

    Example::

        Broadcast(
            BroadcastRoute(
                process=Process(RecordStep(to_analytics_event)),
                output=IntoTopic("events.analytics", payload=AnalyticsEvent),
            ),
            BroadcastRoute(
                process=Process(RecordStep(to_fulfillment_order)),
                output=IntoTopic("orders.fulfillment", payload=FulfillmentOrder),
            ),
        )
    """

    __slots__ = ("_routes",)

    def __init__(self, *routes: BroadcastRoute[InT, Any]) -> None:
        if not routes:
            raise InvalidValue("Broadcast requires at least one route.")
        self._routes = routes

    @property
    def routes(self) -> tuple[BroadcastRoute[InT, Any], ...]:
        """Ordered fan-out branches."""
        return self._routes


__all__ = ["Broadcast", "BroadcastRoute"]
