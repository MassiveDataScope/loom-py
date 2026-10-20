"""Explode DSL node for typed multi-target event transformation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Generic, Protocol, TypeVar

from loom.streaming.core._message import StreamPayload

PayloadT_contra = TypeVar("PayloadT_contra", bound=StreamPayload, contravariant=True)
InT = TypeVar("InT", bound=StreamPayload)


class PayloadExpander(Protocol[PayloadT_contra]):
    """Expand one typed stream event into typed sub-event collections.

    Each key in the returned dict is the concrete type that the downstream
    Router uses to dispatch to the correct branch — type-keyed, not
    string-keyed. The expand() method must be pure: no I/O, no side effects,
    no knowledge of downstream sinks or storage backends.

    Declare ``outputs`` to allow the compiler to validate that every produced
    type has a matching Router branch.

    Example::

        class StoreEventExpander(PayloadExpander[StoreEvent]):
            outputs: ClassVar[tuple[type, ...]] = (StoreRow, LanguageRow)

            @classmethod
            def expand(cls, event: StoreEvent) -> dict[type, list[Any]]:
                return {
                    StoreRow: [StoreRow(id=event.store_id, name=event.name)],
                    LanguageRow: [
                        LanguageRow(code=code) for code in event.language_codes
                    ],
                }

    Args:
        PayloadT_contra: Contravariant type of the incoming stream event.
    """

    outputs: ClassVar[tuple[type, ...]]

    @classmethod
    def expand(cls, event: PayloadT_contra) -> dict[type, list[Any]]:
        """Expand one event into typed sub-event collections.

        Args:
            event: Incoming stream event to expand.

        Returns:
            Mapping from output type to list of event instances of that type.
            Every type in ``outputs`` must appear as a key.
        """
        ...


@dataclass(frozen=True)
class Explode(Generic[InT]):
    """Transformation step that fans one typed event into N typed sub-events.

    Explode sits upstream of a Router and has no knowledge of downstream
    sinks. The Router dispatches each emitted type to the matching branch —
    the same mechanism used for any multi-type stream. This keeps expanders
    and sinks fully decoupled.

    Args:
        exploder: Class implementing PayloadExpander[InT].

    Example::

        class PayloadType(Selector[Any]):
            def select(self, message: Message[Any]) -> object:
                return type(message.payload)

        Process(
            Explode(StoreEventExpander),
            Router.by(
                PayloadType(),
                routes={
                    StoreRow: Process(IntoTopic("stores.rows", payload=StoreRow)),
                    LanguageRow: Process(IntoTopic("stores.languages", payload=LanguageRow)),
                },
            ),
        )
    """

    exploder: type[PayloadExpander[InT]]


__all__ = ["Explode", "PayloadExpander"]
