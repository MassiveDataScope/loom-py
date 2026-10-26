"""Storage sink protocols for the streaming DSL.

Defines the two-level contract between the streaming graph and any storage backend:
- SinkPartition: what runs per Bytewax worker (write_batch / close).
- IntoSink: what any Into* terminal node exposes to the compiler and adapter.

The compiler detects IntoSink nodes by structural protocol check — it never
inspects type names. Any frozen dataclass that satisfies IntoSink is a
first-class terminal node without registration or inheritance.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, TypeVar, runtime_checkable

from loom.core.async_bridge import AsyncBridge
from loom.streaming.core._message import StreamPayload

EventT_contra = TypeVar("EventT_contra", bound=StreamPayload, contravariant=True)
EventT = TypeVar("EventT", bound=StreamPayload)  # invariant — used where type[T] appears


class SinkPartition(Protocol[EventT_contra]):
    """Per-worker write interface for a storage sink.

    The adapter calls write_batch() once per Bytewax epoch and close() on
    shutdown. Implementations must be sync-safe — use AsyncBridge for storage
    targets that require async I/O (e.g. SQLAlchemy async sessions). After a
    call raises, the counts later calls return complete nothing.

    Args:
        EventT_contra: Contravariant event type this partition accepts.
    """

    def write_batch(self, items: Sequence[EventT_contra]) -> int:
        """Write or buffer a batch of events for this epoch.

        Args:
            items: Events delivered by Bytewax for the current epoch.

        Returns:
            How many of the oldest events not yet reported durable are durable
            now, across this and earlier calls; ``0`` when this call only
            buffered. Under at-least-once delivery the adapter completes the
            source offsets of exactly that many events, oldest first, so events
            must become durable in the order they arrived.
        """
        ...

    def close(self) -> int:
        """Release resources and flush any pending data.

        Called once per worker on shutdown or after a failed run. Must be
        idempotent — the adapter may call close() even when write_batch()
        was never invoked.

        Returns:
            How many buffered events this call made durable, as for
            :meth:`write_batch`. Events still buffered and not flushed here are
            never completed, so their offsets are consumed again.
        """
        ...


@runtime_checkable
class IntoSink(Protocol[EventT]):
    """Structural protocol satisfied by all Into* terminal storage sink nodes.

    The compiler detects nodes implementing this protocol through a structural
    isinstance check and resolves their config from streaming.sinks.<name>.
    The adapter calls build_partition() once per Bytewax worker to obtain the
    SinkPartition that handles epoch writes and shutdown.

    Implementing a custom Into* node requires no framework imports, no
    registration, and no base class — any frozen dataclass with the four
    declared members satisfies this protocol and is treated identically to
    built-in nodes such as IntoTable. The three attributes are read-only on
    purpose: a frozen dataclass, a msgspec struct or a class variable all
    satisfy them, so a sink never has to be mutable to be a sink.

    Example::

        @dataclass(frozen=True)
        class IntoMongo(Generic[EventT]):
            payload: type[EventT]
            collection: str
            name: str = ""
            router_branch_safe: ClassVar[bool] = True

            def build_partition(
                self,
                config,
                worker_index,
                worker_count,
                bridge=None,
                session_manager=None,
            ):
                # MongoPartition.write_batch and .close return how many events
                # they made durable; see SinkPartition.
                return MongoPartition(url=config["url"], collection=self.collection)

    Args:
        EventT_contra: Contravariant event type this sink accepts.
    """

    @property
    def payload(self) -> type[EventT]:
        """Event type this sink accepts."""
        ...

    @property
    def name(self) -> str:
        """Key this sink's configuration lives under in ``streaming.sinks``."""
        ...

    @property
    def router_branch_safe(self) -> bool:
        """Whether this sink may appear inside a Router branch.

        Only the attribute's presence is checked.
        """
        ...

    def build_partition(
        self,
        config: Any,
        worker_index: int,
        worker_count: int,
        bridge: AsyncBridge | None = None,
        session_manager: Any = None,
    ) -> SinkPartition[EventT]:
        """Build the per-worker partition for this sink.

        Called once per Bytewax worker at startup. The returned SinkPartition
        is owned by that worker for the lifetime of the run.

        Args:
            config:       Resolved ``streaming.sinks.<name>`` config section, of
                whatever shape this sink's backend resolves it to.
            worker_index: Zero-based index of the worker calling this method.
            worker_count: Total number of workers in this run.
            bridge:       Optional async bridge provided by the Bytewax runtime.
            session_manager: Optional adapter-owned backend resource, of whatever
                shape the sink's backend needs; ``None`` for sinks that need none.

        Returns:
            A SinkPartition that will handle write_batch() and close() calls.
        """
        ...


__all__ = ["IntoSink", "SinkPartition"]
