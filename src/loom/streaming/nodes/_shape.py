"""Logical stream shapes and explicit shape adapters."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from loom.core.model import LoomFrozenStruct


class StreamShape(StrEnum):
    """Logical data shape at a streaming graph edge."""

    RECORD = "record"
    MANY = "many"
    BATCH = "batch"
    NONE = "none"


class WindowStrategy(StrEnum):
    """Windowing strategy for batch collection.

    Determines how records are grouped into batches at runtime.

    Values:
        COLLECT: Count-and-timeout collect using processing time. Suitable
            for testing and low-volume flows. Produces batches of up to
            ``max_records`` items or after ``timeout_ms`` milliseconds,
            whichever comes first.
        TUMBLING: Fixed-length event-time tumbling window driven by
            ``MessageMeta.produced_at_ms``. Not yet implemented.
        SESSION: Event-time session window grouped by inactivity gap driven
            by ``MessageMeta.produced_at_ms``. Not yet implemented.

    Note:
        ``COLLECT`` is the only strategy available in the current adapter.
        ``TUMBLING`` and ``SESSION`` are forward-declared and will raise
        :class:`loom.streaming.compiler._compiler.CompilationError` until implemented.
    """

    COLLECT = "collect"
    TUMBLING = "tumbling"
    SESSION = "session"


class ForEach(LoomFrozenStruct, frozen=True):
    """Explicit shape adapter from ``batch`` to ``record``.

    Pattern:
        Shape adapter.

    Note:
        This is a shape-level adapter only. It flattens a batch stream back
        to individual records so downstream ``RecordStep`` nodes can consume
        them. It carries no business semantics.
    """


class CollectBatch(LoomFrozenStruct, frozen=True):
    """Explicit shape adapter from ``record`` to ``batch``.

    Pattern:
        Shape adapter.

    Groups individual records into batches before handing them to downstream
    batch-aware nodes. Batch is an **optimization grouping**, not a
    first-class semantic unit — downstream nodes receive and return individual
    records after the batch step executes.

    Args:
        max_records: Maximum records collected into one batch.
        timeout_ms: Maximum wait time before materializing a partial batch.
        window: Windowing strategy to use for grouping. Defaults to
            ``WindowStrategy.COLLECT`` (processing-time count-and-timeout).
        event_time: When ``True``, use ``MessageMeta.produced_at_ms`` as the
            event clock instead of wall time. Requires a windowing strategy
            other than ``COLLECT``.

    Raises:
        ValueError: If ``max_records`` or ``timeout_ms`` are below one, or
            if ``event_time=True`` is combined with ``WindowStrategy.COLLECT``.

    Example:
        Collect up to 100 records or wait 500 ms::

            CollectBatch(max_records=100, timeout_ms=500)
    """

    max_records: int
    timeout_ms: int
    window: WindowStrategy = WindowStrategy.COLLECT
    event_time: bool = False

    def __post_init__(self) -> None:
        """Validate batch collection parameters."""
        if self.max_records < 1:
            raise ValueError("CollectBatch.max_records must be greater than zero.")
        if self.timeout_ms < 1:
            raise ValueError("CollectBatch.timeout_ms must be greater than zero.")
        if self.event_time and self.window is WindowStrategy.COLLECT:
            raise ValueError("event_time=True requires a windowing strategy other than COLLECT.")


class Drain(LoomFrozenStruct, frozen=True):
    """Explicit terminal adapter from ``record`` to ``none``.

    Pattern:
        Shape adapter.
    """

    router_branch_safe: ClassVar[bool] = True


__all__ = ["CollectBatch", "Drain", "ForEach", "StreamShape", "WindowStrategy"]
