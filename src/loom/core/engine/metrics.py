from __future__ import annotations

from typing import Protocol

from loom.core.engine.events import RuntimeEvent


class MetricsAdapter(Protocol):
    """Port for recording runtime and compile-time events.

    Implement this protocol to plug in any metrics backend
    (Prometheus, StatsD, in-memory counters, etc.) without coupling
    the framework to a concrete provider.

    Transport adapters may also implement this protocol to record
    HTTP or Kafka-level metrics alongside UseCase-level events.

    Example::

        class PrometheusAdapter:
            def on_event(self, event: RuntimeEvent) -> None:
                if event.kind == EventKind.EXEC_DONE:
                    DURATION.labels(usecase=event.use_case_name).observe(
                        (event.duration_ms or 0) / 1000
                    )
    """

    def on_event(self, event: RuntimeEvent) -> None:
        """Process a runtime or compile-time event.

        Args:
            event: Immutable event carrying kind, use case name,
                optional step name, duration, status, and error.
        """
        ...


class NoMetrics:
    """A metrics adapter recording nothing, for an engine given none."""

    __slots__ = ()

    def on_event(self, event: RuntimeEvent) -> None:
        """Ignore *event*."""
