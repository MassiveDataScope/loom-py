from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Protocol

from loom.core.engine.events import ExecutionEvent
from loom.core.log_names import LogEvent, LogField


class ExecutionObserver(Protocol):
    """Port for observing runtime and compile-time events.

    Implement this protocol to plug in any backend (Prometheus, StatsD, a
    job-outcome recorder, in-memory counters, etc.) without coupling
    the framework to a concrete provider.

    Transport adapters may also implement this protocol to record
    HTTP or Kafka-level events alongside UseCase-level events.

    Example::

        class PrometheusRecorder:
            def on_event(self, event: ExecutionEvent) -> None:
                if event.kind == EventKind.EXEC_DONE:
                    DURATION.labels(usecase=event.use_case_name).observe(
                        (event.duration_ms or 0) / 1000
                    )
    """

    def on_event(self, event: ExecutionEvent) -> None:
        """Process a runtime or compile-time event.

        Args:
            event: Immutable event carrying kind, use case name,
                optional step name, duration, status, and error.
        """
        ...


_log = logging.getLogger(__name__)


class ExecutionObserverEvent(LogEvent):
    """What the fan-out of execution observers logs."""

    FAILED = "engine.execution_observer_failed"


class NullExecutionObserver:
    """An execution observer observing nothing, for an engine given none."""

    __slots__ = ()

    def on_event(self, event: ExecutionEvent) -> None:
        """Ignore *event*."""


class FanOutExecutionObserver:
    """Delivers each event to every observer, whatever one of them raises.

    A failing observer is logged at WARNING and skipped: it breaks neither the
    run that emitted the event nor the observers after it. ``on_event`` may be
    called from any thread, so every observer must be safe for that.
    """

    __slots__ = ("_observers",)

    def __init__(self, observers: Sequence[ExecutionObserver]) -> None:
        self._observers = tuple(observers)

    @classmethod
    def combine(
        cls, own: ExecutionObserver | None, declared: Sequence[ExecutionObserver]
    ) -> ExecutionObserver | None:
        """Return what observes with *own* first and the *declared* ones after, or ``None``.

        Nothing to fan out is returned as is: no observer is ``None`` and a
        lone *own* observer is itself.
        """
        if not declared:
            return own
        return cls([own, *declared] if own is not None else declared)

    def on_event(self, event: ExecutionEvent) -> None:
        """Deliver *event* to each observer in order."""
        for observer in self._observers:
            try:
                observer.on_event(event)
            except Exception:
                _log.warning(
                    ExecutionObserverEvent.FAILED,
                    extra={
                        LogField.OBSERVER: type(observer).__name__,
                        LogField.EVENT_KIND: event.kind,
                    },
                    exc_info=True,
                )
