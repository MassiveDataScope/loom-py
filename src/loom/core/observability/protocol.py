"""Structural contracts of the observability runtime."""

from __future__ import annotations

from typing import Protocol

from loom.core.observability.event import LifecycleEvent


class LifecycleObserver(Protocol):
    """Observer that receives lifecycle events from an ``Observability``.

    Implementors must be safe to call from any thread. Raising inside
    ``on_event`` is allowed — ``Observability.emit`` isolates failures
    per-observer so one broken observer never interrupts the others.

    Example::

        class MyObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                if event.kind is EventKind.ERROR:
                    alert(event.error)
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event.

        Args:
            event: Immutable lifecycle event from the runtime.
        """
        ...


class SpanFlusher(Protocol):
    """Span exporter pipeline the runtime may drain on demand.

    Implemented by ``opentelemetry.sdk.trace.TracerProvider``. Declaring it
    structurally keeps the SDK — an extras-only dependency — out of the
    runtime's imports.

    Example::

        provider.add_span_processor(BatchSpanProcessor(exporter))
        flusher: SpanFlusher = provider
    """

    def force_flush(self) -> bool:
        """Export every span already ended.

        Returns:
            ``True`` when the pipeline drained before its own timeout.
        """
        ...


__all__ = ["LifecycleObserver", "SpanFlusher"]
