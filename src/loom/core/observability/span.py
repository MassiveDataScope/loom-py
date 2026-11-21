"""Span handles for observability.

:meth:`~loom.core.observability.observability.Observability.span` covers the
common case: a span that opens and closes inside one lexical scope, and can
therefore be made *current* for its whole body.

:class:`LoomSpan` covers the case that cannot. A stream opens its span in one
``asend`` of an async generator and closes it in another, so the two ends run
in different :mod:`opentelemetry.context` contexts. A span made current in one
``asend`` would have to be detached in another — which raises internally, is
swallowed by OTEL as a *"Failed to detach context"* log line, and in between
frames leaks the span into whatever the consumer is doing. ``LoomSpan`` never
attaches, so neither can happen.

Only :class:`LoomSpan` is public here. ``SpanIdentity`` and
``apply_terminal_state`` are internal to the observability package.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from time import perf_counter
from typing import Final, Self

from opentelemetry.context import Context
from opentelemetry.trace import (
    Link,
    Span,
    SpanContext,
    StatusCode,
    Tracer,
    get_current_span,
    use_span,
)

from loom.core.logger import LogField, LogKey, Scalar, SpanAttributes
from loom.core.logger.record import exception_type, failure_fields
from loom.core.observability.event import EventKind, LifecycleEvent, Scope

EXCEPTION_EVENT: Final = "exception"
"""Name of the span event that describes a failure, as OpenTelemetry names it."""


def elapsed_ms(started: float) -> float:
    """Return the milliseconds elapsed since a ``perf_counter`` reading."""
    return (perf_counter() - started) * 1000


@dataclass(frozen=True, slots=True)
class SpanIdentity:
    """Everything the START and closing events of one span have in common.

    Internal to the observability package: it exists so that
    ``Observability.span`` and :class:`LoomSpan` cannot drift in the
    events they emit for the same span.
    """

    scope: Scope
    name: str
    trace_id: str | None
    correlation_id: str | None
    event_id: str | None
    attributes: SpanAttributes

    def with_attributes(self, extra: Mapping[LogKey, Scalar]) -> Self:
        """Return a copy carrying *extra* on top of this identity's attributes."""
        if not extra:
            return self
        return replace(self, attributes={**self.attributes, **extra})

    def start_event(self) -> LifecycleEvent:
        """Return the ``START`` event of this span."""
        return LifecycleEvent.start(
            scope=self.scope,
            name=self.name,
            trace_id=self.trace_id,
            correlation_id=self.correlation_id,
            id=self.event_id,
            attributes=self.attributes,
        )

    def end_event(self, duration_ms: float) -> LifecycleEvent:
        """Return the successful ``END`` event of this span."""
        return LifecycleEvent.end(
            scope=self.scope,
            name=self.name,
            trace_id=self.trace_id,
            correlation_id=self.correlation_id,
            id=self.event_id,
            duration_ms=duration_ms,
            attributes=self.attributes,
        )

    def error_event(self, exc: BaseException, duration_ms: float) -> LifecycleEvent:
        """Return the ``ERROR`` event of this span for a failure."""
        return LifecycleEvent.exception(
            scope=self.scope,
            name=self.name,
            trace_id=self.trace_id,
            correlation_id=self.correlation_id,
            id=self.event_id,
            duration_ms=duration_ms,
            failure=exc,
            attributes=self.attributes,
        )


def apply_terminal_state(span: Span, event: LifecycleEvent) -> None:
    """Copy the outcome carried by a closing lifecycle event onto its span.

    A failure sets the status to ``ERROR`` described by the exception type and
    adds an ``exception`` event with its type, code and frames: never its message.
    Internal to the observability package.

    Args:
        span: Span being closed.
        event: ``END`` or ``ERROR`` event describing the outcome.
    """
    if event.duration_ms is not None:
        span.set_attribute(LogField.DURATION_MS, event.duration_ms)
    if event.kind is not EventKind.ERROR:
        span.set_status(StatusCode.OK)
        return
    failure = event.failure
    if failure is None:
        span.set_status(StatusCode.ERROR)
        return
    span.set_status(StatusCode.ERROR, exception_type(type(failure)))
    span.add_event(EXCEPTION_EVENT, failure_fields(failure, stacktrace=True))


class LoomSpan:
    """An open span whose start and end do not share a lexical scope.

    The span is **never made current**: it is neither attached to nor detached
    from the ambient OTEL context, so closing it from a different context than
    the one that opened it is safe. Its parent is captured once, at open time,
    from the context that opened it — which is the correct parent.

    Use :meth:`as_current` for an inner region that *is* lexically safe.

    Prefer :meth:`~loom.core.observability.observability.Observability.span`
    whenever the span opens and closes in one scope.

    Example::

        handle = runtime.open_span(
            Scope.AGENT, "agent_run", attributes={LogField.NAME: "analyst"}
        )
        try:
            async for frame in frames:
                yield frame
        except Exception as exc:
            handle.fail(exc)
            raise
        else:
            handle.end()
    """

    def __init__(
        self,
        *,
        span: Span,
        identity: SpanIdentity,
        emit: Callable[[LifecycleEvent], None],
        on_closed: Callable[[], None],
    ) -> None:
        self._span = span
        self._identity = identity
        self._emit = emit
        self._on_closed = on_closed
        self._started = perf_counter()
        self._closed = False
        self._closing: dict[LogKey, Scalar] = {}

    @classmethod
    def open(
        cls,
        *,
        tracer: Tracer,
        identity: SpanIdentity,
        emit: Callable[[LifecycleEvent], None],
        on_closed: Callable[[], None],
        links: Sequence[Link] | None = None,
        start_time_ns: int | None = None,
        root: bool = False,
    ) -> Self:
        """Start the OTEL span and emit its ``START`` event.

        The event is emitted with the new span current — briefly, and within
        this one call — so that a log line written by an observer carries the
        span it belongs to rather than the parent's.

        Args:
            tracer: Tracer that starts the span.
            identity: Identity shared by every event of this span.
            emit: Sink for the lifecycle events of this span.
            on_closed: Called once the span has ended, so its owner can drain
                the exporter.
            links: Spans this one fans in from. A batch operation has N
                parents and a trace is a tree, so fan-in is expressed as links,
                not as parenthood.
            start_time_ns: Epoch nanoseconds the span really started at, for a
                span opened after the work it describes has already run.
                Defaults to now.
            root: Start the span with an empty context, so it has no parent
                whatever the ambient context holds.

        Returns:
            The open handle. The caller owns closing it.
        """
        start_event = identity.start_event()
        span = tracer.start_span(
            start_event.otel_span_name(),
            attributes=start_event.otel_attributes(),
            links=list(links) if links is not None else None,
            start_time=start_time_ns,
            context=Context() if root else None,
        )
        handle = cls(span=span, identity=identity, emit=emit, on_closed=on_closed)
        with use_span(
            span, end_on_exit=False, record_exception=False, set_status_on_exception=False
        ):
            emit(start_event)
        return handle

    def as_current(self) -> AbstractContextManager[Span]:
        """Make this span current for a region that starts and ends together.

        The span is not ended on exit, and neither exception recording nor
        status setting is delegated to the context manager: a failure is
        recorded exactly once, by :meth:`fail`.

        Returns:
            Context manager yielding the underlying span.
        """
        return use_span(
            self._span,
            end_on_exit=False,
            record_exception=False,
            set_status_on_exception=False,
        )

    @property
    def span_context(self) -> SpanContext:
        """Return the OTEL context of this span, for linking to it.

        A batch span links back to the spans that fanned into it, and those
        contexts must be read from spans that genuinely exist rather than
        fabricated.
        """
        return self._span.get_span_context()

    def annotate(self, attributes: Mapping[LogKey, Scalar]) -> None:
        """Record attributes the closing event of this span carries.

        For values only known once the work is done — a run's token usage, a
        batch's row count. They reach the closing lifecycle event and the OTEL
        span; the ``START`` event is long gone by then. A key given twice keeps
        the last value, and annotating a closed span does nothing.

        Args:
            attributes: Fields merged into the closing event's attributes.

        Example::

            handle.annotate({LogField.COUNT: 142})
        """
        if self._closed:
            return
        self._closing.update(attributes)

    def is_recording(self) -> bool:
        """Return whether this span will be exported.

        A sampler that dropped the span makes it non-recording. Linking to a
        span nobody exported advertises an edge to nothing, so callers building
        links must filter on this.
        """
        return self._span.is_recording()

    def end(self, *, end_time_ns: int | None = None, duration_ms: float | None = None) -> None:
        """Close the span as a success and emit its ``END`` event.

        Calling this after the span is already closed does nothing.

        Args:
            end_time_ns: Epoch nanoseconds the work really ended at, for a span
                opened after the fact with an explicit start time. Defaults to
                now.
            duration_ms: Duration reported on the ``END`` event. Defaults to
                the wall-clock time this handle has been open, which is wrong
                exactly when *end_time_ns* is needed.
        """
        self._close(
            self._closing_identity().end_event(self._duration(duration_ms)),
            end_time_ns=end_time_ns,
        )

    def fail(
        self,
        exc: BaseException,
        *,
        end_time_ns: int | None = None,
        duration_ms: float | None = None,
    ) -> None:
        """Close the span as a failure described by *exc*, and emit ``ERROR``.

        Calling this after the span is already closed does nothing.

        Args:
            exc: Failure that ended the work this span covers.
            end_time_ns: Epoch nanoseconds the work really ended at.
            duration_ms: Duration reported on the ``ERROR`` event.
        """
        self._close(
            self._closing_identity().error_event(exc, self._duration(duration_ms)),
            end_time_ns=end_time_ns,
        )

    def _closing_identity(self) -> SpanIdentity:
        return self._identity.with_attributes(self._closing)

    def _duration(self, duration_ms: float | None) -> float:
        if duration_ms is not None:
            return duration_ms
        return elapsed_ms(self._started)

    def _close(self, event: LifecycleEvent, *, end_time_ns: int | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        apply_terminal_state(self._span, event)
        if self._closing:
            self._span.set_attributes(dict(self._closing.items()))
        with use_span(
            self._span, end_on_exit=False, record_exception=False, set_status_on_exception=False
        ):
            self._emit(event)
        self._span.end(end_time=end_time_ns)
        self._on_closed()


def annotate_current_span(attributes: Mapping[str, str]) -> None:
    """Set *attributes* on the span current in this context, if any.

    The one way a layer under observability (the acting identity) marks the
    span it runs in without importing OpenTelemetry itself.
    """
    get_current_span().set_attributes(attributes)


__all__ = ["LoomSpan", "annotate_current_span"]
