"""The trace ids a log record carries."""

from __future__ import annotations

from dataclasses import dataclass

from opentelemetry import trace

from loom.core.log_names import LogField
from loom.core.tracing.context import get_trace_id


@dataclass(frozen=True, slots=True)
class TraceIds:
    """The ids that tie a record to its trace.

    Inside a recording OpenTelemetry span both ids are the span's, so a collector
    correlates the record with the trace; the span may continue a caller's
    ``traceparent``. Outside one, only loom's own trace id is known.

    Attributes:
        trace_id: The span's trace id as 32 lowercase hex digits, else loom's id.
        span_id: The recording span's id as 16 lowercase hex digits; none outside a span.
    """

    trace_id: str | None
    span_id: str | None = None

    @classmethod
    def current(cls, loom_trace_id: str | None = None) -> TraceIds:
        """The ids of the running work.

        Args:
            loom_trace_id: Loom's id when no span records; the active one by default.
        """
        span = trace.get_current_span()
        if not span.is_recording():
            return cls(trace_id=loom_trace_id or get_trace_id())
        context = span.get_span_context()
        return cls(
            trace_id=format(context.trace_id, "032x"),
            span_id=format(context.span_id, "016x"),
        )

    def fields(self) -> dict[str, str]:
        """The log fields it writes, without the ids it does not know."""
        known = {LogField.TRACE_ID: self.trace_id, LogField.SPAN_ID: self.span_id}
        return {key: value for key, value in known.items() if value is not None}
