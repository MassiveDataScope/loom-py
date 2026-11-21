"""Span attributes derived from a streaming message, and terminal spans.

A message must be traceable from ingestion, through every node, to its death.
The trace id already travels on :class:`~loom.streaming.core._message.MessageMeta`;
what was missing is the *last* span — the one that says how the message ended.

``parent_trace_id`` and ``causation_id`` name a **different** message's trace.
They are emitted as attributes, never as the OTEL parent: another message's
trace is a link or an attribute, not a parent.
"""

from __future__ import annotations

from loom.core.logger import NO_ATTRIBUTES, LogField, LogKey, Scalar, SpanAttributes
from loom.core.observability.event import Scope, TerminalReason
from loom.core.observability.observability import Observability
from loom.core.observability.span import LoomSpan
from loom.streaming.core._message import MessageMeta
from loom.streaming.log_events import StreamingLogField


def message_attributes(meta: MessageMeta) -> SpanAttributes:
    """Return the ``loom.*`` span attributes carried by one message.

    Only the fields that are actually set are returned, so a span never
    advertises an empty lineage.

    Args:
        meta: Metadata of the message the span belongs to.

    Returns:
        Attribute mapping ready to merge into a span's meta fields.
    """
    optional: dict[LogKey, Scalar | None] = {
        StreamingLogField.MESSAGE_ID: meta.message_id,
        StreamingLogField.PARENT_TRACE_ID: meta.parent_trace_id,
        StreamingLogField.CAUSATION_ID: meta.causation_id,
        StreamingLogField.MESSAGE_TYPE: meta.message_type,
        LogField.MESSAGING_DESTINATION_NAME: meta.topic,
        StreamingLogField.KAFKA_PARTITION: meta.partition,
        StreamingLogField.KAFKA_OFFSET: meta.offset,
    }
    return {key: value for key, value in optional.items() if value is not None}


def open_terminal_span(
    runtime: Observability,
    meta: MessageMeta,
    reason: TerminalReason,
    *,
    start_time_ns: int | None = None,
    attributes: SpanAttributes = NO_ATTRIBUTES,
) -> LoomSpan:
    """Open the span that ends one message's life, in that message's own trace.

    The span is opened as a root so it lands in the message's trace rather than
    in whatever trace the surrounding batch or node happens to run under. The
    caller owns closing it exactly once.

    Args:
        runtime: Observability runtime the span is opened on.
        meta: Metadata of the dying message — its trace id is the span's trace.
        reason: How the message ended.
        start_time_ns: Epoch nanoseconds the ending really began at, for a span
            opened after the work it describes has run.
        attributes: Extra span attributes, e.g. the sink or the error kind.

    Returns:
        The open terminal span handle.

    Example::

        span = open_terminal_span(runtime, message.meta, TerminalReason.SINK_WRITE)
        span.end()
    """
    return runtime.open_span(
        Scope.TERMINAL,
        reason.value,
        trace_id=meta.trace_id,
        correlation_id=meta.correlation_id,
        root=True,
        start_time_ns=start_time_ns,
        attributes={
            StreamingLogField.TERMINAL_REASON: reason.value,
            **message_attributes(meta),
            **attributes,
        },
    )


__all__ = ["message_attributes", "open_terminal_span"]
