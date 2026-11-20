"""Span attributes derived from a streaming message, and terminal spans.

A message must be traceable from ingestion, through every node, to its death.
The trace id already travels on :class:`~loom.streaming.core._message.MessageMeta`;
what was missing is the *last* span — the one that says how the message ended.

``parent_trace_id`` and ``causation_id`` name a **different** message's trace.
They are emitted as attributes, never as the OTEL parent: another message's
trace is a link or an attribute, not a parent.
"""

from __future__ import annotations

from collections.abc import Mapping

from loom.core.observability.event import Scope, TerminalReason
from loom.core.observability.observability import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.streaming.core._message import MessageMeta


def message_attributes(meta: MessageMeta) -> dict[str, object]:
    """Return the ``loom.*`` span attributes carried by one message.

    Only the fields that are actually set are returned, so a span never
    advertises an empty lineage.

    Args:
        meta: Metadata of the message the span belongs to.

    Returns:
        Attribute mapping ready to merge into a span's meta fields.
    """
    attrs: dict[str, object] = {"loom.message_id": meta.message_id}
    optional: dict[str, object | None] = {
        "loom.parent_trace_id": meta.parent_trace_id,
        "loom.causation_id": meta.causation_id,
        "loom.message_type": meta.message_type,
        "messaging.source.name": meta.topic,
        "messaging.kafka.partition": meta.partition,
        "messaging.kafka.offset": meta.offset,
    }
    attrs.update({key: value for key, value in optional.items() if value is not None})
    return attrs


def open_terminal_span(
    runtime: ObservabilityRuntime,
    meta: MessageMeta,
    reason: TerminalReason,
    *,
    start_time_ns: int | None = None,
    attributes: Mapping[str, object] | None = None,
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
            "terminal.reason": reason.value,
            **message_attributes(meta),
            **(attributes or {}),
        },
    )


__all__ = ["message_attributes", "open_terminal_span"]
