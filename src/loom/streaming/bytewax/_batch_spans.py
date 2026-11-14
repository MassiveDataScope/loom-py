"""The N+1 rule: a batch operation produces N message spans and one batch span.

A batch has **N parents**. A trace is a tree with one parent per span, so it
cannot express that. OTEL span links are the standard mechanism for fan-in, so
every batch operation produces:

- one *participation* span per message, in that message's own trace, carrying
  ``loom.batch_id`` — this is where a message's story records that it took part
  in a batch, and for a sink write it is where the message dies;
- one *batch* span, in a trace of its own, with one link per participation
  span.

That is navigable both ways: message to batch through ``loom.batch_id``, batch
to messages through the links.

Links follow recording. A link is added only for a participation span that
will actually be exported: advertising an edge to a span the sampler dropped
points at nothing. The count is bounded by ``OtelConfig.max_span_links``, and
the batch span says so with ``loom.links_truncated`` when the bound bites.

Spans are opened *after* the work, with the real window as explicit start and
end timestamps, because the links can only point at contexts that genuinely
exist — which they do, because they were just created.

Sampling caveat: the batch span is a root in a trace of its own, so a
ratio-based sampler decides on *its* trace id like any other root. At a low
ratio most batch spans are therefore dropped along with most messages. What
holds at every ratio is the invariant that matters — a batch span that *is*
exported links to exactly the participation spans of its batch that were also
exported, and never to one that was not.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from time import time_ns

from opentelemetry.trace import Link

from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.core.tracing.context import generate_trace_id
from loom.streaming.core._message import MessageMeta

_NS_PER_MS = 1_000_000

ParticipationOpener = Callable[[MessageMeta, Mapping[str, object], int], LoomSpan]
"""Opens one message's participation span, given its meta, extra attributes, and start time."""


@dataclass(frozen=True, slots=True)
class BatchWindow:
    """The wall-clock window one batch operation really occupied."""

    started_ns: int
    ended_ns: int

    @classmethod
    def since(cls, started_ns: int) -> BatchWindow:
        """Close a window that started at *started_ns* now."""
        return cls(started_ns=started_ns, ended_ns=time_ns())

    @property
    def duration_ms(self) -> float:
        """Return the window length in milliseconds."""
        return (self.ended_ns - self.started_ns) / _NS_PER_MS


@dataclass(frozen=True, slots=True)
class BatchSpan:
    """Identity and attributes of the span covering the batch operation itself."""

    scope: Scope
    name: str
    attributes: Mapping[str, object]


def emit_batch_spans(
    runtime: ObservabilityRuntime,
    metas: Sequence[MessageMeta],
    *,
    batch: BatchSpan,
    open_participation: ParticipationOpener,
    window: BatchWindow,
    error: BaseException | None = None,
) -> str:
    """Emit the N participation spans and the one batch span of a batch operation.

    Args:
        runtime: Observability runtime the spans are opened on.
        metas: Metadata of every message in the batch, in batch order.
        batch: Identity and attributes of the batch span.
        open_participation: Opens one message's participation span. Receives
            the message meta, the batch attributes to merge in, and the epoch
            nanoseconds the batch started at.
        window: Real start and end of the batch operation.
        error: Failure that ended the batch, if it failed. Recorded on every
            span the batch produced, since every message shares the outcome.

    Returns:
        The batch identifier stamped on every span, so the caller can log it.

    Example::

        window_start = time_ns()
        partition.write_batch(payloads)
        emit_batch_spans(
            runtime,
            [item.meta for item in items],
            batch=BatchSpan(Scope.WRITE, "orders:sink", {"sink": "orders"}),
            open_participation=open_write_terminal,
            window=BatchWindow.since(window_start),
        )
    """
    batch_id = generate_trace_id()
    batch_attributes: Mapping[str, object] = {"loom.batch_id": batch_id}
    links: list[Link] = []
    truncated = False
    for meta in metas:
        handle = open_participation(meta, batch_attributes, window.started_ns)
        if handle.is_recording():
            if len(links) < runtime.max_span_links:
                links.append(Link(handle.span_context))
            else:
                truncated = True
        _close(handle, window, error)

    _close(
        runtime.open_span(
            batch.scope,
            batch.name,
            trace_id=batch_id,
            root=True,
            links=links,
            start_time_ns=window.started_ns,
            attributes={
                **batch_attributes,
                "loom.batch_size": len(metas),
                "loom.links_truncated": truncated,
                **batch.attributes,
            },
        ),
        window,
        error,
    )
    return batch_id


def _close(handle: LoomSpan, window: BatchWindow, error: BaseException | None) -> None:
    if error is not None:
        handle.fail(error, end_time_ns=window.ended_ns, duration_ms=window.duration_ms)
        return
    handle.end(end_time_ns=window.ended_ns, duration_ms=window.duration_ms)


__all__ = [
    "BatchSpan",
    "BatchWindow",
    "ParticipationOpener",
    "emit_batch_spans",
]
