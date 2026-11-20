"""Helpers for asserting on exported spans in streaming observability tests.

Assertions in these tests read span structure — trace ids, parents, links,
counts — from a real SDK exporter. A test that builds a runtime with no
exporter gets non-recording spans and can assert nothing about them, which is
how four tracing defects shipped behind a green suite.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import Sampler

from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observability import ObservabilityRuntime
from loom.core.observability.otel_ids import LoomMessageIdGenerator


class EventCollector:
    """Lifecycle observer that keeps every event it receives.

    Span structure answers "what shape is the trace"; lifecycle events answer
    "was a START matched by an END". ``list.append`` is atomic, which is all the
    thread safety a collector needs under the multi-worker runtime.
    """

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one lifecycle event."""
        self.events.append(event)

    def scoped(self, scope: Scope) -> list[LifecycleEvent]:
        """Return every recorded event of one scope, in arrival order."""
        return [event for event in self.events if event.scope is scope]


@dataclass(frozen=True, slots=True)
class SpanRecorder:
    """An observability runtime wired to an in-memory span exporter."""

    runtime: ObservabilityRuntime
    exporter: InMemorySpanExporter
    collector: EventCollector

    def spans(self) -> tuple[ReadableSpan, ...]:
        """Return every exported span, in completion order."""
        return self.exporter.get_finished_spans()

    def names(self) -> list[str]:
        """Return the names of every exported span."""
        return [span.name for span in self.spans()]

    def named(self, name: str) -> list[ReadableSpan]:
        """Return every exported span with one name."""
        return [span for span in self.spans() if span.name == name]

    def one(self, name: str) -> ReadableSpan:
        """Return the single exported span with one name."""
        matches = self.named(name)
        assert len(matches) == 1, f"expected exactly one {name!r}, got {len(matches)}"
        return matches[0]

    def traces(self) -> dict[str, list[str]]:
        """Return the span names of every exported trace, keyed by trace id."""
        grouped: dict[str, list[str]] = {}
        for span in self.spans():
            grouped.setdefault(hex_trace(span), []).append(span.name)
        return grouped

    def trace_of(self, name: str) -> str:
        """Return the trace id of the single exported span with one name."""
        return hex_trace(self.one(name))

    def name_counts(self) -> Counter[str]:
        """Return how many spans were exported under each name."""
        return Counter(self.names())


def build_recorder(
    *,
    sampler: Sampler | None = None,
    max_span_links: int = 128,
) -> SpanRecorder:
    """Build a runtime whose spans land in an in-memory exporter.

    Args:
        sampler: Sampler for the private provider. Defaults to the SDK default.
        max_span_links: Bound on the links a batch span may carry.

    Returns:
        The runtime paired with the exporter its spans land in.
    """
    exporter = InMemorySpanExporter()
    provider = (
        TracerProvider(id_generator=LoomMessageIdGenerator(), sampler=sampler)
        if sampler is not None
        else TracerProvider(id_generator=LoomMessageIdGenerator())
    )
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    collector = EventCollector()
    runtime = ObservabilityRuntime(
        [collector],
        tracer=provider.get_tracer("loom.test"),
        _max_span_links=max_span_links,
    )
    return SpanRecorder(runtime=runtime, exporter=exporter, collector=collector)


def hex_trace(span: ReadableSpan) -> str:
    """Return a span's trace id as 32 lowercase hex characters."""
    context = span.get_span_context()
    assert context is not None
    return format(context.trace_id, "032x")


def linked_span_ids(span: ReadableSpan) -> set[int]:
    """Return the span ids one span links to."""
    return {link.context.span_id for link in span.links}


def span_ids(spans: list[ReadableSpan]) -> set[int]:
    """Return the span ids of a group of spans."""
    ids: set[int] = set()
    for span in spans:
        context = span.get_span_context()
        assert context is not None
        ids.add(context.span_id)
    return ids


__all__ = [
    "EventCollector",
    "SpanRecorder",
    "build_recorder",
    "hex_trace",
    "linked_span_ids",
    "span_ids",
]
