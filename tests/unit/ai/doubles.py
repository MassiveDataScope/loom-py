"""Doubles shared across the loom.ai unit tests."""

from __future__ import annotations

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.core.observability.observability import Observability


def tracing_runtime() -> tuple[Observability, InMemorySpanExporter]:
    """An in-memory tracer that exports every span the host opens."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return Observability([], tracer=provider.get_tracer("loom.ai")), exporter
