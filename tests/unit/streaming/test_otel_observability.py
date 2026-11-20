"""Tests for streaming OpenTelemetry wiring through the observability runtime."""

from __future__ import annotations

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.event import Scope
from loom.core.observability.observability import ObservabilityRuntime


class TestObservabilityConfig:
    def test_observability_config_defaults(self) -> None:
        cfg = ObservabilityConfig()

        assert cfg.log.enabled is True
        assert cfg.otel.enabled is False
        assert cfg.otel.config is None
        assert cfg.prometheus.enabled is False


class TestStreamingScopes:
    def test_streaming_scopes_export_one_nested_trace(self) -> None:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        runtime = ObservabilityRuntime([], tracer=provider.get_tracer("loom.streaming"))

        with runtime.span(Scope.POLL_CYCLE, "orders"), runtime.span(Scope.NODE, "transform"):
            pass

        spans = {span.name: span for span in exporter.get_finished_spans()}
        assert set(spans) == {"poll_cycle:orders", "node:transform"}
        node_parent = spans["node:transform"].parent
        poll_cycle_context = spans["poll_cycle:orders"].get_span_context()
        assert node_parent is not None
        assert poll_cycle_context is not None
        assert node_parent.span_id == poll_cycle_context.span_id
        assert spans["poll_cycle:orders"].status.status_code is StatusCode.OK
