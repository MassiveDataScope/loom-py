"""Unit tests for built-in lifecycle observers."""

from __future__ import annotations

import logging

import pytest
import structlog
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter

from loom.core.config.observability import OtelConfig
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observer import otel as _otel_module
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.otel import (
    build_log_correlation_processor,
    install_otel_log_export,
)
from loom.core.observability.observer.structlog import StructlogLifecycleObserver
from loom.core.observability.topology import ROOT_SCOPES


class TestNoopObserver:
    def test_accepts_any_event_without_error(self) -> None:
        obs = NoopObserver()
        for kind in EventKind:
            obs.on_event(LifecycleEvent(scope=Scope.NODE, name="x", kind=kind))


class TestLifecycleEventHelpers:
    def test_start_end_and_error_builders_set_expected_kinds(self) -> None:
        start = LifecycleEvent.start(scope=Scope.NODE, name="step")
        end = LifecycleEvent.end(scope=Scope.NODE, name="step")
        error = LifecycleEvent.exception(scope=Scope.NODE, name="step", error="boom")

        assert start.kind is EventKind.START
        assert end.kind is EventKind.END
        assert end.status is not None
        assert end.status.value == "success"
        assert error.kind is EventKind.ERROR
        assert error.status is not None
        assert error.status.value == "failure"

    def test_otel_helpers_use_plain_event_data(self) -> None:
        event = LifecycleEvent.start(
            scope=Scope.NODE,
            name="transform",
            trace_id="trace-1",
            id="run-1",
            correlation_id="corr-1",
            meta={"flow": "ingest"},
        )

        assert event.otel_span_name() == "node:transform"
        assert event.otel_attributes() == {
            "scope": "node",
            "name": "transform",
            "trace_id": "trace-1",
            "id": "run-1",
            "correlation_id": "corr-1",
            "flow": "ingest",
        }


class TestOtelLogExport:
    def test_installs_root_handler_and_exports_stdlib_logs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = logging.getLogger()
        original_handlers = list(root.handlers)
        original_level = root.level
        exporter = InMemoryLogRecordExporter()

        monkeypatch.setattr(
            "loom.core.observability.observer.otel.atexit.register",
            lambda *_: None,
        )
        monkeypatch.setattr(
            "loom.core.observability.observer.otel._LOG_EXPORT_STATE",
            None,
        )

        handler = install_otel_log_export(
            OtelConfig(endpoint=None, service_name="loom-test", tracer_name="loom.test"),
            exporter=exporter,
        )
        try:
            assert handler is not None
            assert handler in root.handlers
            root.setLevel(logging.INFO)
            root.info("loom otel log export works")

            if _otel_module._LOG_EXPORT_STATE is not None:
                _otel_module._LOG_EXPORT_STATE[0].force_flush()
            finished = exporter.get_finished_logs()
            assert len(finished) == 1
            assert finished[0].log_record.body == "loom otel log export works"
            assert finished[0].resource.attributes["service.name"] == "loom-test"
        finally:
            current_handlers = list(root.handlers)
            for existing in current_handlers:
                if existing not in original_handlers:
                    root.removeHandler(existing)
                    existing.close()
            root.setLevel(original_level)


class TestStructlogLifecycleObserver:
    def test_start_event_calls_debug(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr(structlog, "get_logger", lambda *_: _FakeLogger())
        observer = StructlogLifecycleObserver()

        observer.on_event(LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.START))
        assert calls == [EventKind.START.value]

    def test_end_event_calls_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr(structlog, "get_logger", lambda *_: _FakeLogger())
        observer = StructlogLifecycleObserver()

        observer.on_event(
            LifecycleEvent(scope=Scope.USE_CASE, name="x", kind=EventKind.END, duration_ms=12.5)
        )
        assert calls == ["end"]

    def test_error_event_calls_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr(structlog, "get_logger", lambda *_: _FakeLogger())
        observer = StructlogLifecycleObserver()

        observer.on_event(
            LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.ERROR, error="boom")
        )
        assert calls == [EventKind.ERROR.value]

    def test_meta_forwarded_on_start(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, object] = {}

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **kwargs: object) -> None:
                captured.update(kwargs)

            def info(self, event: str, **_: object) -> None:
                # Intentional no-op: this branch is not exercised here.
                return None

            def error(self, event: str, **_: object) -> None:
                # Intentional no-op: this branch is not exercised here.
                return None

        monkeypatch.setattr(structlog, "get_logger", lambda *_: _FakeLogger())
        observer = StructlogLifecycleObserver()

        observer.on_event(
            LifecycleEvent(
                scope=Scope.NODE,
                name="x",
                kind=EventKind.START,
                meta={"flow": "my_flow", "node_idx": 0},
            )
        )
        assert captured["flow"] == "my_flow"
        assert captured["node_idx"] == 0

    def test_meta_forwarded_on_end(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, object] = {}

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **kwargs: object) -> None:
                captured.update(kwargs)

            def info(self, event: str, **_: object) -> None:
                return None

            def error(self, event: str, **_: object) -> None:
                return None

        monkeypatch.setattr(structlog, "get_logger", lambda *_: _FakeLogger())
        observer = StructlogLifecycleObserver()

        observer.on_event(
            LifecycleEvent(
                scope=Scope.FLOW,
                name="ingest",
                kind=EventKind.END,
                duration_ms=100.0,
                meta={"flow": "ingest", "node_count": 3},
            )
        )
        assert captured["flow"] == "ingest"
        assert captured["node_count"] == 3


class TestPrometheusLifecycleAdapter:
    pytest.importorskip("prometheus_client")

    def test_records_duration_on_end_event(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleAdapter(registry=registry)

        adapter.on_event(
            LifecycleEvent(
                scope=Scope.USE_CASE, name="CreateOrder", kind=EventKind.END, duration_ms=42.0
            )
        )

        assert registry.get_sample_value(
            "lifecycle_duration_seconds_count",
            {"scope": "use_case", "name": "CreateOrder"},
        ) == pytest.approx(1.0)

    def test_increments_errors_on_error_event(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleAdapter(registry=registry)

        adapter.on_event(LifecycleEvent(scope=Scope.NODE, name="transform", kind=EventKind.ERROR))

        assert registry.get_sample_value(
            "lifecycle_errors_total",
            {"scope": "node", "name": "transform"},
        ) == pytest.approx(1.0)

    def test_start_event_is_ignored(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleAdapter(registry=registry)

        adapter.on_event(LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.START))


class TestOtelLogCorrelationProcessor:
    def test_adds_trace_and_span_ids_when_span_is_recording(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        processor = build_log_correlation_processor()

        class _SpanContext:
            trace_id = 0x123
            span_id = 0x456

        class _Span:
            def is_recording(self) -> bool:
                return True

            def get_span_context(self) -> _SpanContext:
                return _SpanContext()

        monkeypatch.setattr(
            "loom.core.observability.observer.otel.trace.get_current_span", lambda: _Span()
        )

        result = processor(object(), "info", {})

        assert result["otel_trace_id"] == "00000000000000000000000000000123"
        assert result["otel_span_id"] == "0000000000000456"

    def test_ignores_non_recording_span(self, monkeypatch: pytest.MonkeyPatch) -> None:
        processor = build_log_correlation_processor()

        class _Span:
            def is_recording(self) -> bool:
                return False

        monkeypatch.setattr(
            "loom.core.observability.observer.otel.trace.get_current_span", lambda: _Span()
        )

        result = processor(object(), "info", {"x": "1"})

        assert result == {"x": "1"}


class TestObservabilityTopology:
    def test_root_scopes_are_declared_explicitly(self) -> None:
        assert Scope.USE_CASE in ROOT_SCOPES
        assert Scope.NODE not in ROOT_SCOPES
