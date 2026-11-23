"""Unit tests for built-in lifecycle observers."""

from __future__ import annotations

import logging

import pytest
from opentelemetry.sdk.trace import TracerProvider

from loom.core.config.observability import OtelConfig
from loom.core.errors.base import ErrorSummary
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.otel import (
    OtelSpanFilter,
    install_otel_log_export,
)
from tests.helpers.loggers import CapturedLog
from tests.unit.core.observability.conftest import LogExport


class TestNoopObserver:
    def test_accepts_any_event_without_error(self) -> None:
        obs = NoopObserver()
        for kind in EventKind:
            obs.on_event(LifecycleEvent(scope=Scope.NODE, name="x", kind=kind))


class TestLifecycleEventHelpers:
    def test_start_end_and_error_builders_set_expected_kinds(self) -> None:
        start = LifecycleEvent.start(scope=Scope.NODE, name="step")
        end = LifecycleEvent.end(scope=Scope.NODE, name="step")
        error = LifecycleEvent.exception(
            scope=Scope.NODE, name="step", failure=ErrorSummary(type="ValueError")
        )

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
    def test_the_handler_is_built_without_touching_the_root_logger(
        self, log_export: LogExport
    ) -> None:
        assert log_export.handler not in logging.getLogger().handlers

    def test_the_handler_exports_the_stdlib_logs_it_is_attached_to(
        self, log_export: LogExport
    ) -> None:
        root = logging.getLogger()
        root.addHandler(log_export.handler)

        root.info("loom otel log export works")
        log_export.exported()

        [finished] = log_export.exporter.get_finished_logs()
        assert finished.log_record.body == "loom otel log export works"
        assert finished.resource.attributes["service.name"] == "loom-test"

    def test_a_failure_is_exported_without_the_sdk_message_and_kept_for_other_handlers(
        self, log_export: LogExport
    ) -> None:
        try:
            raise ValueError("card 4111")
        except ValueError as exc:
            record = logging.LogRecord(
                "x", logging.ERROR, __file__, 1, "x.y_z", (), (type(exc), exc, exc.__traceback__)
            )

        log_export.handler.handle(record)

        (attributes,) = log_export.exported().values()
        assert "exception.message" not in attributes
        assert record.exc_info is not None

    def test_the_handler_is_idempotent_for_the_process(self, log_export: LogExport) -> None:
        again = install_otel_log_export(OtelConfig(endpoint=None, service_name="other"))

        assert again is log_export.handler


@pytest.fixture
def lifecycle_log(caplog: pytest.LogCaptureFixture) -> CapturedLog:
    caplog.set_level(logging.DEBUG, logger="loom.core.observability.observer.log")
    return CapturedLog(caplog)


def _levels(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.levelname for record in caplog.records]


class TestLogLifecycleObserver:
    @pytest.mark.parametrize(
        ("event", "level", "name"),
        [
            pytest.param(
                LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.START),
                "DEBUG",
                "observability.lifecycle_started",
                id="node-start",
            ),
            pytest.param(
                LifecycleEvent(scope=Scope.PIPELINE, name="x", kind=EventKind.START),
                "INFO",
                "observability.lifecycle_started",
                id="pipeline-start",
            ),
            pytest.param(
                LifecycleEvent(scope=Scope.USE_CASE, name="x", kind=EventKind.END, duration_ms=1),
                "INFO",
                "observability.lifecycle_ended",
                id="end",
            ),
            pytest.param(
                LifecycleEvent(
                    scope=Scope.NODE,
                    name="x",
                    kind=EventKind.ERROR,
                    failure=ErrorSummary(type="ValueError"),
                ),
                "ERROR",
                "observability.lifecycle_failed",
                id="error",
            ),
            pytest.param(
                LifecycleEvent(scope=Scope.TRANSPORT, name="x", kind=EventKind.END),
                "DEBUG",
                "observability.lifecycle_ended",
                id="transport-end",
            ),
            pytest.param(
                LifecycleEvent(
                    scope=Scope.TRANSPORT,
                    name="x",
                    kind=EventKind.ERROR,
                    failure=ErrorSummary(type="ValueError"),
                ),
                "WARNING",
                "observability.lifecycle_failed",
                id="transport-error",
            ),
        ],
    )
    def test_each_kind_and_scope_logs_one_record_at_its_level(
        self,
        event: LifecycleEvent,
        level: str,
        name: str,
        caplog: pytest.LogCaptureFixture,
        lifecycle_log: CapturedLog,
    ) -> None:
        LogLifecycleObserver().on_event(event)

        assert (_levels(caplog), lifecycle_log.messages) == ([level], [name])

    def test_a_record_carries_the_scope_the_name_and_the_trace(
        self, lifecycle_log: CapturedLog
    ) -> None:
        LogLifecycleObserver().on_event(
            LifecycleEvent(
                scope=Scope.USE_CASE,
                name="CreateOrder",
                kind=EventKind.END,
                duration_ms=12.5,
                status=LifecycleStatus.SUCCESS,
                trace_id="trace-1",
            )
        )

        [fields] = lifecycle_log.named("observability.lifecycle_ended")
        assert (fields["loom.scope"], fields["loom.name"], fields["trace_id"]) == (
            "use_case",
            "CreateOrder",
            "trace-1",
        )
        assert (fields["loom.duration_ms"], fields["loom.status"]) == (12.5, "success")

    def test_meta_is_forwarded_under_its_own_prefix(self, lifecycle_log: CapturedLog) -> None:
        LogLifecycleObserver().on_event(
            LifecycleEvent(
                scope=Scope.NODE,
                name="x",
                kind=EventKind.START,
                meta={"flow": "my_flow", "node_idx": 0},
            )
        )

        [fields] = lifecycle_log.named("observability.lifecycle_started")
        assert (fields["loom.meta.flow"], fields["loom.meta.node_idx"]) == ("my_flow", 0)

    def test_a_meta_key_that_is_a_record_attribute_does_not_break_logging(
        self, lifecycle_log: CapturedLog
    ) -> None:
        LogLifecycleObserver().on_event(
            LifecycleEvent(
                scope=Scope.FLOW, name="ingest", kind=EventKind.END, meta={"name": "orders"}
            )
        )

        [fields] = lifecycle_log.named("observability.lifecycle_ended")
        assert fields["loom.meta.name"] == "orders"


class TestPrometheusLifecycleObserver:
    pytest.importorskip("prometheus_client")

    def test_records_duration_on_end_event(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleObserver

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleObserver(registry=registry)

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

        from loom.prometheus.lifecycle import PrometheusLifecycleObserver

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleObserver(registry=registry)

        adapter.on_event(LifecycleEvent(scope=Scope.NODE, name="transform", kind=EventKind.ERROR))

        assert registry.get_sample_value(
            "lifecycle_errors_total",
            {"scope": "node", "name": "transform"},
        ) == pytest.approx(1.0)

    def test_start_event_is_ignored(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleObserver

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleObserver(registry=registry)

        adapter.on_event(LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.START))


class TestOtelSpanFilter:
    def test_adds_trace_and_span_ids_when_span_is_recording(self) -> None:
        tracer = TracerProvider().get_tracer("tests")
        record = logging.makeLogRecord({})

        with tracer.start_as_current_span("work") as span:
            OtelSpanFilter().filter(record)
            context = span.get_span_context()

        assert record.__dict__["otel_trace_id"] == format(context.trace_id, "032x")
        assert record.__dict__["otel_span_id"] == format(context.span_id, "016x")

    def test_ignores_a_record_outside_any_span(self) -> None:
        record = logging.makeLogRecord({})

        assert OtelSpanFilter().filter(record) is True
        assert "otel_trace_id" not in record.__dict__
