"""Unit tests for built-in lifecycle observers."""

from __future__ import annotations

import logging

import pytest
from opentelemetry.sdk.trace import TracerProvider

from loom.core.config.observability import OtelConfig
from loom.core.logger import LogField
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observability import Observability
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.otel import install_otel_log_export, otel_trace_ids
from loom.core.tracing.context import active_trace_id
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
        error = LifecycleEvent.exception(scope=Scope.NODE, name="step", failure=ValueError("x"))

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
            attributes={LogField.COUNT: 3},
        )

        assert event.otel_span_name() == "node:transform"
        assert event.otel_attributes() == {
            "loom.scope": "node",
            "loom.name": "transform",
            "trace_id": "trace-1",
            "loom.event_id": "run-1",
            "loom.correlation_id": "corr-1",
            "loom.count": 3,
        }

    def test_the_runtime_drops_the_exception_once_every_observer_saw_it(self) -> None:
        seen: list[BaseException | None] = []
        event = LifecycleEvent.exception(scope=Scope.NODE, name="step", failure=ValueError("x"))

        class _Seeing:
            def on_event(self, received: LifecycleEvent) -> None:
                seen.append(received.failure)

        Observability([_Seeing()]).emit(event)

        assert (seen[0] is not None, event.failure) == (True, None)


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
                LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.ERROR),
                "ERROR",
                "observability.lifecycle_ended",
                id="error",
            ),
            pytest.param(
                LifecycleEvent(scope=Scope.TRANSPORT, name="x", kind=EventKind.END),
                "DEBUG",
                "observability.lifecycle_ended",
                id="transport-end",
            ),
            pytest.param(
                LifecycleEvent(scope=Scope.TRANSPORT, name="x", kind=EventKind.ERROR),
                "WARNING",
                "observability.lifecycle_ended",
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

    def test_the_attributes_are_fields_under_their_own_keys(
        self, lifecycle_log: CapturedLog
    ) -> None:
        LogLifecycleObserver().on_event(
            LifecycleEvent(
                scope=Scope.NODE,
                name="x",
                kind=EventKind.START,
                attributes={LogField.COUNT: 2, LogField.HTTP_ROUTE: "/orders"},
            )
        )

        [fields] = lifecycle_log.named("observability.lifecycle_started")
        assert (fields["loom.count"], fields["http.route"]) == (2, "/orders")


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


class TestOtelTraceIds:
    def test_a_recording_span_gives_the_one_trace_id_and_its_span_id(self) -> None:
        tracer = TracerProvider().get_tracer("tests")

        with tracer.start_as_current_span("work") as span:
            ids = otel_trace_ids()
            context = span.get_span_context()

        assert ids == (format(context.trace_id, "032x"), format(context.span_id, "016x"))

    def test_outside_any_span_the_trace_id_is_loom_s(self) -> None:
        with active_trace_id("a" * 32):
            assert otel_trace_ids() == ("a" * 32, None)
