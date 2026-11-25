"""Unit tests for built-in lifecycle observers."""

from __future__ import annotations

import logging

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import format_span_id, format_trace_id

from loom.core.errors.summary import ErrorSummary
from loom.core.log_names import LogField
from loom.core.observability.diagnostic import ObservabilityEvent
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observer.log import LifecycleKey, LogLifecycleObserver
from loom.core.observability.observer.noop import NoopObserver
from loom.testing import make
from tests.helpers.loggers import CapturedLog, CapturingHandler
from tests.unit.core.observability.conftest import LogExport

_STARTED = ObservabilityEvent.LIFECYCLE_STARTED
_ENDED = ObservabilityEvent.LIFECYCLE_ENDED
_FAILED = ObservabilityEvent.LIFECYCLE_FAILED


class TestNoopObserver:
    def test_accepts_any_event_without_error(self) -> None:
        obs = NoopObserver()
        for kind in EventKind:
            obs.on_event(make(LifecycleEvent, scope=Scope.NODE, kind=kind))


class TestLifecycleEventHelpers:
    def test_start_end_and_error_builders_set_expected_kinds(self) -> None:
        start = LifecycleEvent.start(scope=Scope.NODE, name="step")
        end = LifecycleEvent.end(scope=Scope.NODE, name="step")
        error = LifecycleEvent.exception(scope=Scope.NODE, name="step", error=ValueError())

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


@pytest.fixture
def lifecycle_log(caplog: pytest.LogCaptureFixture) -> CapturedLog:
    caplog.set_level(logging.DEBUG, logger="loom.core.observability.observer.log")
    return CapturedLog(caplog)


def _levels(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.levelname for record in caplog.records]


def _event(**fields: object) -> LifecycleEvent:
    """A lifecycle event as loom builds one: meta and failure only when given."""
    return make(LifecycleEvent, **{"meta": {}, "failure": None, **fields})


class TestLogLifecycleObserver:
    @pytest.mark.parametrize(
        ("scope", "kind", "level", "name"),
        [
            pytest.param(Scope.NODE, EventKind.START, "DEBUG", _STARTED, id="node-start"),
            pytest.param(Scope.PIPELINE, EventKind.START, "INFO", _STARTED, id="pipeline-start"),
            pytest.param(Scope.USE_CASE, EventKind.END, "INFO", _ENDED, id="end"),
            pytest.param(Scope.NODE, EventKind.ERROR, "ERROR", _FAILED, id="error"),
            pytest.param(Scope.TRANSPORT, EventKind.END, "DEBUG", _ENDED, id="transport-end"),
            pytest.param(
                Scope.TRANSPORT,
                EventKind.ERROR,
                "WARNING",
                _FAILED,
                id="transport-error",
            ),
        ],
    )
    def test_each_kind_and_scope_logs_one_record_at_its_level(
        self,
        scope: Scope,
        kind: EventKind,
        level: str,
        name: ObservabilityEvent,
        caplog: pytest.LogCaptureFixture,
        lifecycle_log: CapturedLog,
    ) -> None:
        LogLifecycleObserver().on_event(_event(scope=scope, kind=kind))

        assert (_levels(caplog), lifecycle_log.messages) == ([level], [name])

    def test_a_record_carries_the_scope_the_name_and_the_trace(
        self, lifecycle_log: CapturedLog
    ) -> None:
        LogLifecycleObserver().on_event(
            _event(
                scope=Scope.USE_CASE,
                name="CreateOrder",
                kind=EventKind.END,
                duration_ms=12.5,
                status=LifecycleStatus.SUCCESS,
                trace_id="trace-1",
            )
        )

        [fields] = lifecycle_log.named(ObservabilityEvent.LIFECYCLE_ENDED)
        assert (fields["loom.scope"], fields["loom.name"], fields["trace_id"]) == (
            "use_case",
            "CreateOrder",
            "trace-1",
        )
        assert (fields["loom.duration_ms"], fields["loom.status"]) == (12.5, "success")

    def test_inside_a_span_the_record_carries_the_span_s_ids(
        self, lifecycle_log: CapturedLog, rendering: CapturingHandler
    ) -> None:
        tracer = TracerProvider().get_tracer("tests.observers")

        with tracer.start_as_current_span("request") as span:
            LogLifecycleObserver().on_event(_event(kind=EventKind.START, trace_id="request-7"))

        context = span.get_span_context()
        [fields] = rendering.named(ObservabilityEvent.LIFECYCLE_STARTED)
        assert fields["trace_id"] == format_trace_id(context.trace_id)
        assert fields["span_id"] == format_span_id(context.span_id)

    @pytest.mark.parametrize(
        ("event_id", "written"),
        [pytest.param("op-1", True, id="given"), pytest.param(None, False, id="absent")],
    )
    def test_the_event_id_is_written_only_when_the_event_has_one(
        self, lifecycle_log: CapturedLog, event_id: str | None, written: bool
    ) -> None:
        LogLifecycleObserver().on_event(_event(kind=EventKind.END, id=event_id))

        [fields] = lifecycle_log.named(_ENDED)
        assert (LifecycleKey.EVENT_ID in fields) is written

    def test_a_failure_is_written_as_its_summary(self, lifecycle_log: CapturedLog) -> None:
        summary = make(ErrorSummary, code="acme.refused")

        LogLifecycleObserver().on_event(_event(kind=EventKind.ERROR, failure=summary))

        [fields] = lifecycle_log.named(_FAILED)
        assert (fields[LogField.ERROR_TYPE], fields[LogField.ERROR_CODE]) == (
            summary.type,
            summary.code,
        )

    def test_meta_is_forwarded_under_its_own_prefix(self, lifecycle_log: CapturedLog) -> None:
        LogLifecycleObserver().on_event(
            _event(
                scope=Scope.NODE,
                kind=EventKind.START,
                meta={"flow": "my_flow", "node_index": 0},
            )
        )

        [fields] = lifecycle_log.named(ObservabilityEvent.LIFECYCLE_STARTED)
        assert fields["loom.lifecycle.meta.flow"] == "my_flow"
        assert fields["loom.lifecycle.meta.node_index"] == 0

    def test_a_meta_key_that_is_a_record_attribute_does_not_break_logging(
        self, lifecycle_log: CapturedLog
    ) -> None:
        LogLifecycleObserver().on_event(
            _event(
                scope=Scope.FLOW,
                name="ingest",
                kind=EventKind.END,
                meta={"name": "orders"},
            )
        )

        [fields] = lifecycle_log.named(ObservabilityEvent.LIFECYCLE_ENDED)
        assert fields["loom.lifecycle.meta.name"] == "orders"


class TestPrometheusLifecycleObserver:
    pytest.importorskip("prometheus_client")

    def test_records_duration_on_end_event(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleObserver

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleObserver(registry=registry)

        adapter.on_event(
            make(
                LifecycleEvent,
                scope=Scope.USE_CASE,
                name="CreateOrder",
                kind=EventKind.END,
                duration_ms=42.0,
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

        adapter.on_event(
            make(LifecycleEvent, scope=Scope.NODE, name="transform", kind=EventKind.ERROR)
        )

        assert registry.get_sample_value(
            "lifecycle_errors_total",
            {"scope": "node", "name": "transform"},
        ) == pytest.approx(1.0)

    def test_start_event_is_ignored(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleObserver

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleObserver(registry=registry)

        adapter.on_event(make(LifecycleEvent, scope=Scope.NODE, kind=EventKind.START))
