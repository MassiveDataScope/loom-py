"""Unit tests for Observability — emit, span, error isolation, noop."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.config.observability import OtelConfig
from loom.core.environment import Environment
from loom.core.errors.summary import ErrorSummary
from loom.core.logger import LogRuntime
from loom.core.logger.install import EXPORT_HANDLER_NAME, HANDLER_NAME
from loom.core.logger.settings import LogFormatName
from loom.core.model.redaction import Dsn
from loom.core.observability.config import (
    LogObservabilityConfig,
    ObservabilityConfig,
    OtelObservabilityConfig,
    PrometheusConfig,
    PrometheusObservabilityConfig,
)
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observability import Observability
from loom.core.tracing.context import active_trace_id
from loom.testing import make
from tests.unit.core.observability.conftest import LogExport


class AcmeRuntimeRecordingObserver:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class _BrokenObserver:
    def on_event(self, event: LifecycleEvent) -> None:
        raise RuntimeError("observer exploded")


class TestEmit:
    def test_fans_out_to_all_observers(self) -> None:
        a, b = AcmeRuntimeRecordingObserver(), AcmeRuntimeRecordingObserver()
        runtime = Observability([a, b])
        event = make(LifecycleEvent, scope=Scope.NODE, kind=EventKind.START)

        runtime.emit(event)

        assert len(a.events) == 1
        assert len(b.events) == 1

    def test_broken_observer_does_not_stop_others(self) -> None:
        broken = _BrokenObserver()
        good = AcmeRuntimeRecordingObserver()
        runtime = Observability([broken, good])
        event = make(LifecycleEvent, scope=Scope.NODE, kind=EventKind.START)

        runtime.emit(event)

        assert len(good.events) == 1

    def test_empty_observers_is_safe(self) -> None:
        runtime = Observability([])
        runtime.emit(make(LifecycleEvent, scope=Scope.NODE, kind=EventKind.START))


class TestSpan:
    def test_emits_start_and_end_on_success(self) -> None:
        obs = AcmeRuntimeRecordingObserver()
        runtime = Observability([obs])

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        assert obs.events[0].kind is EventKind.START
        assert obs.events[1].kind is EventKind.END
        assert obs.events[1].status is LifecycleStatus.SUCCESS

    def test_emits_start_and_error_on_exception(self) -> None:
        obs = AcmeRuntimeRecordingObserver()
        runtime = Observability([obs])

        with pytest.raises(ValueError), runtime.span(Scope.USE_CASE, "CreateOrder"):
            raise ValueError("bad input")

        assert obs.events[0].kind is EventKind.START
        assert obs.events[1].kind is EventKind.ERROR
        assert obs.events[1].failure == ErrorSummary(type="ValueError")

    def test_error_event_reraises_exception(self) -> None:
        runtime = Observability([AcmeRuntimeRecordingObserver()])

        with pytest.raises(RuntimeError, match="boom"), runtime.span(Scope.NODE, "step"):
            raise RuntimeError("boom")

    def test_end_event_carries_duration(self) -> None:
        obs = AcmeRuntimeRecordingObserver()
        runtime = Observability([obs])

        with runtime.span(Scope.JOB, "ingest"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        end = obs.events[1]
        assert end.duration_ms is not None
        assert end.duration_ms >= 0

    def test_propagates_trace_and_correlation_id(self) -> None:
        obs = AcmeRuntimeRecordingObserver()
        runtime = Observability([obs])

        with runtime.span(Scope.NODE, "x", trace_id="t-1", correlation_id="c-1"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        for event in obs.events:
            assert event.trace_id == "t-1"
            assert event.correlation_id == "c-1"

    def test_meta_forwarded_to_both_events(self) -> None:
        obs = AcmeRuntimeRecordingObserver()
        runtime = Observability([obs])

        with runtime.span(Scope.NODE, "x", flow="my_flow"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        for event in obs.events:
            assert event.meta == {"flow": "my_flow"}


class TestNoop:
    def test_noop_runtime_is_callable(self) -> None:
        runtime = Observability.noop()

        with runtime.span(Scope.USE_CASE, "GetOrder"):
            # Intentional no-op: the noop runtime still needs to accept the scope.
            pass

    def test_noop_emit_is_safe(self) -> None:
        runtime = Observability.noop()
        runtime.emit(make(LifecycleEvent, scope=Scope.NODE, kind=EventKind.END))


_OTEL = OtelObservabilityConfig(enabled=True, export_logs=True, config=OtelConfig(endpoint=None))


def _loom_handler() -> logging.Handler:
    (handler,) = [h for h in logging.getLogger().handlers if h.get_name() == HANDLER_NAME]
    return handler


class TestFromConfig:
    def test_export_logs_requires_the_log_backend(self) -> None:
        config = ObservabilityConfig(log=LogObservabilityConfig(enabled=False), otel=_OTEL)

        with pytest.raises(ConfigError, match="export_logs requires observability.log.enabled"):
            Observability.from_config(config)

    def test_the_runtime_reaches_the_export_and_the_stdout_line_alike(
        self, configured_log_export: Callable[[], LogExport], capsys: pytest.CaptureFixture[str]
    ) -> None:
        config = ObservabilityConfig(
            log=LogObservabilityConfig(format=LogFormatName.JSON), otel=_OTEL
        )
        observability = Observability.from_config(config)

        observability.running_as(LogRuntime.CELERY)
        logging.getLogger("tests.export").warning("billing.check_done")

        exported = configured_log_export().exported()["billing.check_done"]
        assert exported["loom.runtime"] == "celery"
        assert json.loads(capsys.readouterr().out.splitlines()[-1])["loom.runtime"] == "celery"

    def test_configuring_twice_replaces_the_export_and_its_resource_is_the_service(
        self, configured_log_export: Callable[[], LogExport]
    ) -> None:
        config = ObservabilityConfig(
            log=LogObservabilityConfig(format=LogFormatName.JSON), otel=_OTEL
        )

        for _ in range(2):
            Observability.from_config(
                config, environment=Environment.STAGING, service_name="billing"
            )
        with active_trace_id("trace-42"):
            logging.getLogger("tests.export").warning("billing.check_done")

        export = configured_log_export()
        export.handler.flush()
        [finished] = [
            item
            for item in export.exporter.get_finished_logs()
            if item.log_record.body == "billing.check_done"
        ]
        assert finished.resource.attributes["service.name"] == "billing"
        assert finished.resource.attributes["deployment.environment.name"] == "staging"
        assert finished.log_record.attributes["trace_id"] == "trace-42"
        names = [h.get_name() for h in logging.getLogger().handlers]
        assert (names.count(EXPORT_HANDLER_NAME), names.count(HANDLER_NAME)) == (1, 1)

    def test_the_log_format_is_installed_with_the_service_identity(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config = ObservabilityConfig(log=LogObservabilityConfig(format=LogFormatName.JSON))

        Observability.from_config(
            config, environment=Environment.STAGING, service_name="billing", service_version="2.4"
        )
        logging.getLogger("tests.observability").warning("billing.check_done")

        entry = json.loads(capsys.readouterr().out)
        assert entry["service.name"] == "billing"
        assert entry["service.version"] == "2.4"
        assert entry["deployment.environment.name"] == "staging"

    def test_the_runtime_is_stamped_once_the_composition_names_it(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config = ObservabilityConfig(log=LogObservabilityConfig(format=LogFormatName.JSON))
        observability = Observability.from_config(config)

        observability.running_as(LogRuntime.CELERY)
        logging.getLogger("tests.observability").warning("billing.check_done")

        assert json.loads(capsys.readouterr().out)["loom.runtime"] == "celery"

    def test_a_disabled_log_backend_installs_no_handler(self) -> None:
        before = list(logging.getLogger().handlers)

        Observability.from_config(ObservabilityConfig(log=LogObservabilityConfig(enabled=False)))

        assert logging.getLogger().handlers == before

    def test_an_unknown_format_is_refused_when_the_configuration_is_read(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="format"):
            msgspec.convert({"format": "xml"}, LogObservabilityConfig)


class TestStartScrapeServer:
    def test_noop_when_port_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runtime = Observability([], _scrape_port=None)
        called: list[int] = []
        monkeypatch.setattr(
            "loom.core.observability.observability.start_http_server",
            lambda p, addr=None: called.append(p),
        )

        runtime.start_scrape_server()

        assert called == []

    def test_calls_start_http_server_with_port_and_addr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = Observability([], _scrape_port=9090, _scrape_addr="127.0.0.1")
        calls: list[tuple[int, str]] = []
        monkeypatch.setattr(
            "loom.core.observability.observability.start_http_server",
            lambda p, addr=None: calls.append((p, addr)),
        )

        runtime.start_scrape_server()

        assert calls == [(9090, "127.0.0.1")]

    def test_is_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runtime = Observability([], _scrape_port=9090)
        called: list[int] = []
        monkeypatch.setattr(
            "loom.core.observability.observability.start_http_server",
            lambda p, addr=None: called.append(p),
        )

        runtime.start_scrape_server()
        runtime.start_scrape_server()

        assert len(called) == 1

    def test_noop_runtime_start_scrape_server_is_safe(self) -> None:
        runtime = Observability.noop()
        runtime.start_scrape_server()  # must not raise

    def test_from_config_resolves_scrape_port_and_addr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config = ObservabilityConfig(
            prometheus=PrometheusObservabilityConfig(
                enabled=True,
                config=PrometheusConfig(port=8080, bind_address="0.0.0.0"),
            )
        )
        monkeypatch.setattr(
            "loom.core.observability.observability.start_http_server", lambda p, addr=None: None
        )

        runtime = Observability.from_config(config)

        assert runtime._scrape_port == 8080
        assert runtime._scrape_addr == "0.0.0.0"

    def test_from_config_no_port_when_pushgateway_configured(self) -> None:
        config = ObservabilityConfig(
            prometheus=PrometheusObservabilityConfig(
                enabled=True,
                pushgateway_url=Dsn("https://pushgateway:9091"),
                config=PrometheusConfig(port=8080),
            )
        )

        runtime = Observability.from_config(config)

        assert runtime._scrape_port is None

    def test_from_config_no_port_when_prometheus_disabled(self) -> None:
        config = ObservabilityConfig(
            prometheus=PrometheusObservabilityConfig(
                enabled=False,
                config=PrometheusConfig(port=8080),
            )
        )

        runtime = Observability.from_config(config)

        assert runtime._scrape_port is None
