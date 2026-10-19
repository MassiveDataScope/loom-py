"""Unit tests for ObservabilityRuntime — emit, span, error isolation, noop."""

from __future__ import annotations

from typing import cast

import pytest

from loom.core.config.observability import OtelConfig
from loom.core.environment import Environment
from loom.core.logger.config import LoggerConfig
from loom.core.observability.config import (
    LogObservabilityConfig,
    ObservabilityConfig,
    OtelObservabilityConfig,
    PrometheusConfig,
    PrometheusObservabilityConfig,
)
from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.runtime import ObservabilityRuntime


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


class _BrokenObserver:
    def on_event(self, event: LifecycleEvent) -> None:
        raise RuntimeError("observer exploded")


class TestEmit:
    def test_fans_out_to_all_observers(self) -> None:
        a, b = _RecordingObserver(), _RecordingObserver()
        runtime = ObservabilityRuntime([a, b])
        event = LifecycleEvent(scope=Scope.NODE, name="transform", kind=EventKind.START)

        runtime.emit(event)

        assert len(a.events) == 1
        assert len(b.events) == 1

    def test_broken_observer_does_not_stop_others(self) -> None:
        broken = _BrokenObserver()
        good = _RecordingObserver()
        runtime = ObservabilityRuntime([broken, good])
        event = LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.START)

        runtime.emit(event)

        assert len(good.events) == 1

    def test_empty_observers_is_safe(self) -> None:
        runtime = ObservabilityRuntime([])
        runtime.emit(LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.START))


class TestSpan:
    def test_emits_start_and_end_on_success(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span(Scope.USE_CASE, "CreateOrder"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        assert obs.events[0].kind is EventKind.START
        assert obs.events[1].kind is EventKind.END
        assert obs.events[1].status is LifecycleStatus.SUCCESS

    def test_emits_start_and_error_on_exception(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with pytest.raises(ValueError), runtime.span(Scope.USE_CASE, "CreateOrder"):
            raise ValueError("bad input")

        assert obs.events[0].kind is EventKind.START
        assert obs.events[1].kind is EventKind.ERROR
        assert obs.events[1].error == "bad input"

    def test_error_event_reraises_exception(self) -> None:
        runtime = ObservabilityRuntime([_RecordingObserver()])

        with pytest.raises(RuntimeError, match="boom"), runtime.span(Scope.NODE, "step"):
            raise RuntimeError("boom")

    def test_end_event_carries_duration(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span(Scope.JOB, "ingest"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        end = obs.events[1]
        assert end.duration_ms is not None
        assert end.duration_ms >= 0

    def test_propagates_trace_and_correlation_id(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span(Scope.NODE, "x", trace_id="t-1", correlation_id="c-1"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        for event in obs.events:
            assert event.trace_id == "t-1"
            assert event.correlation_id == "c-1"

    def test_meta_forwarded_to_both_events(self) -> None:
        obs = _RecordingObserver()
        runtime = ObservabilityRuntime([obs])

        with runtime.span(Scope.NODE, "x", flow="my_flow"):
            # Intentional no-op: the test only needs the context manager to close.
            pass

        for event in obs.events:
            assert event.meta == {"flow": "my_flow"}


class TestNoop:
    def test_noop_runtime_is_callable(self) -> None:
        runtime = ObservabilityRuntime.noop()

        with runtime.span(Scope.USE_CASE, "GetOrder"):
            # Intentional no-op: the noop runtime still needs to accept the scope.
            pass

    def test_noop_emit_is_safe(self) -> None:
        runtime = ObservabilityRuntime.noop()
        runtime.emit(LifecycleEvent(scope=Scope.NODE, name="x", kind=EventKind.END))


class TestFromConfig:
    def test_export_logs_requires_logger_config(self) -> None:
        config = ObservabilityConfig(
            log=LogObservabilityConfig(enabled=True, config=None),
            otel=OtelObservabilityConfig(
                enabled=True,
                export_logs=True,
                config=OtelConfig(endpoint="", service_name="loom"),
            ),
        )

        with pytest.raises(ValueError, match="export_logs requires observability.log.enabled"):
            ObservabilityRuntime.from_config(config)

    def test_export_logs_installs_otel_log_export_when_logger_config_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, object] = {}

        def _fake_configure_logging_from_values(**kwargs: object) -> None:
            captured.update(kwargs)

        def _fake_install_otel_log_export(_: OtelConfig) -> object:
            captured["otel_log_export_installed"] = True
            return object()

        monkeypatch.setattr(
            "loom.core.observability.runtime.configure_logging_from_values",
            _fake_configure_logging_from_values,
        )
        # Patched on the owning module: ``runtime`` imports the OTEL bootstrap
        # lazily so that core stays importable without the OpenTelemetry SDK.
        monkeypatch.setattr(
            "loom.core.observability.observer.otel.install_otel_log_export",
            _fake_install_otel_log_export,
        )

        config = ObservabilityConfig(
            log=LogObservabilityConfig(enabled=True, config=LoggerConfig()),
            otel=OtelObservabilityConfig(
                enabled=True,
                export_logs=True,
                config=OtelConfig(endpoint="", service_name="loom"),
            ),
        )

        runtime = ObservabilityRuntime.from_config(config)

        assert isinstance(runtime, ObservabilityRuntime)
        assert "extra_processors" in captured
        extra_processors = cast(tuple[object, ...], captured["extra_processors"])
        assert len(extra_processors) == 1
        assert captured["otel_log_export_installed"] is True

    @pytest.mark.parametrize(
        ("given", "expected"),
        [({}, Environment.TEST), ({"environment": Environment.STAGING}, Environment.STAGING)],
    )
    def test_the_logger_receives_the_environment_of_the_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
        given: dict[str, Environment],
        expected: Environment,
    ) -> None:
        captured: dict[str, object] = {}
        monkeypatch.setattr(
            "loom.core.observability.runtime.configure_logging_from_values",
            lambda **kwargs: captured.update(kwargs),
        )
        config = ObservabilityConfig(log=LogObservabilityConfig(config=LoggerConfig()))

        ObservabilityRuntime.from_config(config, **given)

        assert captured["environment"] is expected


class TestStartScrapeServer:
    def test_noop_when_port_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runtime = ObservabilityRuntime([], _scrape_port=None)
        called: list[int] = []
        monkeypatch.setattr(
            "loom.core.observability.runtime.start_http_server",
            lambda p, addr=None: called.append(p),
        )

        runtime.start_scrape_server()

        assert called == []

    def test_calls_start_http_server_with_port_and_addr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime = ObservabilityRuntime([], _scrape_port=9090, _scrape_addr="127.0.0.1")
        calls: list[tuple[int, str]] = []
        monkeypatch.setattr(
            "loom.core.observability.runtime.start_http_server",
            lambda p, addr=None: calls.append((p, addr)),
        )

        runtime.start_scrape_server()

        assert calls == [(9090, "127.0.0.1")]

    def test_is_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        runtime = ObservabilityRuntime([], _scrape_port=9090)
        called: list[int] = []
        monkeypatch.setattr(
            "loom.core.observability.runtime.start_http_server",
            lambda p, addr=None: called.append(p),
        )

        runtime.start_scrape_server()
        runtime.start_scrape_server()

        assert len(called) == 1

    def test_noop_runtime_start_scrape_server_is_safe(self) -> None:
        runtime = ObservabilityRuntime.noop()
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
            "loom.core.observability.runtime.start_http_server", lambda p, addr=None: None
        )

        runtime = ObservabilityRuntime.from_config(config)

        assert runtime._scrape_port == 8080
        assert runtime._scrape_addr == "0.0.0.0"

    def test_from_config_no_port_when_pushgateway_configured(self) -> None:
        config = ObservabilityConfig(
            prometheus=PrometheusObservabilityConfig(
                enabled=True,
                pushgateway_url="https://pushgateway:9091",
                config=PrometheusConfig(port=8080),
            )
        )

        runtime = ObservabilityRuntime.from_config(config)

        assert runtime._scrape_port is None

    def test_from_config_no_port_when_prometheus_disabled(self) -> None:
        config = ObservabilityConfig(
            prometheus=PrometheusObservabilityConfig(
                enabled=False,
                config=PrometheusConfig(port=8080),
            )
        )

        runtime = ObservabilityRuntime.from_config(config)

        assert runtime._scrape_port is None
