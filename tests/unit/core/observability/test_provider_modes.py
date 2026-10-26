"""The four provider modes: where spans go and what trace ids they get.

Exporting and id generation are orthogonal. ``endpoint`` chooses the first,
``adopt_host_id_generator`` the second, and nothing in OTEL couples them. Each
combination is pinned on exported span structure, because the failure mode of
getting one wrong is silent: correct-looking traces carrying random ids.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import ProxyTracerProvider

from loom.core.config.observability import OtelConfig
from loom.core.model.redaction import Dsn
from loom.core.observability.event import Scope
from loom.core.observability.observer import otel as _otel
from loom.core.observability.otel_ids import LoomMessageIdGenerator
from loom.core.observability.runtime import ObservabilityRuntime

_TRACE_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
_ADOPT_SETTING = "observability.otel.config.adopt_host_id_generator"


def _host_provider(monkeypatch: pytest.MonkeyPatch) -> InMemorySpanExporter:
    """Install a host-owned SDK provider, as logfire or an agent would."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", provider, raising=False)
    return exporter


def _hex_trace(span: ReadableSpan) -> str:
    context = span.get_span_context()
    assert context is not None
    return format(context.trace_id, "032x")


def _emit(tracer: Any, trace_id: str | None) -> None:
    with ObservabilityRuntime([], tracer=tracer).span(Scope.NODE, "n", trace_id=trace_id):
        pass


class TestLoomOwnedProvider:
    def test_an_endpoint_builds_a_provider_whose_roots_take_the_message_trace(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        exporter = InMemorySpanExporter()
        monkeypatch.setattr(_otel, "_build_exporter", lambda _config: exporter)

        tracer, flusher = _otel.build_tracer(
            OtelConfig(endpoint=Dsn("http://collector:4318/v1/traces"))
        )
        _emit(tracer, _TRACE_A)
        assert flusher is not None
        flusher.force_flush()

        spans = exporter.get_finished_spans()
        assert [span.name for span in spans] == ["node:n"]
        assert _hex_trace(spans[0]) == _TRACE_A
        assert spans[0].parent is None

    def test_the_sampler_from_config_reaches_the_owned_provider(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        exporter = InMemorySpanExporter()
        monkeypatch.setattr(_otel, "_build_exporter", lambda _config: exporter)

        tracer, flusher = _otel.build_tracer(
            OtelConfig(
                endpoint=Dsn("http://collector:4318/v1/traces"),
                sampler="always_off",
                sampler_ratio=0.0,
            )
        )
        _emit(tracer, _TRACE_A)
        assert flusher is not None
        flusher.force_flush()

        assert exporter.get_finished_spans() == ()


class TestHostProviderWithoutAdoption:
    def test_spans_still_export_but_with_random_ids_and_exactly_one_startup_log(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        exporter = _host_provider(monkeypatch)

        with caplog.at_level(logging.INFO, logger=_otel.__name__):
            tracer, flusher = _otel.build_tracer(OtelConfig(endpoint=None))
        _emit(tracer, _TRACE_A)

        assert flusher is None, "Loom must not offer to flush a provider it does not own"
        spans = exporter.get_finished_spans()
        assert [span.name for span in spans] == ["node:n"]
        assert _hex_trace(spans[0]) != _TRACE_A

        records = [r for r in caplog.records if r.message == "otel_host_provider_random_trace_ids"]
        assert len(records) == 1
        assert records[0].setting == _ADOPT_SETTING  # type: ignore[attr-defined]


class TestHostProviderWithAdoption:
    def test_loom_spans_take_the_message_trace_while_host_spans_stay_independent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        exporter = _host_provider(monkeypatch)
        host_tracer = trace.get_tracer_provider().get_tracer("host")
        host_tracer.start_span("host.before").end()

        tracer, _ = _otel.build_tracer(OtelConfig(endpoint=None, adopt_host_id_generator=True))
        _emit(tracer, _TRACE_A)
        host_tracer.start_span("host.after").end()
        trace.get_tracer_provider().get_tracer("host2").start_span("host.fresh").end()

        by_name = {span.name: span for span in exporter.get_finished_spans()}
        assert _hex_trace(by_name["node:n"]) == _TRACE_A
        host_traces = [
            _hex_trace(by_name[name]) for name in ("host.before", "host.after", "host.fresh")
        ]
        assert _TRACE_A not in host_traces, "adoption leaked Loom's trace into host spans"
        assert len(set(host_traces)) == 3, "host spans stopped getting independent random ids"

    def test_adoption_is_installed_on_the_host_provider_itself(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _host_provider(monkeypatch)

        _otel.build_tracer(OtelConfig(endpoint=None, adopt_host_id_generator=True))

        provider = trace.get_tracer_provider()
        assert isinstance(provider, TracerProvider)
        assert isinstance(provider.id_generator, LoomMessageIdGenerator)


class TestAdoptionOnAProxyProvider:
    def test_it_is_a_no_op_with_exactly_one_warning_and_no_exception(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        proxy = ProxyTracerProvider()
        monkeypatch.setattr(trace, "_TRACER_PROVIDER", None, raising=False)
        monkeypatch.setattr(trace, "get_tracer_provider", lambda: proxy)

        with caplog.at_level(logging.WARNING, logger="loom.core.observability.otel_ids"):
            tracer, flusher = _otel.build_tracer(
                OtelConfig(endpoint=None, adopt_host_id_generator=True)
            )

        assert flusher is None
        assert tracer is not None
        assert not hasattr(proxy, "id_generator"), (
            "blind assignment creates a field nobody reads and hides the failure"
        )
        records = [
            r for r in caplog.records if r.message == "otel_adopt_host_id_generator_unavailable"
        ]
        assert len(records) == 1
        assert records[0].setting == _ADOPT_SETTING  # type: ignore[attr-defined]
        assert records[0].provider == "ProxyTracerProvider"  # type: ignore[attr-defined]
        assert "random trace ids" in records[0].consequence  # type: ignore[attr-defined]


class TestConfigValidation:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"sampler": "sometimes"}, "sampler must be one of"),
            ({"sampler_ratio": 1.5}, "sampler_ratio must be within"),
            ({"max_span_links": 0}, "max_span_links must be >= 1"),
        ],
    )
    def test_an_unusable_setting_is_rejected_rather_than_silently_ignored(
        self, kwargs: dict[str, Any], message: str
    ) -> None:
        config = OtelConfig(**kwargs)

        with pytest.raises(ValueError, match=message):
            config.validate()
