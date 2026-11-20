"""Unit tests for Prometheus instrument construction and caching."""

from __future__ import annotations

import gc
import weakref
from typing import Any

import pytest
from prometheus_client import REGISTRY, CollectorRegistry

from loom.core.engine.events import EventKind as UseCaseEventKind
from loom.core.engine.events import RuntimeEvent
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.prometheus import (
    KafkaPrometheusMetrics,
    PrometheusLifecycleAdapter,
    PrometheusMetricsAdapter,
    PrometheusMiddleware,
)
from loom.prometheus._instruments import cached_instruments, counter_spec, histogram_spec


async def _ok_app(scope: Any, receive: Any, send: Any) -> None:
    """Minimal ASGI app that answers every request with an empty 200."""
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b""})


async def _null_receive() -> dict[str, Any]:
    return {}


async def _discard_send(message: dict[str, Any]) -> None:
    return None


def _http_scope(path_template: str) -> dict[str, Any]:
    """Build an ASGI HTTP scope whose matched route is *path_template*."""

    class _Route:
        path = path_template

    return {
        "type": "http",
        "method": "GET",
        "path": path_template,
        "headers": [],
        "route": _Route(),
    }


def _instrument_contract(registry: CollectorRegistry) -> set[tuple[str, str, str, tuple[str, ...]]]:
    """Return ``(name, documentation, type, labelnames)`` for every instrument in *registry*."""
    labelnames: dict[str, tuple[str, ...]] = {
        # ``_labelnames`` is the only place prometheus_client keeps the declared
        # label set of an instrument that has not recorded any sample yet.
        str(getattr(collector, "_name", "")): tuple(getattr(collector, "_labelnames", ()))
        for collector in registry._collector_to_names
    }
    return {
        (metric.name, metric.documentation, metric.type, labelnames.get(metric.name, ()))
        for metric in registry.collect()
    }


class TestMetricContract:
    def test_use_case_adapter_declares_its_instruments(self) -> None:
        registry = CollectorRegistry()
        PrometheusMetricsAdapter(registry=registry)
        assert _instrument_contract(registry) == {
            (
                "loom_usecase_requests",
                "Total number of use-case executions by outcome.",
                "counter",
                ("usecase", "status"),
            ),
            (
                "loom_usecase_duration_seconds",
                "Use-case execution wall-clock time in seconds.",
                "histogram",
                ("usecase",),
            ),
            (
                "loom_usecase_errors",
                "Total number of use-case execution errors by error kind.",
                "counter",
                ("usecase", "error_kind"),
            ),
            (
                "loom_job_events",
                "Total number of job events by kind, job and abandonment reason.",
                "counter",
                ("kind", "job", "reason"),
            ),
        }

    def test_lifecycle_adapter_declares_its_instruments(self) -> None:
        registry = CollectorRegistry()
        PrometheusLifecycleAdapter(registry=registry)
        assert _instrument_contract(registry) == {
            (
                "lifecycle_duration_seconds",
                "Lifecycle span wall-clock duration in seconds.",
                "histogram",
                ("scope", "name"),
            ),
            (
                "lifecycle_errors",
                "Total lifecycle span errors by scope and name.",
                "counter",
                ("scope", "name"),
            ),
        }

    def test_kafka_recorder_declares_its_instruments(self) -> None:
        registry = CollectorRegistry()
        KafkaPrometheusMetrics(registry=registry)
        assert _instrument_contract(registry) == {
            (
                "streaming_kafka_produced",
                "Total Kafka records produced.",
                "counter",
                ("topic", "status"),
            ),
            (
                "streaming_kafka_consumed",
                "Total Kafka records consumed.",
                "counter",
                ("topic", "status"),
            ),
            (
                "streaming_kafka_encode_duration_seconds",
                "Kafka envelope encode duration in seconds.",
                "histogram",
                ("content_type",),
            ),
            (
                "streaming_kafka_decode_duration_seconds",
                "Kafka envelope decode duration in seconds.",
                "histogram",
                ("content_type",),
            ),
        }

    def test_http_middleware_declares_its_instruments(self) -> None:
        registry = CollectorRegistry()
        PrometheusMiddleware(_ok_app, registry=registry)
        assert _instrument_contract(registry) == {
            (
                "http_requests",
                "Total HTTP requests by method, path template, and status code.",
                "counter",
                ("method", "path_template", "status_code"),
            ),
            (
                "http_request_duration_seconds",
                "HTTP request duration in seconds by method and path template.",
                "histogram",
                ("method", "path_template"),
            ),
        }


class TestDefaultRegistryCache:
    def test_two_use_case_adapters_share_default_instruments(self) -> None:
        use_case = "UseCaseCacheProbe"
        for adapter in (PrometheusMetricsAdapter(), PrometheusMetricsAdapter()):
            adapter.on_event(
                RuntimeEvent(
                    kind=UseCaseEventKind.EXEC_DONE,
                    use_case_name=use_case,
                    status="success",
                )
            )
        assert REGISTRY.get_sample_value(
            "loom_usecase_requests_total",
            {"usecase": use_case, "status": "success"},
        ) == pytest.approx(2.0)

    def test_the_default_registry_passed_explicitly_shares_its_instruments(self) -> None:
        use_case = "UseCaseAliasedRegistryProbe"
        for adapter in (PrometheusMetricsAdapter(), PrometheusMetricsAdapter(registry=REGISTRY)):
            adapter.on_event(
                RuntimeEvent(
                    kind=UseCaseEventKind.EXEC_DONE,
                    use_case_name=use_case,
                    status="success",
                )
            )
        assert REGISTRY.get_sample_value(
            "loom_usecase_requests_total",
            {"usecase": use_case, "status": "success"},
        ) == pytest.approx(2.0)

    def test_two_lifecycle_adapters_share_default_instruments(self) -> None:
        span = "LifecycleCacheProbe"
        for adapter in (PrometheusLifecycleAdapter(), PrometheusLifecycleAdapter()):
            adapter.on_event(LifecycleEvent(scope=Scope.USE_CASE, name=span, kind=EventKind.ERROR))
        assert REGISTRY.get_sample_value(
            "lifecycle_errors_total",
            {"scope": "use_case", "name": span},
        ) == pytest.approx(2.0)

    def test_two_kafka_recorders_share_default_instruments(self) -> None:
        topic = "kafka-cache-probe"
        for metrics in (KafkaPrometheusMetrics(), KafkaPrometheusMetrics()):
            metrics.on_event(
                LifecycleEvent(
                    scope=Scope.TRANSPORT,
                    name="kafka_produce",
                    kind=EventKind.END,
                    meta={"topic": topic},
                )
            )
        assert REGISTRY.get_sample_value(
            "streaming_kafka_produced_total",
            {"topic": topic, "status": "success"},
        ) == pytest.approx(2.0)

    def test_custom_registries_get_independent_instruments(self) -> None:
        first_registry = CollectorRegistry()
        second_registry = CollectorRegistry()
        labels = {"topic": "kafka-isolation-probe", "status": "success"}
        event = LifecycleEvent(
            scope=Scope.TRANSPORT,
            name="kafka_produce",
            kind=EventKind.END,
            meta={"topic": labels["topic"]},
        )

        KafkaPrometheusMetrics(registry=first_registry).on_event(event)
        KafkaPrometheusMetrics(registry=second_registry)

        assert first_registry.get_sample_value(
            "streaming_kafka_produced_total", labels
        ) == pytest.approx(1.0)
        assert second_registry.get_sample_value("streaming_kafka_produced_total", labels) is None

    @pytest.mark.asyncio
    async def test_two_http_middlewares_share_default_instruments(self) -> None:
        path_template = "/http-cache-probe"
        for middleware in (PrometheusMiddleware(_ok_app), PrometheusMiddleware(_ok_app)):
            await middleware(_http_scope(path_template), _null_receive, _discard_send)
        assert REGISTRY.get_sample_value(
            "http_requests_total",
            {"method": "GET", "path_template": path_template, "status_code": "200"},
        ) == pytest.approx(2.0)


class TestCustomRegistryCache:
    def test_two_kafka_recorders_share_instruments_on_one_custom_registry(self) -> None:
        registry = CollectorRegistry()
        labels = {"topic": "kafka-custom-cache-probe", "status": "success"}
        event = LifecycleEvent(
            scope=Scope.TRANSPORT,
            name="kafka_produce",
            kind=EventKind.END,
            meta={"topic": labels["topic"]},
        )

        for metrics in (
            KafkaPrometheusMetrics(registry=registry),
            KafkaPrometheusMetrics(registry=registry),
        ):
            metrics.on_event(event)

        assert registry.get_sample_value("streaming_kafka_produced_total", labels) == pytest.approx(
            2.0
        )

    def test_specs_sharing_a_name_resolve_to_one_instrument(self) -> None:
        registry = CollectorRegistry()
        first = counter_spec("loom_shared_name_probe", "First documentation.", "label")
        second = counter_spec("loom_shared_name_probe", "Second documentation.", "label")

        assert (
            cached_instruments(registry, (first,))[first]
            is cached_instruments(registry, (second,))[second]
        )

    def test_a_name_reused_for_another_kind_is_rejected(self) -> None:
        registry = CollectorRegistry()
        counter = counter_spec("loom_kind_clash_probe", "Probe.")
        histogram = histogram_spec("loom_kind_clash_probe", "Probe.")
        cached_instruments(registry, (counter,))

        with pytest.raises(ValueError, match="already registered"):
            cached_instruments(registry, (histogram,))

    def test_a_name_reused_with_other_labels_is_rejected(self) -> None:
        registry = CollectorRegistry()
        first = counter_spec("loom_label_clash_probe", "Probe.", "topic")
        second = counter_spec("loom_label_clash_probe", "Probe.", "status")
        cached_instruments(registry, (first,))

        with pytest.raises(ValueError, match="already registered"):
            cached_instruments(registry, (second,))

    def test_a_collected_registry_releases_its_cached_instruments(self) -> None:
        registry = CollectorRegistry()
        cached_instruments(registry, (counter_spec("loom_collected_registry_probe", "Probe."),))
        registry_ref = weakref.ref(registry)

        del registry
        gc.collect()

        assert registry_ref() is None
