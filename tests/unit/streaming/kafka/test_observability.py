from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observability import Observability
from loom.prometheus import KafkaPrometheusMetrics

pytestmark = pytest.mark.kafka


def _make_obs(registry: CollectorRegistry) -> tuple[Observability, KafkaPrometheusMetrics]:
    metrics = KafkaPrometheusMetrics(registry=registry)
    return Observability([metrics]), metrics


class TestKafkaPrometheusMetrics:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            pytest.param(
                "kafka_produce",
                'streaming_kafka_produced_total{status="success",topic="orders"}',
                id="produce_end_increments_produced_total",
            ),
            pytest.param(
                "kafka_consume",
                'streaming_kafka_consumed_total{status="success",topic="orders"}',
                id="consume_end_increments_consumed_total",
            ),
        ],
    )
    def test_end_increments_its_total(
        self, name: str, expected: str, kafka_registry: CollectorRegistry
    ) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(LifecycleEvent.end(scope=Scope.TRANSPORT, name=name, meta={"topic": "orders"}))

        text = generate_latest(kafka_registry).decode()
        assert expected in text

    @pytest.mark.parametrize(
        ("name", "error", "expected"),
        [
            pytest.param(
                "kafka_produce",
                "timeout",
                'streaming_kafka_produced_total{status="delivery_error",topic="orders"}',
                id="produce_error_increments_produced_total_with_delivery_error",
            ),
            pytest.param(
                "kafka_decode",
                "bad bytes",
                'streaming_kafka_consumed_total{status="decode_error",topic="orders"}',
                id="decode_error_increments_consumed_total_with_decode_error",
            ),
        ],
    )
    def test_error_increments_its_total_with_the_error_status(
        self, name: str, error: str, expected: str, kafka_registry: CollectorRegistry
    ) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.exception(
                scope=Scope.TRANSPORT,
                name=name,
                error=error,
                meta={"topic": "orders"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert expected in text

    @pytest.mark.parametrize(
        ("name", "duration_ms", "expected"),
        [
            pytest.param(
                "kafka_encode",
                10.0,
                "streaming_kafka_encode_duration_seconds",
                id="encode_end_observes_encode_duration",
            ),
            pytest.param(
                "kafka_decode",
                20.0,
                "streaming_kafka_decode_duration_seconds",
                id="decode_end_observes_decode_duration",
            ),
        ],
    )
    def test_end_observes_its_duration(
        self, name: str, duration_ms: float, expected: str, kafka_registry: CollectorRegistry
    ) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name=name,
                duration_ms=duration_ms,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert expected in text

    def test_non_transport_events_are_ignored(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(LifecycleEvent.end(scope=Scope.NODE, name="transform", duration_ms=5.0))

        text = generate_latest(kafka_registry).decode()
        assert 'topic="' not in text
        assert 'content_type="' not in text

    def test_unknown_transport_name_is_ignored(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(LifecycleEvent.end(scope=Scope.TRANSPORT, name="unknown_op", meta={"topic": "x"}))

        text = generate_latest(kafka_registry).decode()
        assert 'topic="x"' not in text

    def test_all_four_instruments_in_one_run(self, kafka_registry: CollectorRegistry) -> None:
        obs, _ = _make_obs(kafka_registry)

        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT, name="kafka_produce", meta={"topic": "orders"}
            )
        )
        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT, name="kafka_consume", meta={"topic": "orders"}
            )
        )
        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name="kafka_encode",
                duration_ms=1.0,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )
        obs.emit(
            LifecycleEvent.end(
                scope=Scope.TRANSPORT,
                name="kafka_decode",
                duration_ms=2.0,
                meta={"content_type": "application/x-loom-msgpack"},
            )
        )

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_produced_total" in text
        assert "streaming_kafka_consumed_total" in text
        assert "streaming_kafka_encode_duration_seconds" in text
        assert "streaming_kafka_decode_duration_seconds" in text
