"""Prometheus metrics adapter for Kafka transport operations.

Optional module — requires ``prometheus-client``::

    pip install "loom-py[prometheus]"

Usage::

    from prometheus_client import CollectorRegistry
    from loom.prometheus import KafkaPrometheusMetrics
    from loom.core.observability.observability import ObservabilityRuntime

    registry = CollectorRegistry()
    metrics = KafkaPrometheusMetrics(registry=registry)
    obs = ObservabilityRuntime([metrics])
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.prometheus._instruments import cached_instruments, counter_spec, histogram_spec

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class KafkaMetricName(StrEnum):
    """Prometheus metric names for Kafka transport instruments."""

    PRODUCED_TOTAL = "streaming_kafka_produced_total"
    CONSUMED_TOTAL = "streaming_kafka_consumed_total"
    ENCODE_DURATION = "streaming_kafka_encode_duration_seconds"
    DECODE_DURATION = "streaming_kafka_decode_duration_seconds"


_PRODUCED_TOTAL = counter_spec(
    KafkaMetricName.PRODUCED_TOTAL,
    "Total Kafka records produced.",
    "topic",
    "status",
)
_CONSUMED_TOTAL = counter_spec(
    KafkaMetricName.CONSUMED_TOTAL,
    "Total Kafka records consumed.",
    "topic",
    "status",
)
_ENCODE_DURATION = histogram_spec(
    KafkaMetricName.ENCODE_DURATION,
    "Kafka envelope encode duration in seconds.",
    "content_type",
)
_DECODE_DURATION = histogram_spec(
    KafkaMetricName.DECODE_DURATION,
    "Kafka envelope decode duration in seconds.",
    "content_type",
)
_INSTRUMENTS = (_PRODUCED_TOTAL, _CONSUMED_TOTAL, _ENCODE_DURATION, _DECODE_DURATION)


class KafkaPrometheusMetrics:
    """Prometheus metrics recorder for Kafka transport lifecycle events.

    Listens for :class:`~loom.core.observability.event.LifecycleEvent` with
    ``scope=TRANSPORT`` and records four instruments:

    - ``streaming_kafka_produced_total`` — produce operations by topic/status.
    - ``streaming_kafka_consumed_total`` — consume operations by topic/status.
    - ``streaming_kafka_encode_duration_seconds`` — encode latency by content type.
    - ``streaming_kafka_decode_duration_seconds`` — decode latency by content type.

    Wire this into an :class:`~loom.core.observability.observability.ObservabilityRuntime`
    and pass the runtime to the Kafka client constructors via ``obs=``.

    Args:
        registry: Optional ``CollectorRegistry``. Defaults to the global
            Prometheus registry when ``None``.

    Example::

        metrics = KafkaPrometheusMetrics(registry=registry)
        obs = ObservabilityRuntime([metrics])
        consumer = KafkaConsumerClient(settings, obs=obs)
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        instruments = cached_instruments(registry, _INSTRUMENTS)
        self._produced_total = instruments[_PRODUCED_TOTAL]
        self._consumed_total = instruments[_CONSUMED_TOTAL]
        self._encode_duration = instruments[_ENCODE_DURATION]
        self._decode_duration = instruments[_DECODE_DURATION]

    def on_event(self, event: LifecycleEvent) -> None:
        """Record one Kafka transport lifecycle event on Prometheus instruments.

        Only ``TRANSPORT`` scope events with names ``kafka_produce``,
        ``kafka_consume``, ``kafka_encode``, and ``kafka_decode`` are handled.
        All other events are ignored.

        Args:
            event: Lifecycle event from the runtime.
        """
        if event.scope is not Scope.TRANSPORT:
            return
        match event.name:
            case "kafka_produce":
                topic = str(event.meta.get("topic", "unknown"))
                status = "success" if event.kind is EventKind.END else "delivery_error"
                self._produced_total.labels(topic=topic, status=status).inc()
            case "kafka_consume":
                topic = str(event.meta.get("topic", "unknown"))
                self._consumed_total.labels(topic=topic, status="success").inc()
            case "kafka_encode":
                if event.duration_ms is not None:
                    ct = str(event.meta.get("content_type", "unknown"))
                    self._encode_duration.labels(content_type=ct).observe(event.duration_ms / 1000)
            case "kafka_decode":
                if event.kind is EventKind.ERROR:
                    topic = str(event.meta.get("topic", "unknown"))
                    self._consumed_total.labels(topic=topic, status="decode_error").inc()
                elif event.duration_ms is not None:
                    ct = str(event.meta.get("content_type", "unknown"))
                    self._decode_duration.labels(content_type=ct).observe(event.duration_ms / 1000)
