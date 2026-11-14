"""Tests for message-level Kafka clients (KafkaMessageProducer, KafkaMessageConsumer)."""

from __future__ import annotations

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry, generate_latest

from loom.core.observability.event import LifecycleEvent
from loom.core.observability.runtime import ObservabilityRuntime
from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    KafkaDeserializationError,
    KafkaMessageConsumer,
    KafkaMessageProducer,
    KafkaRecord,
    MessageDescriptor,
    MessageEnvelope,
    MsgspecCodec,
)
from tests.unit.streaming.kafka.cases import OrderCreated
from tests.unit.streaming.kafka.doubles import RawConsumerStub, RawProducerStub

pytestmark = pytest.mark.kafka


class _OrderKeyResolver:
    def resolve(self, record: KafkaRecord[MessageEnvelope[OrderCreated]]) -> bytes:
        """Resolve the key from the typed message payload."""

        return record.value.payload.order_id.encode("utf-8")


class TestKafkaMessageProducer:
    def test_encodes_and_delegates_to_raw(
        self,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
        raw_producer_stub: RawProducerStub,
    ) -> None:
        producer = KafkaMessageProducer(raw=raw_producer_stub, codec=order_created_codec)

        producer.send(
            topic="orders",
            key="tenant-a",
            payload=order_created_payload,
            descriptor=order_created_descriptor_v1,
            correlation_id="corr-1",
            parent_trace_id="parent-1",
            causation_id="cause-1",
            trace_id="trace-1",
            produced_at_ms=99,
            headers={"h": b"1"},
        )

        assert len(raw_producer_stub.sent) == 1
        record = raw_producer_stub.sent[0]
        assert record.topic == "orders"
        assert record.key == "tenant-a"
        assert record.headers == {
            "h": b"1",
            "x-correlation-id": b"corr-1",
            "x-parent-trace-id": b"parent-1",
            "x-causation-id": b"cause-1",
            "x-trace-id": b"trace-1",
        }
        assert record.timestamp_ms == 99
        assert isinstance(record.value, bytes)

        decoded = order_created_codec.decode(record.value, OrderCreated)
        assert decoded.payload == order_created_payload
        assert decoded.meta.trace_id == "trace-1"
        assert decoded.meta.parent_trace_id == "parent-1"
        assert decoded.meta.correlation_id == "corr-1"
        assert decoded.meta.causation_id == "cause-1"
        assert decoded.meta.produced_at_ms == 99
        assert decoded.meta.descriptor.message_type == "order.created"

    def test_resolves_key_when_explicit_key_is_absent(
        self,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
        raw_producer_stub: RawProducerStub,
    ) -> None:
        producer = KafkaMessageProducer(
            raw=raw_producer_stub,
            codec=order_created_codec,
            key_resolver=_OrderKeyResolver(),
        )

        producer.send(
            topic="orders",
            payload=order_created_payload,
            descriptor=order_created_descriptor_v1,
        )

        assert len(raw_producer_stub.sent) == 1
        record = raw_producer_stub.sent[0]
        assert record.key == b"o-1"

    def test_keeps_explicit_key_over_resolved_key(
        self,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
        raw_producer_stub: RawProducerStub,
    ) -> None:
        producer = KafkaMessageProducer(
            raw=raw_producer_stub,
            codec=order_created_codec,
            key_resolver=_OrderKeyResolver(),
        )

        producer.send(
            topic="orders",
            key="tenant-a",
            payload=order_created_payload,
            descriptor=order_created_descriptor_v1,
        )

        assert len(raw_producer_stub.sent) == 1
        record = raw_producer_stub.sent[0]
        assert record.key == "tenant-a"

    def test_can_disable_record_timestamp(
        self,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
        raw_producer_stub: RawProducerStub,
    ) -> None:
        producer = KafkaMessageProducer(
            raw=raw_producer_stub,
            codec=order_created_codec,
            use_message_timestamp=False,
        )

        producer.send(
            topic="orders",
            payload=order_created_payload,
            descriptor=order_created_descriptor_v1,
            produced_at_ms=99,
        )

        assert len(raw_producer_stub.sent) == 1
        record = raw_producer_stub.sent[0]
        assert record.timestamp_ms is None

    def test_flush_and_close_delegate(self) -> None:
        raw = RawProducerStub()
        codec = MsgspecCodec[OrderCreated]()
        producer = KafkaMessageProducer(raw=raw, codec=codec)

        producer.flush(500)
        assert raw.flushed is True

        producer.close()
        assert raw.closed is True

    def test_emits_encode_metrics(
        self,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
        raw_producer_stub: RawProducerStub,
    ) -> None:
        producer = KafkaMessageProducer(
            raw=raw_producer_stub,
            codec=order_created_codec,
            obs=ObservabilityRuntime([kafka_metrics]),
        )

        producer.send(
            topic="orders",
            payload=order_created_payload,
            descriptor=order_created_descriptor_v1,
        )

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_encode_duration_seconds" in text

    def test_encode_event_carries_message_trace_id(
        self,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_payload: OrderCreated,
        order_created_descriptor_v1: MessageDescriptor,
        raw_producer_stub: RawProducerStub,
    ) -> None:
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        producer = KafkaMessageProducer(
            raw=raw_producer_stub,
            codec=order_created_codec,
            obs=ObservabilityRuntime([_RecordingObserver()]),
        )

        producer.send(
            topic="orders",
            payload=order_created_payload,
            descriptor=order_created_descriptor_v1,
            trace_id="trace-1",
            correlation_id="corr-1",
        )

        assert events, "Expected at least one lifecycle event"
        assert events[-1].trace_id == "trace-1"
        assert events[-1].correlation_id == "corr-1"

    def test_context_manager_closes_raw_on_exit(self) -> None:
        raw = RawProducerStub()
        codec = MsgspecCodec[OrderCreated]()

        with KafkaMessageProducer(raw=raw, codec=codec) as producer:
            producer.send(
                topic="orders",
                payload=OrderCreated(order_id="o-1", amount=1),
                descriptor=MessageDescriptor(
                    message_type="order.created",
                    message_version=1,
                ),
            )

        assert raw.closed is True

    def test_context_manager_does_not_mask_body_exception(self) -> None:
        raw = RawProducerStub()
        raw.close_error = RuntimeError("close-boom")
        codec = MsgspecCodec[OrderCreated]()

        with (
            pytest.raises(ValueError, match="body-boom"),
            KafkaMessageProducer(raw=raw, codec=codec),
        ):
            raise ValueError("body-boom")


class TestKafkaMessageConsumer:
    def test_decodes_envelope(
        self,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_envelope_with_metadata: MessageEnvelope[OrderCreated],
        raw_consumer_stub: RawConsumerStub,
    ) -> None:
        encoded = order_created_codec.encode(order_created_envelope_with_metadata)
        raw_consumer_stub.load_records(
            [
                KafkaRecord(
                    topic="orders",
                    key=b"tenant-a",
                    value=encoded,
                    headers={"x": b"1"},
                    partition=2,
                    offset=9,
                    timestamp_ms=123,
                ),
            ]
        )
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        consumer = KafkaMessageConsumer(
            raw=raw_consumer_stub,
            codec=order_created_codec,
            payload_type=OrderCreated,
            obs=ObservabilityRuntime([_RecordingObserver()]),
        )

        record = consumer.poll(100)

        assert record is not None
        assert events, "Expected at least one lifecycle event"
        assert events[-1].trace_id == order_created_envelope_with_metadata.meta.trace_id
        assert events[-1].correlation_id == order_created_envelope_with_metadata.meta.correlation_id

    def test_returns_none_when_no_record(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub([None])
        consumer = KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        )

        assert consumer.poll(100) is None

    def test_raises_deserialization_error(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub(
            [
                KafkaRecord(topic="orders", key=None, value=b"bad-bytes"),
            ]
        )
        consumer = KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        )

        with pytest.raises(KafkaDeserializationError):
            consumer.poll(100)

    def test_close_delegates(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub()
        consumer = KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        )

        consumer.close()
        assert raw.closed is True

    def test_commit_delegates(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub()
        consumer = KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        )

        consumer.commit(asynchronous=True)

        assert raw.commit_calls == [True]

    def test_commit_offset_delegates(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub()
        consumer = KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        )

        consumer.commit_offset([TopicPartition("orders", 1, 12)])

        assert len(raw.commit_offset_calls) == 1
        assert raw.commit_offset_calls[0][0].topic == "orders"
        assert raw.commit_offset_calls[0][0].partition == 1
        assert raw.commit_offset_calls[0][0].offset == 12

    def test_commit_offset_wraps_backend_error(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub()
        raw.commit_offset_error = RuntimeError("commit-boom")
        consumer = KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        )

        partitions = [TopicPartition("orders", 1, 12)]
        with pytest.raises(RuntimeError, match="commit-boom"):
            consumer.commit_offset(partitions)

    def test_emits_decode_metrics(
        self,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
        order_created_codec: MsgspecCodec[OrderCreated],
        order_created_envelope: MessageEnvelope[OrderCreated],
        raw_consumer_stub: RawConsumerStub,
    ) -> None:
        encoded = order_created_codec.encode(order_created_envelope)
        raw_consumer_stub.load_records([KafkaRecord(topic="orders", key=None, value=encoded)])
        consumer = KafkaMessageConsumer(
            raw=raw_consumer_stub,
            codec=order_created_codec,
            payload_type=OrderCreated,
            obs=ObservabilityRuntime([kafka_metrics]),
        )

        consumer.poll(100)

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_decode_duration_seconds" in text

    def test_emits_error_metric_on_decode_failure(
        self,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
        order_created_codec: MsgspecCodec[OrderCreated],
        raw_consumer_stub: RawConsumerStub,
    ) -> None:
        raw_consumer_stub.load_records([KafkaRecord(topic="orders", key=None, value=b"bad")])
        consumer = KafkaMessageConsumer(
            raw=raw_consumer_stub,
            codec=order_created_codec,
            payload_type=OrderCreated,
            obs=ObservabilityRuntime([kafka_metrics]),
        )

        with pytest.raises(KafkaDeserializationError):
            consumer.poll(100)

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_consumed_total" in text

    def test_context_manager_closes_raw_on_exit(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub()

        with KafkaMessageConsumer(
            raw=raw,
            codec=codec,
            payload_type=OrderCreated,
        ) as consumer:
            consumer.poll(100)

        assert raw.closed is True

    def test_context_manager_does_not_mask_body_exception(self) -> None:
        codec = MsgspecCodec[OrderCreated]()
        raw = RawConsumerStub()
        raw.close_error = RuntimeError("close-boom")

        with (
            pytest.raises(ValueError, match="body-boom"),
            KafkaMessageConsumer(raw=raw, codec=codec, payload_type=OrderCreated),
        ):
            raise ValueError("body-boom")
