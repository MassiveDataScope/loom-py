"""Tests for raw Kafka transport clients (KafkaProducerClient, KafkaConsumerClient)."""

from __future__ import annotations

import pytest
from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry, generate_latest

from loom.core.observability.event import LifecycleEvent
from loom.core.observability.runtime import ObservabilityRuntime
from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    ConsumerSettings,
    KafkaCommitError,
    KafkaConsumerClient,
    KafkaDeliveryError,
    KafkaPollError,
    KafkaProducerClient,
    KafkaRecord,
    ProducerSettings,
)
from tests.unit.streaming.kafka.fakes import (
    ConsumerBackendStub,
    FakeDeliveryError,
    FakeKafkaMessage,
    ProducerBackendStub,
    install_raw_consumer_stub,
    install_raw_producer_stub,
)

pytestmark = pytest.mark.kafka


class _FakeError:
    def __str__(self) -> str:
        return "boom"


class TestKafkaProducerClient:
    def test_raw_producer_sends_bytes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",), client_id="p1"),
        )

        producer.send(
            KafkaRecord(
                topic="orders",
                key="tenant-a",
                value=b"raw-payload",
                headers={"h": b"1"},
                timestamp_ms=44,
            )
        )
        producer.flush(250)
        producer.close()

        fake = installer.stub
        assert fake is not None
        assert fake.config["bootstrap.servers"] == "k1:9092"
        assert fake.produced[0]["topic"] == "orders"
        assert fake.produced[0]["key"] == b"tenant-a"
        assert fake.produced[0]["value"] == b"raw-payload"
        assert fake.produced[0]["headers"] == [("h", b"1")]
        assert fake.produced[0]["timestamp"] == 44
        assert fake.poll_calls == [0.0]
        assert fake.flush_calls == [0.25, None]

    def test_raw_producer_delivery_callback_and_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen: list[KafkaDeliveryError | None] = []
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",)),
            delivery_callback=lambda record, error: seen.append(error),
        )

        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
        fake = installer.stub
        assert fake is not None
        callback = fake.produced[0]["on_delivery"]
        assert callback is not None

        callback(FakeDeliveryError(), None)

        with pytest.raises(KafkaDeliveryError, match="delivery-boom"):
            producer.flush()
        assert seen[-1] is not None

    def test_raw_producer_flush_consumes_pending_delivery_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(ProducerSettings(brokers=("k1:9092",)))
        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
        fake = installer.stub
        assert fake is not None
        fake.produced[0]["on_delivery"](FakeDeliveryError(), None)

        with pytest.raises(KafkaDeliveryError, match="delivery-boom"):
            producer.flush()
        producer.flush()

    def test_raw_producer_emits_metrics(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",)),
            obs=ObservabilityRuntime([kafka_metrics]),
        )

        producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))
        fake = installer.stub
        assert fake is not None
        fake.produced[0]["on_delivery"](None, None)
        producer.flush()

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_produced_total" in text

    def test_raw_producer_transport_event_carries_header_trace_id(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        producer = KafkaProducerClient(
            ProducerSettings(brokers=("k1:9092",)),
            obs=ObservabilityRuntime([_RecordingObserver()]),
        )

        producer.send(
            KafkaRecord(
                topic="orders",
                key=b"k",
                value=b"payload",
                headers={
                    "x-trace-id": b"trace-1",
                    "x-correlation-id": b"corr-1",
                },
            )
        )
        fake = installer.stub
        assert fake is not None
        fake.produced[0]["on_delivery"](None, None)

        assert events, "Expected at least one lifecycle event"
        assert events[-1].trace_id == "trace-1"
        assert events[-1].correlation_id == "corr-1"

    def test_raw_producer_context_manager_closes_on_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_producer_stub(monkeypatch)

        with KafkaProducerClient(ProducerSettings(brokers=("k1:9092",))) as producer:
            producer.send(KafkaRecord(topic="orders", key=b"k", value=b"payload"))

        fake = installer.stub
        assert fake is not None
        assert fake.flush_calls == [None]

    def test_raw_producer_context_manager_does_not_mask_body_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _FlushFailingProducer(ProducerBackendStub):
            def flush(self, timeout: float | None = None) -> None:
                super().flush(timeout)
                raise RuntimeError("close-boom")

        monkeypatch.setattr(
            "loom.streaming.kafka.client._producer._Producer",
            lambda config: _FlushFailingProducer(config),
        )

        with (
            pytest.raises(ValueError, match="body-boom"),
            KafkaProducerClient(ProducerSettings(brokers=("k1:9092",))),
        ):
            raise ValueError("body-boom")


class TestKafkaConsumerClient:
    def test_raw_consumer_polls_bytes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(value=b"raw-bytes")
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        record = consumer.poll(500)
        consumer.close()

        assert fake.subscribed == ["orders"]
        assert fake.poll_calls == [0.5]
        assert fake.closed is True
        assert record is not None
        assert record.topic == "orders"
        assert record.key == b"tenant-a"
        assert record.value == b"raw-bytes"
        assert record.headers == {"x": b"1"}
        assert record.partition == 2
        assert record.offset == 9
        assert record.timestamp_ms == 123

    def test_raw_consumer_returns_none_when_no_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )
        assert consumer.poll(100) is None

    def test_raw_consumer_transport_event_carries_header_trace_id(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(
            headers=[
                ("x-trace-id", b"trace-1"),
                ("x-correlation-id", b"corr-1"),
            ]
        )
        events: list[LifecycleEvent] = []

        class _RecordingObserver:
            def on_event(self, event: LifecycleEvent) -> None:
                events.append(event)

        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
            obs=ObservabilityRuntime([_RecordingObserver()]),
        )

        record = consumer.poll(100)

        assert record is not None
        assert events, "Expected at least one lifecycle event"
        assert events[-1].trace_id == "trace-1"
        assert events[-1].correlation_id == "corr-1"

    def test_raw_consumer_raises_on_kafka_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(error=_FakeError())
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )
        with pytest.raises(KafkaPollError, match="boom"):
            consumer.poll(100)

    def test_raw_consumer_commit_delegates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        consumer.commit(asynchronous=True)

        assert fake.commit_calls == [True]

    def test_raw_consumer_commit_offset_delegates(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        consumer.commit_offset([TopicPartition("orders", 1, 12)])

        assert len(fake.commit_offset_calls) == 1
        assert fake.commit_offset_calls[0][0].topic == "orders"
        assert fake.commit_offset_calls[0][0].partition == 1
        assert fake.commit_offset_calls[0][0].offset == 12

    def test_raw_consumer_commit_offset_wraps_backend_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.commit_error = RuntimeError("commit-boom")
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        partitions = [TopicPartition("orders", 1, 12)]
        with pytest.raises(KafkaCommitError, match="commit-boom"):
            consumer.commit_offset(partitions)

    def test_raw_consumer_commit_wraps_backend_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.commit_error = RuntimeError("commit-boom")
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        with pytest.raises(KafkaCommitError, match="commit-boom"):
            consumer.commit()

    def test_raw_consumer_poll_does_not_wrap_keyboard_interrupt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class _InterruptingConsumer(ConsumerBackendStub):
            def poll(self, timeout: float) -> FakeKafkaMessage | None:
                del timeout
                raise KeyboardInterrupt

        fake = _InterruptingConsumer({})
        monkeypatch.setattr("loom.streaming.kafka.client._consumer._Consumer", lambda config: fake)
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        with pytest.raises(KeyboardInterrupt):
            consumer.poll(100)

    def test_raw_consumer_emits_metrics(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kafka_registry: CollectorRegistry,
        kafka_metrics: KafkaPrometheusMetrics,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.next_message = FakeKafkaMessage(value=b"data")
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
            obs=ObservabilityRuntime([kafka_metrics]),
        )

        consumer.poll(100)

        text = generate_latest(kafka_registry).decode()
        assert "streaming_kafka_consumed_total" in text

    def test_raw_consumer_context_manager_closes_on_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None

        with KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        ) as consumer:
            consumer.poll(100)

        assert fake.closed is True

    def test_consume_batch_converts_messages_in_broker_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.queued_messages = [
            FakeKafkaMessage(value=b"first", offset=7),
            FakeKafkaMessage(value=b"second", offset=8),
        ]
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        records = consumer.consume_batch(10)

        assert fake.consume_calls == [(10, 0.001)]
        assert [record.value for record in records] == [b"first", b"second"]
        assert [record.offset for record in records] == [7, 8]

    def test_consume_batch_returns_empty_list_without_messages(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        install_raw_consumer_stub(monkeypatch)
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        assert consumer.consume_batch(5) == []

    def test_consume_batch_raises_on_kafka_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.queued_messages = [FakeKafkaMessage(error=_FakeError())]
        consumer = KafkaConsumerClient(
            ConsumerSettings(
                brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
            ),
        )

        with pytest.raises(KafkaPollError, match="boom"):
            consumer.consume_batch(5)

    def test_raw_consumer_context_manager_does_not_mask_body_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        fake = installer.stub
        assert fake is not None
        fake.close_error = RuntimeError("close-boom")

        with (
            pytest.raises(ValueError, match="body-boom"),
            KafkaConsumerClient(
                ConsumerSettings(
                    brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_most_once"
                ),
            ),
        ):
            raise ValueError("body-boom")


def _consumer_settings() -> ConsumerSettings:
    return ConsumerSettings(
        brokers=("localhost:9092",),
        group_id="test",
        topics=("orders",),
        delivery="at_most_once",
    )


class TestCommittedOffset:
    def test_returns_committed_offset_from_group_coordinator(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        installer.stub.committed_offsets[("orders", 2)] = 41
        client = KafkaConsumerClient.unassigned(_consumer_settings())

        assert client.committed_offset("orders", 2, timeout_ms=5_000) == 41
        partitions, timeout = installer.stub.committed_calls[0]
        assert (partitions[0].topic, partitions[0].partition) == ("orders", 2)
        assert timeout == 5.0

    def test_invalid_offset_maps_to_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        del installer  # default committed offset is OFFSET_INVALID (-1001)
        client = KafkaConsumerClient.unassigned(_consumer_settings())

        assert client.committed_offset("orders", 2, timeout_ms=5_000) is None

    def test_per_partition_error_is_a_hard_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        installer.stub.committed_partition_errors[("orders", 2)] = "COORDINATOR_LOAD_IN_PROGRESS"
        client = KafkaConsumerClient.unassigned(_consumer_settings())

        with pytest.raises(KafkaCommitError, match="COORDINATOR_LOAD_IN_PROGRESS"):
            client.committed_offset("orders", 2, timeout_ms=5_000)

    def test_backend_failure_is_a_hard_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        installer.stub.committed_error = RuntimeError("coordinator-timeout")
        client = KafkaConsumerClient.unassigned(_consumer_settings())

        with pytest.raises(KafkaCommitError, match="coordinator-timeout"):
            client.committed_offset("orders", 2, timeout_ms=5_000)


class TestTombstoneHandling:
    def test_consume_batch_skips_compacted_topic_tombstones(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        installer = install_raw_consumer_stub(monkeypatch)
        installer.stub.queued_messages = [
            FakeKafkaMessage(offset=1, value=b"a"),
            FakeKafkaMessage(offset=2, value=None),  # tombstone
            FakeKafkaMessage(offset=3, value=b"b"),
        ]
        client = KafkaConsumerClient.unassigned(_consumer_settings())

        records = client.consume_batch(10)

        assert [(r.offset, r.value) for r in records] == [(1, b"a"), (3, b"b")]
