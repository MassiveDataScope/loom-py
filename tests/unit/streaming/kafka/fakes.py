"""Shared Kafka test doubles for streaming Kafka tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from confluent_kafka import TopicPartition

from loom.streaming.kafka import KafkaRecord


class ProducerBackendStub:
    """In-memory confluent-like producer stub."""

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.produced: list[dict[str, Any]] = []
        self.flush_calls: list[float | None] = []
        self.poll_calls: list[float] = []

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]] | None,
        timestamp: int | None = None,
        on_delivery: Any = None,
    ) -> None:
        self.produced.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": headers,
                "timestamp": timestamp,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout: float) -> None:
        self.poll_calls.append(timeout)

    def flush(self, timeout: float | None = None) -> None:
        self.flush_calls.append(timeout)


class FakeDeliveryError:
    """Fake delivery error emitted by the producer callback."""

    def __str__(self) -> str:
        return "delivery-boom"


class FakeKafkaMessage:
    """Kafka-like message returned by the raw consumer stub."""

    def __init__(
        self,
        *,
        topic: str = "orders",
        key: bytes | None = b"tenant-a",
        value: bytes | None = b"payload",
        headers: list[tuple[str, bytes | None]] | None = None,
        partition: int = 2,
        offset: int = 9,
        timestamp_ms: int = 123,
        error: object | None = None,
    ) -> None:
        self._topic = topic
        self._key = key
        self._value = value
        self._headers: list[tuple[str, bytes | None]] = (
            headers if headers is not None else [("x", b"1")]
        )
        self._partition = partition
        self._offset = offset
        self._timestamp_ms = timestamp_ms
        self._error = error

    def error(self) -> object | None:
        return self._error

    def value(self) -> bytes | None:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return (0, self._timestamp_ms)

    def headers(self) -> list[tuple[str, bytes | None]]:
        return self._headers

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


@dataclass(slots=True)
class CommittedPartitionResult:
    """Result row of Consumer.committed(): offset plus optional per-partition error."""

    topic: str
    partition: int
    offset: int
    error: object | None = None


class ConsumerBackendStub:
    """In-memory confluent-like consumer stub."""

    def __init__(self, config: dict[str, str]) -> None:
        self.config = config
        self.subscribed: list[str] = []
        self.subscribe_calls = 0
        self.assigned: list[TopicPartition] = []
        self.next_message: Any | None = None
        self.queued_messages: list[FakeKafkaMessage] = []
        self.closed = False
        self.poll_calls: list[float] = []
        self.consume_calls: list[tuple[int, float]] = []
        self.commit_calls: list[bool] = []
        self.commit_offset_calls: list[list[TopicPartition]] = []
        self.commit_error: Exception | None = None
        self.close_error: Exception | None = None
        self.committed_offsets: dict[tuple[str, int], int] = {}
        self.committed_partition_errors: dict[tuple[str, int], object] = {}
        self.committed_calls: list[tuple[list[TopicPartition], float]] = []
        self.committed_error: Exception | None = None

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics
        self.subscribe_calls += 1

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = partitions

    def poll(self, timeout: float) -> FakeKafkaMessage | None:
        self.poll_calls.append(timeout)
        return self.next_message

    def consume(self, num_messages: int, timeout: float) -> list[FakeKafkaMessage]:
        self.consume_calls.append((num_messages, timeout))
        batch = self.queued_messages[:num_messages]
        del self.queued_messages[:num_messages]
        return batch

    def commit(
        self,
        *,
        asynchronous: bool = False,
        offsets: list[TopicPartition] | None = None,
    ) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        if offsets is not None:
            self.commit_offset_calls.append(offsets)
            return
        self.commit_calls.append(asynchronous)

    def commit_offset(
        self, partitions: list[TopicPartition], *, asynchronous: bool = False
    ) -> None:
        self.commit(offsets=partitions, asynchronous=asynchronous)

    def committed(self, partitions: list[TopicPartition], timeout: float) -> list[object]:
        self.committed_calls.append((list(partitions), timeout))
        if self.committed_error is not None:
            raise self.committed_error
        results: list[object] = []
        for requested in partitions:
            offset = self.committed_offsets.get((requested.topic, requested.partition), -1001)
            error = self.committed_partition_errors.get((requested.topic, requested.partition))
            results.append(
                CommittedPartitionResult(
                    topic=requested.topic,
                    partition=requested.partition,
                    offset=offset,
                    error=error,
                )
            )
        return results

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class RawProducerStub:
    """In-memory raw producer for message-level and runtime sink tests."""

    def __init__(self) -> None:
        self.sent: list[KafkaRecord[bytes]] = []
        self.flushed = False
        self.closed = False
        self.close_error: Exception | None = None
        self.flush_error: Exception | None = None
        self.flush_errors: list[Exception | None] = []

    def send(self, record: KafkaRecord[bytes]) -> None:
        self.sent.append(record)

    def flush(self, timeout_ms: int | None = None) -> None:
        """Flush, raising the next queued outcome first, then ``flush_error``."""
        del timeout_ms
        self.flushed = True
        error = self.flush_errors.pop(0) if self.flush_errors else self.flush_error
        if error is not None:
            raise error

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class RawConsumerStub:
    """In-memory raw consumer for message-level tests."""

    def __init__(self, records: list[KafkaRecord[bytes] | None] | None = None) -> None:
        self._records = list(records or [])
        self.closed = False
        self.commit_calls: list[bool] = []
        self.commit_offset_calls: list[list[TopicPartition]] = []
        self.close_error: Exception | None = None
        self.commit_offset_error: Exception | None = None

    def load_records(self, records: list[KafkaRecord[bytes] | None]) -> None:
        """Replace the queued records consumed by the stub."""
        self._records = list(records)

    def poll(self, timeout_ms: int) -> KafkaRecord[bytes] | None:
        del timeout_ms
        if self._records:
            return self._records.pop(0)
        return None

    def commit(self, *, asynchronous: bool = False) -> None:
        self.commit_calls.append(asynchronous)

    def commit_offset(
        self, partitions: list[TopicPartition], *, asynchronous: bool = False
    ) -> None:
        del asynchronous
        if self.commit_offset_error is not None:
            raise self.commit_offset_error
        self.commit_offset_calls.append(partitions)

    def close(self) -> None:
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


@dataclass(slots=True)
class ProducerBackendInstaller:
    """Callable installer that captures the raw producer stub created by Kafka."""

    stub: ProducerBackendStub = field(default_factory=lambda: ProducerBackendStub({}))

    def __call__(self, config: dict[str, str]) -> ProducerBackendStub:
        self.stub.config = config
        return self.stub


@dataclass(slots=True)
class ConsumerBackendInstaller:
    """Callable installer that captures the raw consumer stub created by Kafka."""

    stub: ConsumerBackendStub = field(default_factory=lambda: ConsumerBackendStub({}))

    def __call__(self, config: dict[str, str]) -> ConsumerBackendStub:
        self.stub.config = config
        return self.stub


def install_raw_producer_stub(
    monkeypatch: pytest.MonkeyPatch,
    installer: ProducerBackendInstaller | None = None,
) -> ProducerBackendInstaller:
    """Install a raw producer stub into the Kafka client module and return the installer."""
    producer_installer = installer or ProducerBackendInstaller()
    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        producer_installer,
    )
    return producer_installer


def install_raw_consumer_stub(
    monkeypatch: pytest.MonkeyPatch,
    installer: ConsumerBackendInstaller | None = None,
) -> ConsumerBackendInstaller:
    """Install a raw consumer stub into the Kafka client module and return the installer."""
    consumer_installer = installer or ConsumerBackendInstaller()
    monkeypatch.setattr(
        "loom.streaming.kafka.client._consumer._Consumer",
        consumer_installer,
    )
    return consumer_installer


class PartitionClientStub:
    """Client-level stub for partitioned-source tests (KafkaConsumerClient shape)."""

    def __init__(self) -> None:
        self.committed: dict[tuple[str, int], int | None] = {}
        self.committed_requests: list[tuple[str, int, int]] = []
        self.assign_calls: list[tuple[str, int, int]] = []
        self.batches: list[list[KafkaRecord[bytes]]] = []
        self.consume_calls: list[int] = []
        self.commit_offset_calls: list[list[TopicPartition]] = []
        self.commit_async_flags: list[bool] = []
        self.commit_error: Exception | None = None
        self.closed = False

    def committed_offset(self, topic: str, partition: int, *, timeout_ms: int) -> int | None:
        self.committed_requests.append((topic, partition, timeout_ms))
        return self.committed.get((topic, partition))

    def assign_partition(self, topic: str, partition: int, offset: int) -> None:
        self.assign_calls.append((topic, partition, offset))

    def consume_batch(self, max_records: int) -> list[KafkaRecord[bytes]]:
        self.consume_calls.append(max_records)
        if self.batches:
            return self.batches.pop(0)
        return []

    def commit_offset(
        self, partitions: list[TopicPartition], *, asynchronous: bool = False
    ) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commit_offset_calls.append(list(partitions))
        self.commit_async_flags.append(asynchronous)

    def close(self) -> None:
        self.closed = True


class PartitionClientInstaller:
    """Shim replacing KafkaConsumerClient in the runtime-source module.

    ``unassigned`` hands out the pre-seeded stubs in order (one per built
    partition), recording the settings/observability it was called with.
    """

    def __init__(self, stubs: list[PartitionClientStub] | None = None) -> None:
        self.stubs = stubs if stubs is not None else [PartitionClientStub()]
        self.unassigned_calls: list[tuple[object, object]] = []

    def unassigned(self, settings: object, observability: object = None) -> PartitionClientStub:
        self.unassigned_calls.append((settings, observability))
        return self.stubs.pop(0)
