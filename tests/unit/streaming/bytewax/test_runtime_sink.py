"""Tests for runtime Kafka sink wiring in the Bytewax adapter."""

from __future__ import annotations

from typing import Any

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming.bytewax import _runtime_io  # noqa: E402
from loom.streaming.core._message import Message  # noqa: E402
from loom.streaming.kafka._errors import (  # noqa: E402
    KafkaDeliveryError,
    KafkaSerializationError,
)
from loom.streaming.kafka._record import KafkaRecord  # noqa: E402
from loom.streaming.nodes._boundary import PartitionGuarantee, PartitionPolicy  # noqa: E402
from tests.unit.streaming.bytewax.cases import (  # noqa: E402
    build_compiled_sink,
    build_order_message,
)
from tests.unit.streaming.kafka.fakes import RawProducerStub  # noqa: E402

pytestmark = pytest.mark.bytewax


class _OrderPartitionStrategy:
    """Partition by order id."""

    def partition_key(self, message: Message[Any]) -> bytes | str | None:
        return f"order-{message.payload.order_id}".encode()


class TestRuntimeSinkPartitionPolicy:
    def test_uses_partition_policy_when_message_has_no_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.ENTITY_STABLE,
            allow_repartition=False,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(partition_policy=policy))

        sink.write_batch([build_order_message("123", None)])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.topic == "orders.out"
        assert record.key == b"order-123"

    def test_preserves_existing_key_without_repartition(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.BEST_EFFORT,
            allow_repartition=False,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(partition_policy=policy))

        sink.write_batch([build_order_message("123", b"tenant-a")])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.key == b"tenant-a"

    def test_can_override_existing_key_when_repartition_allowed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        policy = PartitionPolicy(
            strategy=_OrderPartitionStrategy(),
            guarantee=PartitionGuarantee.BEST_EFFORT,
            allow_repartition=True,
        )
        sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(partition_policy=policy))

        sink.write_batch([build_order_message("123", b"tenant-a")])

        assert len(fake_raw.sent) == 1
        record = fake_raw.sent[0]
        assert isinstance(record, KafkaRecord)
        assert record.key == b"order-123"


class _RecordingTracker:
    """Commit tracker that records the offsets a batch completed."""

    def __init__(self) -> None:
        self.completed: list[tuple[str, int, int]] = []

    def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
        """Accept the fan-out hook; no test here forks."""

    def complete(self, topic: str, partition: int, offset: int) -> None:
        self.completed.append((topic, partition, offset))


class _DlqRejectingProducer(RawProducerStub):
    """Raw producer whose sends to the DLQ topic raise ``dlq_error``."""

    def __init__(self, dlq_error: Exception) -> None:
        super().__init__()
        self.dlq_error = dlq_error

    def send(self, record: KafkaRecord[bytes]) -> None:
        if record.topic == "orders.dlq":
            raise self.dlq_error
        super().send(record)


def _dlq_sink(
    monkeypatch: pytest.MonkeyPatch,
    raw: RawProducerStub,
) -> tuple[_runtime_io._KafkaMessageSinkPartition, _RecordingTracker]:
    monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: raw)
    sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(dlq_topic="orders.dlq"))
    tracker = _RecordingTracker()
    sink.bind_commit_tracker(tracker)
    return sink, tracker


class TestDlqRouting:
    def test_batch_delivered_to_dlq_commits_its_offsets(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        fake_raw.flush_errors = [KafkaDeliveryError("broker unavailable")]
        sink, tracker = _dlq_sink(monkeypatch, fake_raw)

        sink.write_batch([build_order_message("123", None, partition=2, offset=9)])

        dlq_record = next(r for r in fake_raw.sent if r.topic == "orders.dlq")
        assert b"broker unavailable" in dlq_record.headers.get("x-dlq-error", b"")
        assert fake_raw.flush_count == 2
        assert tracker.completed == [("orders.in", 2, 9)]

    def test_dlq_send_failure_raises_without_committing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _DlqRejectingProducer(KafkaDeliveryError("dlq queue full"))
        fake_raw.flush_errors = [KafkaDeliveryError("broker unavailable")]
        sink, tracker = _dlq_sink(monkeypatch, fake_raw)

        with pytest.raises(KafkaDeliveryError, match="orders.dlq") as raised:
            sink.write_batch([build_order_message("123", None, partition=2, offset=9)])

        assert str(raised.value.__cause__) == "dlq queue full"
        assert tracker.completed == []

    def test_dlq_non_delivery_error_propagates_without_committing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = _DlqRejectingProducer(KafkaSerializationError("payload not encodable"))
        fake_raw.flush_errors = [KafkaDeliveryError("broker unavailable")]
        sink, tracker = _dlq_sink(monkeypatch, fake_raw)

        with pytest.raises(KafkaSerializationError, match="payload not encodable"):
            sink.write_batch([build_order_message("123", None, partition=2, offset=9)])

        assert tracker.completed == []

    def test_dlq_flush_failure_raises_without_committing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        fake_raw.flush_errors = [
            KafkaDeliveryError("broker unavailable"),
            KafkaDeliveryError("dlq not acknowledged"),
        ]
        sink, tracker = _dlq_sink(monkeypatch, fake_raw)

        with pytest.raises(KafkaDeliveryError, match="orders.dlq") as raised:
            sink.write_batch([build_order_message("123", None, partition=2, offset=9)])

        assert str(raised.value.__cause__) == "dlq not acknowledged"
        assert tracker.completed == []

    def test_no_dlq_reraises_delivery_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        fake_raw.flush_error = KafkaDeliveryError("broker unavailable")
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink())

        messages = [build_order_message("123", None)]
        with pytest.raises(KafkaDeliveryError):
            sink.write_batch(messages)

    def test_close_delegates_to_raw_message_producer(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        closed: list[str] = []

        class _ClosingProducer(RawProducerStub):
            def close(self) -> None:
                closed.append("done")

        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: _ClosingProducer())
        sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink())

        sink.close()

        assert closed == ["done"]
