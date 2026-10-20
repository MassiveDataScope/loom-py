"""Tests for direct Bytewax runtime I/O builders."""

from __future__ import annotations

from typing import cast

import pytest
from confluent_kafka import TopicPartition

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming.bytewax import _adapter, _runtime_io  # noqa: E402
from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker  # noqa: E402
from loom.streaming.compiler import CompiledMongoCDCSource  # noqa: E402
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind, snapshot_message  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.kafka import MsgspecCodec  # noqa: E402
from loom.streaming.kafka._errors import KafkaDeliveryError  # noqa: E402
from loom.streaming.kafka._record import KafkaRecord  # noqa: E402
from loom.streaming.kafka._wire import DecodeError  # noqa: E402
from loom.streaming.mongo import MongoSourceConfig  # noqa: E402
from loom.streaming.mongo._bytewax_source import MongoCDCSource  # noqa: E402
from tests.unit.streaming.bytewax.cases import (  # noqa: E402
    Order,
    build_compiled_plan,
    build_compiled_sink,
    build_compiled_source,
    build_order_message,
)
from tests.unit.streaming.kafka.fakes import (  # noqa: E402
    ConsumerBackendStub,
    PartitionClientInstaller,
    PartitionClientStub,
    RawProducerStub,
)

pytestmark = pytest.mark.bytewax


class TestRuntimeIOBuilders:
    def test_build_runtime_source_returns_mongo_source_without_commit_tracker(self) -> None:
        source = CompiledMongoCDCSource(
            settings=MongoSourceConfig(uri="mongodb://localhost:27017", database="app"),
            collections=("orders",),
            watch_options={"full_document": "updateLookup"},
            shape=build_compiled_source().shape,
        )

        runtime_source = _runtime_io.build_runtime_source(source)

        assert isinstance(runtime_source, MongoCDCSource)
        assert _runtime_io.build_commit_tracker(source) is None

    def test_build_runtime_source_returns_partitioned_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stub = PartitionClientStub()
        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", PartitionClientInstaller([stub]))
        source = _runtime_io.build_runtime_source(build_compiled_source())

        assert isinstance(source, _runtime_io.KafkaPartitionedSource)
        partition = source.build_part("s", "orders.in:2", resume_state=None)
        assert partition.next_batch() == []
        partition.close()
        assert stub.closed is True

    def test_build_runtime_sink_returns_sink_and_terminal_mappings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)
        source = build_compiled_source(enable_auto_commit=False)
        tracker = _runtime_io.build_commit_tracker(source)
        assert tracker is not None

        sink = _runtime_io.build_runtime_sink(build_compiled_sink(), tracker)
        error_sinks = _runtime_io.build_runtime_error_sinks(
            {ErrorKind.WIRE: build_compiled_sink(topic="orders.dlq")},
            tracker,
        )
        terminal_sinks = _runtime_io.build_runtime_terminal_sinks(
            {(0,): build_compiled_sink(topic="orders.terminal")},
            tracker,
        )

        assert isinstance(sink, _runtime_io._KafkaMessageSink)
        assert isinstance(error_sinks[ErrorKind.WIRE], _runtime_io._KafkaDecodeErrorSink)
        assert isinstance(terminal_sinks[(0,)], _runtime_io._KafkaMessageSink)

        partition = sink.build("step", 0, 1)
        assert isinstance(partition, _runtime_io._KafkaMessageSinkPartition)
        partition.write_batch([build_order_message("123", None)])
        assert len(fake_raw.sent) >= 1

        error_partition = cast(
            _runtime_io._KafkaDecodeErrorSinkPartition,
            error_sinks[ErrorKind.WIRE].build("step", 0, 1),
        )
        error_partition.write_batch(
            [
                DecodeError(
                    error=ErrorEnvelope(
                        kind=ErrorKind.WIRE,
                        reason="decode failed",
                        original_message=None,
                    ),
                    raw=b"bad-wire",
                    topic="orders.in",
                    key=b"tenant-a",
                    headers={"h": b"1"},
                    partition=0,
                    offset=4,
                    timestamp_ms=12,
                )
            ]
        )

        terminal_partition = terminal_sinks[(0,)].build("step", 0, 1)
        terminal_partition.write_batch([build_order_message("789", None)])
        partition.close()
        error_partition.close()
        terminal_partition.close()

        assert [record.topic for record in fake_raw.sent] == [
            "orders.out",
            "orders.dlq",
            "orders.terminal",
        ]

    def test_runtime_sinks_generate_child_traces_with_parent_lineage(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)
        monkeypatch.setattr(_runtime_io, "generate_trace_id", lambda: "child-trace")

        sink = _runtime_io.build_runtime_sink(build_compiled_sink(), None)
        error_sinks = _runtime_io.build_runtime_error_sinks(
            {ErrorKind.TASK: build_compiled_sink(topic="orders.errors")},
            None,
        )

        message = Message(
            payload=Order(order_id="123"),
            meta=MessageMeta(
                message_id="m-1",
                trace_id="parent-trace",
                parent_trace_id="grandparent-trace",
                correlation_id="corr-1",
                causation_id="cause-1",
                topic="orders.in",
                partition=2,
                offset=9,
            ),
        )
        sink.build("step", 0, 1).write_batch([message])

        error_envelope = ErrorEnvelope[Order](
            kind=ErrorKind.TASK,
            reason="boom",
            original_message=snapshot_message(message),
        )
        cast(
            _runtime_io._KafkaErrorEnvelopeSinkPartition,
            error_sinks[ErrorKind.TASK].build("step", 0, 1),
        ).write_batch([error_envelope])

        codec = MsgspecCodec[Order]()
        assert len(fake_raw.sent) >= 2
        first_record, second_record = fake_raw.sent[:2]
        decoded_message = codec.decode(first_record.value, Order)
        decoded_error = MsgspecCodec[ErrorEnvelope[Order]]().decode(
            second_record.value,
            ErrorEnvelope[Order],
        )

        assert decoded_message.meta.trace_id == "child-trace"
        assert decoded_message.meta.parent_trace_id == "parent-trace"
        assert decoded_error.meta.trace_id == "parent-trace"
        assert decoded_error.meta.parent_trace_id == "grandparent-trace"

    def test_build_runtime_error_sink_writes_error_envelope_payloads(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        error_sinks = _runtime_io.build_runtime_error_sinks(
            {ErrorKind.TASK: build_compiled_sink(topic="orders.errors")},
            None,
        )
        partition = cast(
            _runtime_io._KafkaErrorEnvelopeSinkPartition,
            error_sinks[ErrorKind.TASK].build("step", 0, 1),
        )
        original = build_order_message("123", b"tenant-a")
        envelope: ErrorEnvelope[Order] = ErrorEnvelope(
            kind=ErrorKind.TASK,
            reason="boom",
            original_message=snapshot_message(original),
        )

        partition.write_batch([envelope])
        partition.close()

        assert len(fake_raw.sent) >= 1
        assert [record.topic for record in fake_raw.sent] == ["orders.errors"]
        assert fake_raw.sent[0].key == b"tenant-a"
        assert fake_raw.sent[0].headers["x-error-kind"] == b"task"
        assert fake_raw.sent[0].headers["x-error-reason"] == b"boom"

    def test_build_runtime_error_sink_writes_decode_error_payloads(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        error_sinks = _runtime_io.build_runtime_error_sinks(
            {ErrorKind.WIRE: build_compiled_sink(topic="orders.errors")},
            None,
        )
        partition = cast(
            _runtime_io._KafkaDecodeErrorSinkPartition,
            error_sinks[ErrorKind.WIRE].build("step", 0, 1),
        )
        envelope = DecodeError(
            error=ErrorEnvelope(
                kind=ErrorKind.WIRE,
                reason="decode failed",
                original_message=None,
            ),
            raw=b"bad-wire",
            topic="orders.in",
            key=b"tenant-a",
            headers={"h": b"1"},
            partition=0,
            offset=4,
            timestamp_ms=12,
        )

        partition.write_batch([envelope])
        partition.close()

        assert len(fake_raw.sent) >= 1
        assert [record.topic for record in fake_raw.sent] == ["orders.errors"]
        assert fake_raw.sent[0].key == b"tenant-a"
        assert fake_raw.sent[0].headers["x-error-kind"] == b"wire"
        assert fake_raw.sent[0].headers["x-error-reason"] == b"decode failed"

    def test_build_commit_tracker_defaults_to_legacy_at_most_once(self) -> None:
        """Configs setting neither delivery nor enable_auto_commit keep prior behavior."""
        source = build_compiled_source()

        assert source.settings.to_confluent_config()["enable.auto.commit"] is True
        assert _runtime_io.build_commit_tracker(source) is None

    def test_build_commit_tracker_with_explicit_at_least_once(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(delivery="at_least_once"))

        assert isinstance(tracker, KafkaCommitTracker)

    def test_build_commit_tracker_with_explicit_at_most_once(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(delivery="at_most_once"))

        assert tracker is None

    def test_commit_tracker_commits_coalesced_after_sink_write(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        source_cfg = build_compiled_source(enable_auto_commit=False)
        tracker = _runtime_io.build_commit_tracker(source_cfg)
        assert tracker is not None

        stub = PartitionClientStub()
        stub.batches = [
            [KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=9)],
            [],
        ]
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaConsumerClient", PartitionClientInstaller([stub]))
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)

        source = _runtime_io.build_runtime_source(source_cfg, tracker)
        sink = _runtime_io.build_runtime_sink(build_compiled_sink(), tracker)
        assert isinstance(source, _runtime_io.KafkaPartitionedSource)
        source_partition = source.build_part("s", "orders.in:2", resume_state=None)

        records = source_partition.next_batch()
        assert [r.offset for r in records] == [9]
        # the sink completes; no commit happens yet (coalescing)
        sink_partition = sink.build("step", 0, 1)
        sink_partition.write_batch([build_order_message("123", None, partition=2, offset=9)])
        assert stub.commit_offset_calls == []
        # the next source-partition cycle flushes the watermark
        source_partition.next_batch()
        assert stub.commit_offset_calls == [[TopicPartition("orders.in", 2, 10)]]

    def test_commit_tracker_waits_for_contiguous_offsets(
        self,
    ) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 2, consumer, None)

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=3)
        )
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=4)
        )
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=5)
        )

        tracker.complete("orders.in", 2, 5)
        tracker.complete("orders.in", 2, 3)
        tracker.flush_partition("orders.in", 2)
        assert consumer.commit_offset_calls == [[TopicPartition("orders.in", 2, 4)]]

        tracker.complete("orders.in", 2, 4)
        tracker.flush_partition("orders.in", 2)

        assert consumer.commit_offset_calls == [
            [TopicPartition("orders.in", 2, 4)],
            [TopicPartition("orders.in", 2, 6)],
        ]

    def test_commit_tracker_accounts_for_broadcast_fanout(
        self,
    ) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 2, consumer, None)

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=9)
        )
        tracker.fork("orders.in", 2, 9, 2)

        tracker.complete("orders.in", 2, 9)
        assert tracker.flush_partition("orders.in", 2) == []

        tracker.complete("orders.in", 2, 9)
        assert tracker.flush_partition("orders.in", 2) == []

        tracker.complete("orders.in", 2, 9)
        assert tracker.flush_partition("orders.in", 2) == [TopicPartition("orders.in", 2, 10)]
        assert consumer.commit_offset_calls == [[TopicPartition("orders.in", 2, 10)]]

    def test_commit_tracker_propagates_commit_offset_errors(
        self,
    ) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        consumer.commit_error = RuntimeError("commit-boom")
        tracker.attach_partition("orders.in", 2, consumer, None)

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=2, offset=9)
        )

        tracker.complete("orders.in", 2, 9)
        with pytest.raises(RuntimeError, match="commit-boom"):
            tracker.flush_partition("orders.in", 2)

    def test_build_inline_sink_partition_can_write_dlq_payloads(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_raw = RawProducerStub()
        monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: fake_raw)
        fake_raw.flush_errors = [KafkaDeliveryError("broker unavailable")]

        partition = _runtime_io.build_inline_sink_partition(
            build_compiled_sink(dlq_topic="orders.dlq"),
        )
        partition.write_batch([build_order_message("123", None)])

        topics = [record.topic for record in fake_raw.sent]
        assert "orders.out" in topics
        assert "orders.dlq" in topics

    def test_branch_terminal_without_sink_falls_back_to_a_drop_sink(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A branch with no terminal still owes its record a completion.

        The broadcast that created the branch already forked the offset once
        per branch. Discarding the stream silently left that fork outstanding
        forever and froze the partition's watermark.
        """
        tracker = KafkaCommitTracker()
        outputs: list[tuple[str, object, object]] = []
        monkeypatch.setattr(
            _adapter,
            "bw_output",
            lambda step_id, stream, sink: outputs.append((step_id, stream, sink)),
        )
        ctx = _adapter._BuildContext(
            plan=build_compiled_plan(),
            bridge=None,
            flow_runtime=ObservabilityRuntime.noop(),
            flow_run_id="run-1",
            sink=None,
            terminal_sinks={},
            error_sinks={},
            commit_tracker=tracker,
        )

        ctx.wire_branch_terminal("branch", object(), (0, 1))

        assert len(outputs) == 1, "the branch stream was discarded instead of drained"
        assert isinstance(outputs[0][2], _adapter._DropSink)


class TestCommitTrackerGapsAndFloor:
    def test_watermark_passes_offset_gaps(self) -> None:
        """Offset gaps (transactions, compaction) must never freeze commits."""
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 0, consumer, None)

        for offset in (100, 101, 104, 106):  # 102-103 and 105 are never delivered
            tracker.register_record(
                KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=offset)
            )
        for offset in (100, 104, 101, 106):
            tracker.complete("orders.in", 0, offset)

        assert tracker.flush_partition("orders.in", 0) == [TopicPartition("orders.in", 0, 107)]

    def test_floor_suppresses_commits_at_or_below_committed_offset(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        # production attaches floor and seeded watermark together, from the
        # group offset observed when the partition was built
        tracker.attach_partition("orders.in", 0, consumer, 6)

        for offset in (3, 4):
            tracker.register_record(
                KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=offset)
            )
            tracker.complete("orders.in", 0, offset)
        # watermark 5 is strictly below the floor: the group never rewinds
        assert tracker.flush_partition("orders.in", 0) == []

        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=5)
        )
        tracker.complete("orders.in", 0, 5)
        # watermark == floor commits idempotently (keep-alive relies on this)
        assert tracker.flush_partition("orders.in", 0) == [TopicPartition("orders.in", 0, 6)]

    def test_partition_committers_route_commits_to_their_owner(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        owner_zero = ConsumerBackendStub({})
        owner_one = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 0, owner_zero, None)
        tracker.attach_partition("orders.in", 1, owner_one, None)

        for partition in (0, 1):
            tracker.register_record(
                KafkaRecord(
                    topic="orders.in", key=None, value=b"raw", partition=partition, offset=5
                )
            )
            tracker.complete("orders.in", partition, 5)
            tracker.flush_partition("orders.in", partition)

        assert owner_zero.commit_offset_calls == [[TopicPartition("orders.in", 0, 6)]]
        assert owner_one.commit_offset_calls == [[TopicPartition("orders.in", 1, 6)]]

    def test_force_flush_recommits_current_watermark_for_keepalive(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 0, consumer, None)
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=9)
        )
        tracker.complete("orders.in", 0, 9)
        assert tracker.flush_partition("orders.in", 0) == [TopicPartition("orders.in", 0, 10)]

        assert tracker.flush_partition("orders.in", 0) == []  # sin avance: nada que commitear
        assert tracker.keepalive_partition("orders.in", 0) == [TopicPartition("orders.in", 0, 10)]

    def test_rebuilding_a_partition_clears_inflight_state(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 0, consumer, None)
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=9)
        )

        tracker.attach_partition("orders.in", 0, consumer, None)  # rebuild resets
        tracker.complete("orders.in", 0, 9)  # orphan complete: no-op

        assert tracker.keepalive_partition("orders.in", 0) == []


class TestCommitTrackerRobustness:
    def test_duplicate_register_of_inflight_offset_is_a_noop(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        tracker.attach_partition("orders.in", 0, consumer, None)
        record = KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=9)

        tracker.register_record(record)
        tracker.register_record(record)  # redelivery of an in-flight offset
        tracker.complete("orders.in", 0, 9)

        assert tracker.flush_partition("orders.in", 0) == [TopicPartition("orders.in", 0, 10)]
        assert tracker.flush_partition("orders.in", 0) == []

    def test_commit_failure_remarks_dirty_so_next_flush_retries(self) -> None:
        tracker = _runtime_io.build_commit_tracker(build_compiled_source(enable_auto_commit=False))
        assert tracker is not None
        consumer = ConsumerBackendStub({})
        consumer.commit_error = RuntimeError("transient-commit-boom")
        tracker.attach_partition("orders.in", 0, consumer, None)
        tracker.register_record(
            KafkaRecord(topic="orders.in", key=None, value=b"raw", partition=0, offset=9)
        )
        tracker.complete("orders.in", 0, 9)

        with pytest.raises(RuntimeError, match="transient-commit-boom"):
            tracker.flush_partition("orders.in", 0)

        consumer.commit_error = None
        assert tracker.flush_partition("orders.in", 0) == [TopicPartition("orders.in", 0, 10)]
