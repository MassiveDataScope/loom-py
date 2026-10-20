"""DLQ records derived from the sink's own send requests."""

from __future__ import annotations

from typing import Any, cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming.bytewax import _runtime_io  # noqa: E402
from loom.streaming.core._errors import (  # noqa: E402
    ErrorEnvelope,
    ErrorKind,
    ErrorMessage,
    ErrorMessageMeta,
)
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.kafka._errors import KafkaDeliveryError  # noqa: E402
from loom.streaming.kafka._message import (  # noqa: E402
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_PARENT_TRACE_ID,
    HEADER_TRACE_ID,
)
from loom.streaming.kafka._record import KafkaRecord  # noqa: E402
from loom.streaming.kafka._wire import DecodeError  # noqa: E402
from tests.unit.streaming.bytewax.cases import Order, build_compiled_sink  # noqa: E402
from tests.unit.streaming.kafka.fakes import RawProducerStub  # noqa: E402

pytestmark = pytest.mark.bytewax


def _failing_sink_producer(monkeypatch: pytest.MonkeyPatch) -> RawProducerStub:
    """Install a raw producer whose first flush, the sink's, fails."""
    raw = RawProducerStub()
    raw.flush_errors = [KafkaDeliveryError("broker down")]
    monkeypatch.setattr(_runtime_io, "KafkaProducerClient", lambda settings: raw)
    return raw


def _dlq_records(raw: RawProducerStub) -> list[KafkaRecord[bytes]]:
    return [record for record in raw.sent if record.topic == "orders.dlq"]


def test_message_dlq_records_keep_the_failed_message_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _failing_sink_producer(monkeypatch)
    sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(dlq_topic="orders.dlq"))
    messages = [
        Message(
            payload=Order(order_id="a"),
            meta=MessageMeta(
                message_id="m-1",
                topic="orders.in",
                partition=2,
                offset=9,
                trace_id="trace-old-1",
                parent_trace_id="grandparent-1",
                correlation_id="corr-1",
                causation_id="cause-1",
                produced_at_ms=10,
            ),
        ),
        Message(
            payload=Order(order_id="b"),
            meta=MessageMeta(
                message_id="m-2",
                topic="orders.in",
                partition=3,
                offset=11,
                trace_id="trace-old-2",
                parent_trace_id="grandparent-2",
                correlation_id="corr-2",
                causation_id="cause-2",
                produced_at_ms=20,
            ),
        ),
    ]

    sink.write_batch(cast(Any, messages))

    first, second = _dlq_records(raw)
    assert first.headers[HEADER_TRACE_ID] == b"trace-old-1"
    assert second.headers[HEADER_TRACE_ID] == b"trace-old-2"
    assert first.headers[HEADER_PARENT_TRACE_ID] == b"grandparent-1"
    assert second.headers[HEADER_PARENT_TRACE_ID] == b"grandparent-2"
    assert first.headers["x-dlq-error"] == b"broker down"


def test_message_dlq_record_keeps_the_message_key(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _failing_sink_producer(monkeypatch)
    sink = _runtime_io._KafkaMessageSinkPartition(build_compiled_sink(dlq_topic="orders.dlq"))
    message = Message(
        payload=Order(order_id="a"),
        meta=MessageMeta(message_id="m-1", topic="orders.in", key=b"tenant-a"),
    )

    sink.write_batch(cast(Any, [message]))

    (record,) = _dlq_records(raw)
    assert record.key == b"tenant-a"


def test_error_envelope_dlq_records_carry_the_sink_record_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _failing_sink_producer(monkeypatch)
    sink = _runtime_io._KafkaErrorEnvelopeSinkPartition(build_compiled_sink(dlq_topic="orders.dlq"))
    original = Message(
        payload=Order(order_id="a"),
        meta=MessageMeta(
            message_id="m-1",
            topic="orders.in",
            partition=2,
            offset=9,
            trace_id="trace-old",
            parent_trace_id="parent-trace",
            correlation_id="corr-1",
            causation_id="cause-1",
            produced_at_ms=10,
            headers={"x-original": b"1"},
            key=b"tenant-a",
        ),
    )
    with_original: ErrorEnvelope[Order] = ErrorEnvelope(
        kind=ErrorKind.TASK,
        reason="boom",
        payload_type="order.created",
        original_message=ErrorMessage(
            payload=original.payload,
            meta=ErrorMessageMeta(
                message_id=original.meta.message_id,
                correlation_id=original.meta.correlation_id,
                trace_id=original.meta.trace_id,
                parent_trace_id=original.meta.parent_trace_id,
                causation_id=original.meta.causation_id,
                produced_at_ms=original.meta.produced_at_ms,
                message_type=original.meta.message_type,
                message_version=original.meta.message_version,
                topic=original.meta.topic,
                partition=original.meta.partition,
                offset=original.meta.offset,
                key=cast(bytes | None, original.meta.key),
                headers=original.meta.headers,
            ),
        ),
    )
    without_original: ErrorEnvelope[Order] = ErrorEnvelope(
        kind=ErrorKind.ROUTING,
        reason="missing route",
    )

    sink.write_batch(cast(Any, [with_original, without_original]))

    first, second = _dlq_records(raw)
    assert first.headers[HEADER_TRACE_ID] == b"trace-old"
    assert first.headers[HEADER_PARENT_TRACE_ID] == b"parent-trace"
    assert first.headers[HEADER_CORRELATION_ID] == b"corr-1"
    assert first.key == b"tenant-a"
    assert first.headers["x-error-kind"] == b"task"
    assert first.headers["x-error-reason"] == b"boom"
    assert first.headers["x-dlq-error"] == b"broker down"
    assert HEADER_PARENT_TRACE_ID not in second.headers
    assert HEADER_CORRELATION_ID not in second.headers
    assert second.key is None


def test_decode_error_dlq_record_uses_the_original_record_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _failing_sink_producer(monkeypatch)
    sink = _runtime_io._KafkaDecodeErrorSinkPartition(build_compiled_sink(dlq_topic="orders.dlq"))
    decode_error = DecodeError(
        error=ErrorEnvelope(kind=ErrorKind.WIRE, reason="bad wire"),
        raw=b"raw",
        topic="orders.in",
        key=b"tenant-a",
        headers={
            HEADER_CORRELATION_ID: b"corr-1",
            HEADER_TRACE_ID: b"trace-old",
            HEADER_PARENT_TRACE_ID: b"parent-trace",
            HEADER_CAUSATION_ID: b"cause-1",
        },
        partition=2,
        offset=3,
        timestamp_ms=4,
    )

    sink.write_batch([decode_error])

    (record,) = _dlq_records(raw)
    assert record.key == b"tenant-a"
    assert record.headers[HEADER_TRACE_ID] == b"trace-old"
    assert record.headers[HEADER_PARENT_TRACE_ID] == b"parent-trace"
    assert record.headers[HEADER_CORRELATION_ID] == b"corr-1"
    assert record.headers[HEADER_CAUSATION_ID] == b"cause-1"
    assert record.headers["x-error-kind"] == b"wire"
    assert record.headers["x-error-reason"] == b"bad wire"
    assert record.headers["x-dlq-error"] == b"broker down"
