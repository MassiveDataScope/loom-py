"""Direct coverage for Bytewax DLQ helpers."""

from __future__ import annotations

from typing import Any, cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming.bytewax import _dlq  # noqa: E402
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
from loom.streaming.kafka._wire import DecodeError  # noqa: E402
from tests.unit.streaming.bytewax.cases import Order  # noqa: E402

pytestmark = pytest.mark.bytewax


class _RecordingProducer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def send(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


def test_decode_str_header_handles_missing_and_present_values() -> None:
    headers = {"x": b"value"}

    assert _dlq._decode_str_header(headers, "x") == "value"
    assert _dlq._decode_str_header(headers, "missing") is None


def test_send_batch_to_dlq_preserves_trace_lineage() -> None:
    producer = _RecordingProducer()

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

    _dlq.send_batch_to_dlq(
        cast(Any, producer),
        "orders.dlq",
        cast(Any, messages),
        KafkaDeliveryError("broker down"),
    )

    assert [call["topic"] for call in producer.calls] == ["orders.dlq", "orders.dlq"]
    assert producer.calls[0]["parent_trace_id"] == "grandparent-1"
    assert producer.calls[1]["parent_trace_id"] == "grandparent-2"
    assert producer.calls[0]["trace_id"] == "trace-old-1"
    assert producer.calls[1]["trace_id"] == "trace-old-2"
    assert producer.calls[0]["headers"]["x-dlq-error"] == b"broker down"


def test_send_error_batch_to_dlq_handles_original_and_missing_messages() -> None:
    producer = _RecordingProducer()

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

    _dlq.send_error_batch_to_dlq(
        cast(Any, producer),
        "orders.dlq",
        cast(Any, [with_original, without_original]),
        KafkaDeliveryError("broker down"),
    )

    assert [call["trace_id"] for call in producer.calls] == ["trace-old", None]
    assert producer.calls[0]["parent_trace_id"] == "parent-trace"
    assert producer.calls[0]["correlation_id"] == "corr-1"
    assert producer.calls[0]["key"] == b"tenant-a"
    assert producer.calls[0]["headers"]["x-error-kind"] == b"task"
    assert producer.calls[1]["parent_trace_id"] is None
    assert producer.calls[1]["correlation_id"] is None
    assert producer.calls[1]["key"] is None


def test_send_decode_error_batch_to_dlq_uses_original_headers_and_trace_id() -> None:
    producer = _RecordingProducer()

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

    _dlq.send_decode_error_batch_to_dlq(
        cast(Any, producer),
        "orders.dlq",
        cast(Any, [decode_error]),
        KafkaDeliveryError("broker down"),
    )

    assert len(producer.calls) == 1
    call = producer.calls[0]
    assert call["trace_id"] == "trace-old"
    assert call["parent_trace_id"] == "parent-trace"
    assert call["correlation_id"] == "corr-1"
    assert call["causation_id"] == "cause-1"
    assert call["headers"]["x-error-kind"] == b"wire"
    assert call["headers"]["x-error-reason"] == b"bad wire"
