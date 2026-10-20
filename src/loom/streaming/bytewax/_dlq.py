"""Dead-letter queue helpers for the streaming Bytewax runtime."""

from __future__ import annotations

from loom.streaming.core._errors import ErrorEnvelope
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._errors import KafkaDeliveryError
from loom.streaming.kafka._message import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_PARENT_TRACE_ID,
    HEADER_TRACE_ID,
    MessageDescriptor,
)
from loom.streaming.kafka._wire import DecodeError
from loom.streaming.kafka.message._producer import KafkaMessageProducer


def _decode_str_header(headers: dict[str, bytes], key: str) -> str | None:
    raw = headers.get(key)
    return raw.decode() if raw is not None else None


def send_batch_to_dlq(
    producer: KafkaMessageProducer[StreamPayload],
    dlq_topic: str,
    messages: list[Message[StreamPayload]],
    exc: KafkaDeliveryError,
) -> None:
    """Send every message in *messages* to the DLQ topic.

    The records are not flushed here; the caller flushes to confirm delivery.

    Args:
        producer: Message producer used to write DLQ records.
        dlq_topic: Target DLQ Kafka topic name.
        messages: Batch of messages that failed delivery.
        exc: Delivery error that triggered the DLQ fallback.
    """
    error_bytes = str(exc).encode("utf-8")
    for message in messages:
        descriptor = MessageDescriptor(
            message_type=message.meta.message_type or dlq_topic,
            message_version=message.meta.message_version or 1,
        )
        headers = {**message.meta.headers, "x-dlq-error": error_bytes}
        producer.send(
            topic=dlq_topic,
            payload=message.payload,
            descriptor=descriptor,
            headers=headers,
            correlation_id=message.meta.correlation_id,
            parent_trace_id=message.meta.parent_trace_id,
            causation_id=message.meta.causation_id,
            trace_id=message.meta.trace_id,
            produced_at_ms=message.meta.produced_at_ms,
        )


def send_error_batch_to_dlq(
    producer: KafkaMessageProducer[ErrorEnvelope[StreamPayload]],
    dlq_topic: str,
    envelopes: list[ErrorEnvelope[StreamPayload]],
    exc: KafkaDeliveryError,
) -> None:
    """Send every error envelope in *envelopes* to the DLQ topic.

    The records are not flushed here; the caller flushes to confirm delivery.

    Args:
        producer: Message producer used to write DLQ records.
        dlq_topic: Target DLQ Kafka topic name.
        envelopes: Batch of error envelopes that failed delivery.
        exc: Delivery error that triggered the DLQ fallback.
    """
    error_bytes = str(exc).encode("utf-8")
    for envelope in envelopes:
        original = envelope.original_message
        descriptor = MessageDescriptor(
            message_type=f"loom.streaming.error.{envelope.kind.value}",
            message_version=1,
        )
        headers = {"x-dlq-error": error_bytes}
        correlation_id = None
        causation_id = None
        produced_at_ms = None
        key = None
        if original is not None:
            headers = {
                **original.meta.headers,
                **headers,
                "x-error-kind": envelope.kind.value.encode("utf-8"),
            }
            correlation_id = original.meta.correlation_id
            causation_id = original.meta.causation_id
            produced_at_ms = original.meta.produced_at_ms
            key = original.meta.key
        producer.send(
            topic=dlq_topic,
            payload=envelope,
            descriptor=descriptor,
            key=key,
            headers=headers,
            correlation_id=correlation_id,
            parent_trace_id=original.meta.parent_trace_id if original is not None else None,
            causation_id=causation_id,
            trace_id=original.meta.trace_id if original is not None else None,
            produced_at_ms=produced_at_ms,
        )


def send_decode_error_batch_to_dlq(
    producer: KafkaMessageProducer[DecodeError],
    dlq_topic: str,
    errors: list[DecodeError],
    exc: KafkaDeliveryError,
) -> None:
    """Send every decode error in *errors* to the DLQ topic.

    Trace context is recovered from the original Kafka record headers when
    present (written there by the producer via ``x-correlation-id`` et al.),
    preserving lineage even when the envelope payload could not be decoded.
    The records are not flushed here; the caller flushes to confirm delivery.

    Args:
        producer: Message producer used to write DLQ records.
        dlq_topic: Target DLQ Kafka topic name.
        errors: Batch of decode errors that failed delivery.
        exc: Delivery error that triggered the DLQ fallback.
    """
    error_bytes = str(exc).encode("utf-8")
    for error in errors:
        headers: dict[str, bytes] = {
            **error.headers,
            "x-dlq-error": error_bytes,
            "x-error-kind": error.error.kind.value.encode("utf-8"),
            "x-error-reason": error.error.reason.encode("utf-8"),
        }
        descriptor = MessageDescriptor(
            message_type=f"loom.streaming.error.{error.error.kind.value}",
            message_version=1,
        )
        producer.send(
            topic=dlq_topic,
            payload=error,
            descriptor=descriptor,
            key=error.key,
            headers=headers,
            correlation_id=_decode_str_header(error.headers, HEADER_CORRELATION_ID),
            parent_trace_id=_decode_str_header(error.headers, HEADER_PARENT_TRACE_ID),
            causation_id=_decode_str_header(error.headers, HEADER_CAUSATION_ID),
            trace_id=_decode_str_header(error.headers, HEADER_TRACE_ID),
            produced_at_ms=error.timestamp_ms,
        )


__all__ = ["send_batch_to_dlq", "send_decode_error_batch_to_dlq", "send_error_batch_to_dlq"]
