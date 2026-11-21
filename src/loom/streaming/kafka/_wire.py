"""Kafka wire decoding helpers for streaming adapters."""

from __future__ import annotations

import logging
from typing import Any, Generic, TypeAlias, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.tracing import generate_trace_id
from loom.streaming.core._dispatch import DispatchTable
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._codec import KafkaCodec
from loom.streaming.kafka._message import MessageEnvelope
from loom.streaming.kafka._record import KafkaRecord

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)

_ERROR_PREFIX = "loom.streaming.error."
logger = logging.getLogger(__name__)
_ERROR_ENVELOPE_TYPES = frozenset(
    f"{_ERROR_PREFIX}{kind.value}" for kind in ErrorKind if kind is not ErrorKind.WIRE
)
_WIRE_MESSAGE_TYPE = f"{_ERROR_PREFIX}{ErrorKind.WIRE.value}"


def _record_ctx(record: KafkaRecord[bytes]) -> dict[str, Any]:
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
    }


class _DescriptorProbe(msgspec.Struct, frozen=True):
    message_type: str
    message_version: int


class _MetadataProbe(msgspec.Struct, frozen=True):
    descriptor: _DescriptorProbe


class _EnvelopeProbe(msgspec.Struct, frozen=True):
    meta: _MetadataProbe


class _ErrorPayloadProbe(msgspec.Struct, frozen=True):
    kind: str
    reason: str
    payload_type: str | None = None


class _EnvelopeErrorProbe(msgspec.Struct, frozen=True):
    meta: _MetadataProbe
    payload: _ErrorPayloadProbe


class DecodeOk(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Successful Kafka wire decode result.

    Args:
        message: Transport-neutral message ready for DSL execution.
    """

    message: Message[PayloadT]


class DecodeError(LoomFrozenStruct, frozen=True):
    """Failed Kafka wire decode result with raw dead-letter context.

    Args:
        error: Structured WIRE error for DSL error routing.
        raw: Original record bytes that failed to decode.
        topic: Source Kafka topic.
        key: Source Kafka key, normalized to bytes when available.
        headers: Source Kafka headers.
        partition: Source Kafka partition when available.
        offset: Source Kafka offset when available.
        timestamp_ms: Source Kafka timestamp when available.
    """

    error: ErrorEnvelope[Any]
    raw: bytes
    topic: str
    key: bytes | None
    headers: dict[str, bytes]
    partition: int | None = None
    offset: int | None = None
    timestamp_ms: int | None = None

    @classmethod
    def loom_message_type(cls) -> str:
        """Return the stable logical message type for wire decode errors."""
        return _WIRE_MESSAGE_TYPE


DecodeResult: TypeAlias = DecodeOk[PayloadT] | DecodeError
"""Result of decoding one Kafka wire record without raising decode errors."""


def envelope_to_message(
    envelope: MessageEnvelope[PayloadT],
    record: KafkaRecord[bytes],
) -> Message[PayloadT]:
    """Convert a Kafka wire envelope and record context to a DSL message.

    Args:
        envelope: Decoded standard Kafka message envelope.
        record: Original Kafka transport record.

    Returns:
        Transport-neutral streaming message with envelope metadata preserved
        where it can influence DSL routing or user logic. If the envelope has
        no trace identifier, a fresh one is generated so the message still
        participates in lineage.
    """

    descriptor = envelope.meta.descriptor
    return Message(
        payload=envelope.payload,
        meta=MessageMeta(
            message_id=_message_id(record),
            correlation_id=envelope.meta.correlation_id,
            trace_id=envelope.meta.trace_id or generate_trace_id(),
            parent_trace_id=envelope.meta.parent_trace_id,
            causation_id=envelope.meta.causation_id,
            produced_at_ms=envelope.meta.produced_at_ms,
            message_type=descriptor.message_type,
            message_version=descriptor.message_version,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            headers=record.headers,
        ),
    )


def try_decode_record(
    record: KafkaRecord[bytes],
    payload_type: type[PayloadT],
    codec: KafkaCodec[PayloadT],
) -> DecodeResult[PayloadT]:
    """Decode one Kafka record to a DSL message without raising decode errors.

    Args:
        record: Raw Kafka record whose value contains a Loom message envelope.
        payload_type: Expected payload model type.
        codec: Codec used to decode the envelope bytes.

    Returns:
        ``DecodeOk`` when decoding succeeds, otherwise ``DecodeError`` carrying
        the original raw bytes and Kafka context needed by a DLQ sink.
    """

    try:
        envelope = codec.decode(record.value, payload_type)
    except Exception as exc:
        return _wire_error(str(exc), record)
    return DecodeOk(message=envelope_to_message(envelope, record))


def try_decode_multi_record(
    record: KafkaRecord[bytes],
    dispatch: DispatchTable,
    codec: KafkaCodec[Any],
) -> DecodeResult[Any]:
    """Decode one Kafka record from a heterogeneous topic using a dispatch table.

    Uses an exact-type dispatch strategy:

    1. Plain payload envelopes dispatch by ``MessageEnvelope.meta.descriptor.message_type``.
    2. Business/task/routing error envelopes dispatch by their outer error kind
       type and inner ``ErrorEnvelope.payload_type``.
    3. Wire-decode payloads dispatch by the dedicated wire error type.

    Unknown ``message_type`` values and decode failures both produce a
    ``DecodeError`` with ``ErrorKind.WIRE``.

    Args:
        record: Raw Kafka record from a heterogeneous topic.
        dispatch: Pre-built dispatch table keyed by ``message_type`` and error
            ``payload_type`` strings.
        codec: Codec used for full envelope decoding.

    Returns:
        ``DecodeOk`` on success, ``DecodeError`` on probe or decode failure.
    """

    outer_message_type = _probe_message_type(record.value)
    if outer_message_type is None:
        return _wire_error("failed to probe message_type from envelope", record)
    logger.debug(
        "multi_source_probe",
        extra={**_record_ctx(record), "outer_message_type": outer_message_type},
    )

    payload_type = dispatch.plain.get(outer_message_type)
    if payload_type is None:
        if outer_message_type in _ERROR_ENVELOPE_TYPES:
            return _decode_error_envelope(record, dispatch, codec, outer_message_type)
        wire_payload_type = dispatch.wire.get(outer_message_type)
        if wire_payload_type is not None:
            logger.debug(
                "multi_source_dispatch_wire",
                extra={
                    **_record_ctx(record),
                    "outer_message_type": outer_message_type,
                    "target_type": _describe_target_type(wire_payload_type),
                },
            )
            return try_decode_record(record, wire_payload_type, codec)
        return _wire_error(f"unknown message_type: {outer_message_type!r}", record)
    logger.debug(
        "multi_source_dispatch_plain",
        extra={
            **_record_ctx(record),
            "outer_message_type": outer_message_type,
            "target_type": _describe_target_type(payload_type),
        },
    )

    return try_decode_record(record, payload_type, codec)


def _decode_error_envelope(
    record: KafkaRecord[bytes],
    dispatch: DispatchTable,
    codec: KafkaCodec[Any],
    outer_message_type: str,
) -> DecodeResult[Any]:
    inner_payload_type = _probe_error_payload_type(record.value)
    if inner_payload_type is None:
        return _wire_error(
            f"error envelope missing payload_type for {outer_message_type!r}", record
        )

    target_type = dispatch.error.get(inner_payload_type)
    if target_type is None:
        return _wire_error(f"unknown error payload_type: {inner_payload_type!r}", record)
    logger.debug(
        "multi_source_dispatch_error",
        extra={
            **_record_ctx(record),
            "outer_message_type": outer_message_type,
            "payload_type": inner_payload_type,
            "target_type": _describe_target_type(target_type),
        },
    )

    return try_decode_record(record, target_type, codec)


def _probe_message_type(raw: bytes) -> str | None:
    try:
        probe = msgspec.msgpack.decode(raw, type=_EnvelopeProbe)
        return probe.meta.descriptor.message_type
    except Exception:
        return None


def _probe_error_payload_type(raw: bytes) -> str | None:
    try:
        probe = msgspec.msgpack.decode(raw, type=_EnvelopeErrorProbe)
        return probe.payload.payload_type
    except Exception:
        return None


def _wire_error(reason: str, record: KafkaRecord[bytes]) -> DecodeError:
    logger.warning("multi_source_wire_error", extra={**_record_ctx(record), "reason": reason})
    return DecodeError(
        error=ErrorEnvelope(kind=ErrorKind.WIRE, reason=reason, original_message=None),
        raw=record.value,
        topic=record.topic,
        key=_normalize_key(record.key),
        headers=record.headers,
        partition=record.partition,
        offset=record.offset,
        timestamp_ms=record.timestamp_ms,
    )


def _message_id(record: KafkaRecord[bytes]) -> str:
    if record.partition is not None and record.offset is not None:
        return f"{record.topic}:{record.partition}:{record.offset}"
    if record.key is not None:
        return f"{record.topic}:{record.key!s}"
    if record.timestamp_ms is not None:
        return f"{record.topic}:{record.timestamp_ms}"
    return record.topic


def _describe_target_type(t: Any) -> str:
    if not isinstance(t, type):
        return repr(t)
    return f"{t.__module__}.{t.__qualname__}"


def _normalize_key(key: bytes | str | None) -> bytes | None:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key
    return key.encode("utf-8")
