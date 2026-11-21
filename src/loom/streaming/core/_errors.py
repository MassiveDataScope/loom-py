"""Streaming error model for explicit error branches."""

from __future__ import annotations

from enum import StrEnum
from typing import Generic, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.core._message import Message

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct, covariant=True)


class ErrorKind(StrEnum):
    """Logical categories for streaming flow errors."""

    WIRE = "wire"
    ROUTING = "routing"
    TASK = "task"
    BUSINESS = "business"


class ErrorMessageMeta(LoomFrozenStruct, frozen=True, kw_only=True):
    """Wire-safe snapshot of the original message metadata.

    Args:
        message_id: Stable event identifier.
        correlation_id: Optional correlation identifier.
        trace_id: Optional trace identifier.
        parent_trace_id: Optional upstream trace identifier.
        causation_id: Optional upstream event identifier.
        produced_at_ms: Optional original producer timestamp in epoch
            milliseconds.
        message_type: Optional logical message contract name.
        message_version: Optional logical message contract version.
        topic: Source topic name when available.
        partition: Source partition when available.
        offset: Source offset when available.
        key: Optional transport key when available.  Stored as bytes to keep the
            error envelope msgspec-compatible when nested inside Kafka payloads.
        headers: Opaque transport headers.
    """

    message_id: str
    correlation_id: str | None = None
    trace_id: str | None = None
    parent_trace_id: str | None = None
    causation_id: str | None = None
    produced_at_ms: int | None = None
    message_type: str | None = None
    message_version: int | None = None
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    key: bytes | None = None
    headers: dict[str, bytes] = msgspec.field(default_factory=dict)


class ErrorMessage(LoomFrozenStruct, Generic[PayloadT], frozen=True, kw_only=True):
    """Wire-safe snapshot of the original message carried by an error.

    Args:
        payload: Original domain payload.
        meta: Snapshot of the original message metadata.
    """

    payload: PayloadT
    meta: ErrorMessageMeta


class ErrorEnvelope(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Structured error payload routed through explicit error branches.

    Args:
        kind: Logical error category.
        reason: Human-readable reason.
        payload_type: The ``message_type`` of the original payload, mirrored
            from ``original_message.meta.message_type`` for efficient
            top-level dispatch without nested decode.
        original_message: Wire-safe snapshot of the original message when
            available.
    """

    kind: ErrorKind
    reason: str
    payload_type: str | None = None
    original_message: ErrorMessage[PayloadT] | None = None


def snapshot_message(message: Message[PayloadT]) -> ErrorMessage[PayloadT]:
    """Capture a wire-safe snapshot of a streaming message.

    Args:
        message: Original transport-neutral message.

    Returns:
        A msgspec-compatible snapshot preserving payload and transport metadata.
    """

    return ErrorMessage(
        payload=message.payload,
        meta=ErrorMessageMeta(
            message_id=message.meta.message_id,
            correlation_id=message.meta.correlation_id,
            trace_id=message.meta.trace_id,
            parent_trace_id=message.meta.parent_trace_id,
            causation_id=message.meta.causation_id,
            produced_at_ms=message.meta.produced_at_ms,
            message_type=message.meta.message_type,
            message_version=message.meta.message_version,
            topic=message.meta.topic,
            partition=message.meta.partition,
            offset=message.meta.offset,
            key=_normalize_key(message.meta.key),
            headers=message.meta.headers,
        ),
    )


def _normalize_key(key: bytes | str | None) -> bytes | None:
    if key is None:
        return None
    if isinstance(key, bytes):
        return key
    return key.encode("utf-8")


__all__ = [
    "ErrorEnvelope",
    "ErrorKind",
    "ErrorMessage",
    "ErrorMessageMeta",
    "snapshot_message",
]
