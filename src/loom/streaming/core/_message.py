"""Transport-neutral streaming message model."""

from __future__ import annotations

from typing import Generic, TypeVar

import msgspec
from typing_extensions import TypeAliasType

from loom.core.fields import LogKey, Scalar
from loom.core.logger import LogField, SpanAttributes
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.log_events import StreamingLogField

StreamPayload = TypeAliasType("StreamPayload", LoomStruct | LoomFrozenStruct)
"""Union of all valid streaming payload types."""

PayloadT = TypeVar("PayloadT", bound=StreamPayload, covariant=True)


class MessageMeta(LoomFrozenStruct, frozen=True, kw_only=True):
    """Transport-neutral metadata for one logical event.

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
        key: Optional transport key when available.
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
    key: bytes | str | None = None
    headers: dict[str, bytes] = msgspec.field(default_factory=dict)

    def span_attributes(self) -> SpanAttributes:
        """The ``loom.*`` span attributes this message carries, only those that are set.

        A span never advertises an empty lineage.
        """
        optional: dict[LogKey, Scalar | None] = {
            StreamingLogField.MESSAGE_ID: self.message_id,
            StreamingLogField.PARENT_TRACE_ID: self.parent_trace_id,
            StreamingLogField.CAUSATION_ID: self.causation_id,
            StreamingLogField.MESSAGE_TYPE: self.message_type,
            LogField.MESSAGING_DESTINATION_NAME: self.topic,
            StreamingLogField.KAFKA_PARTITION: self.partition,
            StreamingLogField.KAFKA_OFFSET: self.offset,
        }
        return {key: value for key, value in optional.items() if value is not None}


class Message(LoomFrozenStruct, Generic[PayloadT], frozen=True, kw_only=True):
    """Logical typed streaming event.

    Args:
        payload: Typed event payload.
        meta: Transport-neutral metadata.
    """

    payload: PayloadT
    meta: MessageMeta


__all__ = ["Message", "MessageMeta", "StreamPayload"]
