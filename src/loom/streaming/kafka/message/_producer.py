"""Message-level Kafka producer with codec and envelope semantics."""

from __future__ import annotations

from time import perf_counter
from typing import Generic, Literal, TypeVar

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observability import Observability
from loom.streaming.kafka._codec import KafkaCodec
from loom.streaming.kafka._key_resolver import PartitionKeyResolver
from loom.streaming.kafka._message import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_PARENT_TRACE_ID,
    HEADER_TRACE_ID,
    MessageDescriptor,
    MessageEnvelope,
    build_message,
)
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._protocol import KafkaProducer

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


def _build_record_headers(
    headers: dict[str, bytes] | None,
    correlation_id: str | None,
    parent_trace_id: str | None,
    causation_id: str | None,
    trace_id: str | None,
) -> dict[str, bytes]:
    result: dict[str, bytes] = dict(headers) if headers else {}
    if correlation_id is not None:
        result[HEADER_CORRELATION_ID] = correlation_id.encode()
    if parent_trace_id is not None:
        result[HEADER_PARENT_TRACE_ID] = parent_trace_id.encode()
    if causation_id is not None:
        result[HEADER_CAUSATION_ID] = causation_id.encode()
    if trace_id is not None:
        result[HEADER_TRACE_ID] = trace_id.encode()
    return result


class KafkaMessageProducer(Generic[PayloadT]):
    """Builds a standard envelope, encodes via codec, delegates to a raw producer.

    All dependencies are injected via constructor — the message producer
    depends on the ``KafkaProducer`` and ``KafkaCodec`` protocols, not on
    concrete implementations.

    Args:
        raw: Raw Kafka producer for byte-level transport.
        codec: Codec for encoding message envelopes to bytes.
        key_resolver: Optional resolver used when ``send`` does not receive
            an explicit key.
        use_message_timestamp: Whether Kafka records should carry the
            envelope ``produced_at_ms`` timestamp.
        observer: Optional observability observer.
    """

    def __init__(
        self,
        raw: KafkaProducer,
        codec: KafkaCodec[PayloadT],
        key_resolver: PartitionKeyResolver[MessageEnvelope[PayloadT]] | None = None,
        use_message_timestamp: bool = True,
        obs: Observability | None = None,
    ) -> None:
        self._raw = raw
        self._codec = codec
        self._key_resolver = key_resolver
        self._use_message_timestamp = use_message_timestamp
        self._obs = obs

    def send(
        self,
        *,
        topic: str,
        payload: PayloadT,
        descriptor: MessageDescriptor,
        key: bytes | str | None = None,
        headers: dict[str, bytes] | None = None,
        correlation_id: str | None = None,
        parent_trace_id: str | None = None,
        causation_id: str | None = None,
        trace_id: str | None = None,
        produced_at_ms: int | None = None,
    ) -> None:
        """Build, encode, and produce one standard message envelope.

        Args:
            topic: Target Kafka topic.
            payload: Typed payload body.
            descriptor: Stable message descriptor.
            key: Optional Kafka partition key.
            headers: Optional Kafka headers.
            correlation_id: Optional correlation identifier.
            parent_trace_id: Optional upstream trace identifier.
            causation_id: Optional upstream message identifier.
            trace_id: Optional explicit trace identifier.
            produced_at_ms: Optional producer timestamp in epoch milliseconds.

        Raises:
            KafkaSerializationError: If the envelope cannot be encoded.
            KafkaDeliveryError: If Kafka rejects the produce call.
        """
        message = build_message(
            payload,
            descriptor,
            correlation_id=correlation_id,
            parent_trace_id=parent_trace_id,
            causation_id=causation_id,
            trace_id=trace_id,
            produced_at_ms=produced_at_ms,
        )
        record_headers = _build_record_headers(
            headers,
            correlation_id,
            parent_trace_id,
            causation_id,
            trace_id,
        )
        encode_started = perf_counter()
        encoded = self._codec.encode(message)
        if self._obs is not None:
            self._obs.emit(
                LifecycleEvent.end(
                    scope=Scope.TRANSPORT,
                    name="kafka_encode",
                    duration_ms=(perf_counter() - encode_started) * 1000,
                    trace_id=message.meta.trace_id,
                    correlation_id=message.meta.correlation_id,
                    meta={"content_type": descriptor.content_type.media_type},
                )
            )
        resolved_key: bytes | str | None = key
        if resolved_key is None and self._key_resolver is not None:
            resolved_key = self._key_resolver.resolve(
                KafkaRecord(
                    topic=topic,
                    key=None,
                    value=message,
                    headers=record_headers,
                    timestamp_ms=message.meta.produced_at_ms,
                )
            )
        self._raw.send(
            KafkaRecord(
                topic=topic,
                key=resolved_key,
                value=encoded,
                headers=record_headers,
                timestamp_ms=message.meta.produced_at_ms if self._use_message_timestamp else None,
            )
        )

    def flush(self, timeout_ms: int | None = None) -> None:
        """Flush pending records.

        Args:
            timeout_ms: Optional maximum flush wait in milliseconds.

        Raises:
            KafkaDeliveryError: If flush fails or pending delivery errors
                remain.
        """
        self._raw.flush(timeout_ms)

    def close(self) -> None:
        """Flush and close the producer.

        Raises:
            KafkaDeliveryError: If pending delivery failures remain.
        """
        self._raw.close()

    def __enter__(self) -> KafkaMessageProducer[PayloadT]:
        """Return self for context-manager usage."""
        return self

    def __exit__(self, *exc: object) -> Literal[False]:
        """Flush and close the producer on context exit."""
        try:
            self.close()
        except Exception:
            if exc[0] is None:
                raise
        return False
