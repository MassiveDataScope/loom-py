"""Message-level Kafka consumer with codec and envelope semantics."""

from __future__ import annotations

from time import perf_counter
from typing import Generic, Literal, TypeVar

from confluent_kafka import TopicPartition

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.core.observability.event import LifecycleEvent, Scope
from loom.core.observability.observability import ObservabilityRuntime
from loom.streaming.kafka._codec import KafkaCodec
from loom.streaming.kafka._errors import KafkaDeserializationError
from loom.streaming.kafka._message import HEADER_CORRELATION_ID, HEADER_TRACE_ID, MessageEnvelope
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._protocol import KafkaConsumer

PayloadT = TypeVar("PayloadT", bound=LoomStruct | LoomFrozenStruct)


class KafkaMessageConsumer(Generic[PayloadT]):
    """Polls raw bytes from a Kafka consumer and decodes them via codec.

    All dependencies are injected via constructor — the message consumer
    depends on the ``KafkaConsumer`` and ``KafkaCodec`` protocols, not on
    concrete implementations.

    Args:
        raw: Raw Kafka consumer for byte-level transport.
        codec: Codec for decoding bytes to message envelopes.
        payload_type: Expected payload model type for decoding.
        observer: Optional observability observer.
    """

    def __init__(
        self,
        raw: KafkaConsumer,
        codec: KafkaCodec[PayloadT],
        payload_type: type[PayloadT],
        obs: ObservabilityRuntime | None = None,
    ) -> None:
        self._raw = raw
        self._codec = codec
        self._payload_type = payload_type
        self._obs = obs

    def poll(self, timeout_ms: int) -> KafkaRecord[MessageEnvelope[PayloadT]] | None:
        """Read and decode one standard message envelope from Kafka.

        Args:
            timeout_ms: Maximum poll wait in milliseconds.

        Returns:
            One typed record carrying a decoded message envelope, or
            ``None`` when no record is available.

        Raises:
            KafkaPollError: If the backend poll fails.
            KafkaDeserializationError: If the envelope cannot be decoded.
        """
        record = self._raw.poll(timeout_ms)
        if record is None:
            return None
        started = perf_counter()
        try:
            message = self._codec.decode(record.value, self._payload_type)
        except Exception as exc:
            if self._obs is not None:
                self._obs.emit(
                    LifecycleEvent.exception(
                        scope=Scope.TRANSPORT,
                        name="kafka_decode",
                        trace_id=_header_trace_id(record.headers),
                        correlation_id=_header_correlation_id(record.headers),
                        error=str(exc),
                        meta={"topic": record.topic},
                    )
                )
            raise KafkaDeserializationError(str(exc)) from exc
        if self._obs is not None:
            self._obs.emit(
                LifecycleEvent.end(
                    scope=Scope.TRANSPORT,
                    name="kafka_decode",
                    duration_ms=(perf_counter() - started) * 1000,
                    trace_id=message.meta.trace_id,
                    correlation_id=message.meta.correlation_id,
                    meta={"content_type": message.meta.descriptor.content_type.media_type},
                )
            )
        return KafkaRecord(
            topic=record.topic,
            key=record.key,
            value=message,
            headers=record.headers,
            partition=record.partition,
            offset=record.offset,
            timestamp_ms=record.timestamp_ms,
        )

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets through the raw consumer.

        Args:
            asynchronous: Whether the backend may commit asynchronously.
        """
        self._raw.commit(asynchronous=asynchronous)

    def commit_offset(self, partitions: list[TopicPartition]) -> None:
        """Commit explicit offsets through the raw consumer.

        Args:
            partitions: Kafka topic-partition offsets to commit.
        """
        self._raw.commit_offset(partitions)

    def close(self) -> None:
        """Close the consumer and release resources."""
        self._raw.close()

    def __enter__(self) -> KafkaMessageConsumer[PayloadT]:
        """Return self for context-manager usage."""
        return self

    def __exit__(self, *exc: object) -> Literal[False]:
        """Close the consumer on context exit."""
        try:
            self.close()
        except Exception:
            if exc[0] is None:
                raise
        return False


def _header_trace_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_TRACE_ID)
    return raw.decode() if raw is not None else None


def _header_correlation_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_CORRELATION_ID)
    return raw.decode() if raw is not None else None
