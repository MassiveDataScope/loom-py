"""Raw Kafka consumer backed by confluent-kafka."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any, Literal, Protocol, cast

from confluent_kafka import Consumer as _Consumer
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka import Message as _RawMessage

from loom.core.errors import InternalError
from loom.core.log_names import LogField
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.streaming.event import StreamingEvent, StreamingKey
from loom.streaming.kafka._config import ConsumerSettings
from loom.streaming.kafka._errors import KafkaCommitError, KafkaPollError
from loom.streaming.kafka._message import HEADER_CORRELATION_ID, HEADER_TRACE_ID
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka.client._failure import failure_message
from loom.streaming.kafka.client._retry import (
    DEFAULT_COORDINATOR_RETRY,
    CoordinatorRetryPolicy,
    with_coordinator_retry,
)

logger = logging.getLogger(__name__)


class _CommitMethod(Protocol):
    def __call__(
        self,
        *,
        asynchronous: bool = ...,
        offsets: list[TopicPartition] | None = ...,
    ) -> object: ...


class KafkaConsumerClient:
    """Confluent-backed raw Kafka consumer.

    Returns ``KafkaRecord[bytes]`` from Kafka. No deserialization —
    values are raw bytes as received from the broker.

    Args:
        settings: Typed consumer settings.
        obs: Optional observability.
        retry_policy: Backoff schedule for transient group-coordinator errors
            on offset fetch and commit.
    """

    def __init__(
        self,
        settings: ConsumerSettings,
        obs: Observability | None = None,
        *,
        retry_policy: CoordinatorRetryPolicy = DEFAULT_COORDINATOR_RETRY,
        _subscribe: bool = True,
    ) -> None:
        self._obs = obs
        self._retry_policy = retry_policy
        config: dict[str, Any] = dict(settings.to_confluent_config())
        config["on_commit"] = self._on_commit
        self._consumer = _Consumer(config)
        if _subscribe:
            self._consumer.subscribe(list(settings.topics))

    @classmethod
    def unassigned(
        cls,
        settings: ConsumerSettings,
        observability: Observability | None = None,
        *,
        retry_policy: CoordinatorRetryPolicy = DEFAULT_COORDINATOR_RETRY,
    ) -> KafkaConsumerClient:
        """Build a consumer with neither subscription nor assignment.

        Used to query the group coordinator (:meth:`committed_offset`) before
        deciding the start offset, then pin the partition with
        :meth:`assign_partition`.

        Args:
            settings: Typed consumer settings.
            observability: Optional observability.
            retry_policy: Backoff schedule for transient group-coordinator
                errors.

        Returns:
            Consumer client not yet attached to any partition.
        """
        return cls(settings, observability, retry_policy=retry_policy, _subscribe=False)

    def assign_partition(self, topic: str, partition: int, offset: int) -> None:
        """Pin this consumer to exactly one topic partition via ``assign``.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.
            offset: Start offset (a concrete offset or a confluent sentinel
                such as ``OFFSET_BEGINNING``/``OFFSET_END``).
        """
        self._consumer.assign([TopicPartition(topic, partition, offset)])

    def committed_offset(self, topic: str, partition: int, *, timeout_ms: int) -> int | None:
        """Read the consumer group's committed offset for one partition.

        Works without group membership (plain ``OffsetFetch`` to the group
        coordinator), so it is safe on assign-mode and unassigned consumers.

        Args:
            topic: Physical topic name.
            partition: Kafka partition index.
            timeout_ms: Explicit coordinator timeout — a coordinator that does
                not answer is a hard error, never a silent fallback.

        Returns:
            The committed offset, or ``None`` when the group has no valid
            committed offset for the partition.

        Raises:
            KafkaCommitError: If the offset fetch fails or times out.
        """
        try:
            results = with_coordinator_retry(
                lambda: self._consumer.committed(
                    [TopicPartition(topic, partition)], timeout=timeout_ms / 1000
                ),
                policy=self._retry_policy,
            )
        except Exception as exc:
            raise KafkaCommitError(failure_message(exc)) from exc
        if not results:
            return None
        result = results[0]
        if result.error is not None:
            raise KafkaCommitError(failure_message(result.error))
        offset = result.offset
        if offset is None or offset < 0:
            return None
        return int(offset)

    def poll(self, timeout_ms: int) -> KafkaRecord[bytes] | None:
        """Read one raw byte record from Kafka.

        Args:
            timeout_ms: Maximum poll wait in milliseconds.

        Returns:
            One raw Kafka record or ``None`` when no record is available.

        Raises:
            KafkaPollError: If the backend poll fails or returns a broker
                error.
        """
        try:
            message = self._consumer.poll(timeout_ms / 1000)
        except Exception as exc:
            raise KafkaPollError(failure_message(exc)) from exc
        if message is None:
            return None
        record = _checked_record(message)
        self._observe_birth(record)
        return record

    def consume_batch(self, max_records: int) -> list[KafkaRecord[bytes]]:
        """Read up to ``max_records`` raw byte records without blocking.

        Uses a negligible backend timeout, so the call returns whatever the
        consumer already buffered.  Record order is the broker order per
        partition.  Compacted-topic tombstones (records with a ``None``
        value) are skipped: they carry no payload to decode, and the
        gap-tolerant commit watermark treats unregistered offsets as gaps,
        so skipping never freezes commits.

        Args:
            max_records: Maximum number of records to return.

        Returns:
            Raw Kafka records; empty when nothing is available.

        Raises:
            KafkaPollError: If the backend consume fails or any message
                carries a broker error.
        """
        try:
            messages = self._consumer.consume(max_records, timeout=0.001)
        except Exception as exc:
            raise KafkaPollError(failure_message(exc)) from exc
        records: list[KafkaRecord[bytes]] = []
        for message in messages:
            error = message.error()
            if error is not None:
                raise KafkaPollError(failure_message(error))
            if message.value() is None:
                continue
            record = _to_record(message)
            self._observe_birth(record)
            records.append(record)
        return records

    def _observe_birth(self, record: KafkaRecord[bytes]) -> None:
        """Open and close the span where one message enters the system.

        This is the message's birth: the first span of its trace, opened in the
        trace id the producer put on the wire, so the story of this message
        continues the story of whatever produced it.

        The span is a root, and its trace id comes from the Kafka header rather
        than from the ambient context, which is what makes a trace continuous
        across services.
        """
        if self._obs is None:
            return
        self._obs.open_span(
            Scope.TRANSPORT,
            "kafka_consume",
            trace_id=_header_trace_id(record.headers),
            correlation_id=_header_correlation_id(record.headers),
            root=True,
            topic=record.topic,
        ).end()

    def commit(self, *, asynchronous: bool = False) -> None:
        """Commit consumed offsets.

        Args:
            asynchronous: Whether the backend may commit asynchronously.

        Raises:
            KafkaCommitError: If the backend commit fails.
        """
        try:
            commit = cast(_CommitMethod, self._consumer.commit)
            with_coordinator_retry(
                lambda: commit(asynchronous=asynchronous),
                policy=self._retry_policy,
            )
        except Exception as exc:
            raise KafkaCommitError(failure_message(exc)) from exc

    def commit_offset(
        self,
        partitions: list[TopicPartition],
        *,
        asynchronous: bool = False,
    ) -> None:
        """Commit explicit Kafka offsets.

        Args:
            partitions: Kafka topic-partition offsets to commit.
            asynchronous: When ``True``, librdkafka coalesces the commit in
                the background and this call returns before the broker
                answers, so a rejection surfaces through :meth:`_on_commit`
                rather than as an exception raised here.

        Raises:
            KafkaCommitError: If the backend commit fails (synchronous mode).
        """
        try:
            commit = cast(_CommitMethod, self._consumer.commit)
            with_coordinator_retry(
                lambda: commit(offsets=partitions, asynchronous=asynchronous),
                policy=self._retry_policy,
            )
        except Exception as exc:
            raise KafkaCommitError(failure_message(exc)) from exc

    def _on_commit(self, error: KafkaError | None, partitions: list[TopicPartition]) -> None:
        """Report the outcome of an asynchronous offset commit.

        Asynchronous commits return before the broker answers, so a rejection
        can only arrive here. Without this callback such a failure was
        discarded entirely: the offset silently stayed where it was and the
        only visible symptom was consumer lag with no explanation.

        This does not weaken delivery. A commit that never lands means the
        group offset does not advance, so those records are reprocessed on the
        next run — which is what at-least-once permits. The retention
        keep-alive re-commits the watermark later, so the failure is transient
        as long as it is *visible*, which is what this restores.
        """
        if error is None:
            return
        logger.error(
            StreamingEvent.OFFSET_COMMIT_FAILED,
            extra={
                LogField.ERROR_CODE: error.name(),
                StreamingKey.PARTITIONS: [
                    f"{part.topic}:{part.partition}@{part.offset}" for part in partitions
                ],
            },
        )

    def close(self) -> None:
        """Close the consumer and release resources."""
        self._consumer.close()

    def __enter__(self) -> KafkaConsumerClient:
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


def _checked_record(message: _RawMessage) -> KafkaRecord[bytes]:
    """Translate one confluent message, raising on broker-reported errors."""
    error = message.error()
    if error is not None:
        raise KafkaPollError(failure_message(error))
    return _to_record(message)


def _to_record(message: _RawMessage) -> KafkaRecord[bytes]:
    value_bytes = message.value()
    if value_bytes is None:
        raise InternalError("KafkaConsumerClient received a record without a value")
    _, timestamp_ms = message.timestamp()
    headers = _normalize_headers(message.headers())
    key = message.key()
    normalized_key: bytes | str | None = None if key is None else key
    topic = message.topic()
    if topic is None:
        raise InternalError("KafkaConsumerClient received a record without a topic")
    return KafkaRecord(
        topic=topic,
        key=normalized_key,
        value=value_bytes,
        headers=headers,
        partition=message.partition(),
        offset=message.offset(),
        timestamp_ms=timestamp_ms if timestamp_ms >= 0 else None,
    )


def _header_trace_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_TRACE_ID)
    return raw.decode() if raw is not None else None


def _header_correlation_id(headers: dict[str, bytes]) -> str | None:
    raw = headers.get(HEADER_CORRELATION_ID)
    return raw.decode() if raw is not None else None


def _normalize_headers(
    raw_headers: Mapping[str, str | bytes | None] | Iterable[tuple[str, str | bytes | None]] | None,
) -> dict[str, bytes]:
    """Return Kafka headers without tombstone header values."""
    headers: dict[str, bytes] = {}
    if raw_headers is None:
        return headers
    if isinstance(raw_headers, Mapping):
        iterable = cast(Iterable[tuple[str, str | bytes | None]], raw_headers.items())
    else:
        iterable = raw_headers
    for header_key, header_value in iterable:
        if header_value is not None:
            headers[header_key] = (
                header_value if isinstance(header_value, bytes) else header_value.encode("utf-8")
            )
    return headers
