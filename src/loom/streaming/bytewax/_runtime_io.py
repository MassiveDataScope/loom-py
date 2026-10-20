"""Runtime Bytewax source and sink adapters for Loom streaming flows."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep, time_ns
from typing import Any, Generic, TypeAlias, TypeVar, cast

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, KafkaError
from confluent_kafka.admin import AdminClient

from loom.core.observability.event import Scope, TerminalReason
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.core.tracing import generate_trace_id
from loom.streaming.bytewax._batch_spans import BatchSpan, BatchWindow, emit_batch_spans
from loom.streaming.bytewax._commit_tracker import CommitCompletionPort, KafkaCommitTracker
from loom.streaming.compiler._plan import (
    CompiledMongoCDCSource,
    CompiledMultiSource,
    CompiledSingleSource,
    CompiledSink,
    CompiledSource,
)
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.core._tracing import open_terminal_span
from loom.streaming.core._typing import StreamPayload
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._config import ConsumerSettings
from loom.streaming.kafka._errors import KafkaDeliveryError, KafkaPollError
from loom.streaming.kafka._message import (
    HEADER_CAUSATION_ID,
    HEADER_CORRELATION_ID,
    HEADER_PARENT_TRACE_ID,
    HEADER_TRACE_ID,
    MessageDescriptor,
)
from loom.streaming.kafka._record import KafkaRecord
from loom.streaming.kafka._wire import DecodeError
from loom.streaming.kafka.client._consumer import KafkaConsumerClient
from loom.streaming.kafka.client._producer import KafkaProducerClient
from loom.streaming.kafka.message._producer import KafkaMessageProducer
from loom.streaming.mongo._bytewax_source import MongoCDCSource
from loom.streaming.nodes._boundary import PartitionPolicy

logger = logging.getLogger(__name__)
ItemT = TypeVar("ItemT")
PayloadT = TypeVar("PayloadT", bound=StreamPayload)
_CompiledKafkaSource: TypeAlias = CompiledSingleSource | CompiledMultiSource


@dataclass(frozen=True)
class _KafkaSendRequest(Generic[PayloadT]):
    """Resolved Kafka send parameters for one runtime item."""

    payload: PayloadT
    descriptor: MessageDescriptor
    key: bytes | str | None
    headers: dict[str, bytes]
    correlation_id: str | None
    parent_trace_id: str | None
    causation_id: str | None
    trace_id: str | None
    produced_at_ms: int | None


_DlqLineage: TypeAlias = Callable[[ItemT, _KafkaSendRequest[PayloadT]], _KafkaSendRequest[PayloadT]]
"""Adjusts the trace lineage of one DLQ record derived from the sink's send request."""


@dataclass(frozen=True, slots=True)
class _BatchOutcome:
    """How one Kafka batch write ended, for the two exits that return normally.

    A delivery failure diverted to a DLQ topic returns exactly like a delivered
    batch once the DLQ topic has received every record: it commits and raises
    nothing. The return value is therefore the only thing that lets a caller
    above ``_write_kafka_batch`` tell the two apart — and a diversion is a
    failure, because the batch never reached its outbound topic.

    Args:
        error: Delivery failure that caused the diversion, or ``None`` when the
            batch was delivered.
        dlq_topic: Topic the batch was diverted to, or ``None``.
    """

    error: KafkaDeliveryError | None = None
    dlq_topic: str | None = None


_DELIVERED = _BatchOutcome()
"""The outcome of a batch that flushed without a delivery failure."""


def _write_kafka_batch(
    *,
    sink: CompiledSink,
    producer: KafkaProducerClient,
    message_producer: KafkaMessageProducer[PayloadT],
    items: list[ItemT],
    item_to_send: Callable[[ItemT], _KafkaSendRequest[PayloadT]],
    item_to_commit: Callable[[ItemT], tuple[str | None, int | None, int | None]] | None,
    commit_tracker: CommitCompletionPort | None,
    dlq_lineage: _DlqLineage[ItemT, PayloadT] | None = None,
) -> _BatchOutcome:
    try:
        for item in items:
            _send_request(message_producer, sink.topic, item_to_send(item))
        producer.flush()
        _commit_runtime_items(items, item_to_commit, commit_tracker)
    except KafkaDeliveryError as exc:
        if sink.dlq_topic is None:
            raise
        dlq_error = str(exc).encode("utf-8")
        try:
            for item in items:
                request = item_to_send(item)
                if dlq_lineage is not None:
                    request = dlq_lineage(item, request)
                headers = {**request.headers, "x-dlq-error": dlq_error}
                _send_request(message_producer, sink.dlq_topic, replace(request, headers=headers))
            producer.flush()
        except KafkaDeliveryError as dlq_exc:
            raise KafkaDeliveryError(
                f"delivery failed after diverting the batch to DLQ topic {sink.dlq_topic!r} "
                f"(sink delivery error: {exc})"
            ) from dlq_exc
        _commit_runtime_items(items, item_to_commit, commit_tracker)
        return _BatchOutcome(error=exc, dlq_topic=sink.dlq_topic)
    return _DELIVERED


def _send_request(
    message_producer: KafkaMessageProducer[PayloadT],
    topic: str,
    request: _KafkaSendRequest[PayloadT],
) -> None:
    message_producer.send(
        topic=topic,
        payload=request.payload,
        descriptor=request.descriptor,
        key=request.key,
        headers=request.headers,
        correlation_id=request.correlation_id,
        parent_trace_id=request.parent_trace_id,
        causation_id=request.causation_id,
        trace_id=request.trace_id,
        produced_at_ms=request.produced_at_ms,
    )


def _keep_message_lineage(
    message: Message[StreamPayload],
    request: _KafkaSendRequest[StreamPayload],
) -> _KafkaSendRequest[StreamPayload]:
    """Keep the failed message's own trace on its DLQ record instead of a child trace."""
    return replace(
        request,
        trace_id=message.meta.trace_id,
        parent_trace_id=message.meta.parent_trace_id,
    )


def _commit_runtime_items(
    items: list[ItemT],
    item_to_commit: Callable[[ItemT], tuple[str | None, int | None, int | None]] | None,
    commit_tracker: CommitCompletionPort | None,
) -> None:
    if commit_tracker is None or item_to_commit is None:
        return
    for item in items:
        topic, partition, offset = item_to_commit(item)
        if topic is not None and partition is not None and offset is not None:
            commit_tracker.complete(topic, partition, offset)


_COMMITTED_FETCH_TIMEOUT_MS = 10_000
_METADATA_FETCH_TIMEOUT_S = 10.0
# Cluster metadata converges a moment after a topic is created or a leader is
# elected, so a source that starts in that window retries instead of dying.
_METADATA_VISIBILITY_TIMEOUT_S = 15.0
_METADATA_RETRY_INTERVAL_S = 0.25
_RETRIABLE_METADATA_CODES = frozenset(
    {KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.LEADER_NOT_AVAILABLE}
)
_DEFAULT_POLL_TIMEOUT_MS = 100
_ADMIN_KEY_PREFIXES = ("security.", "sasl.", "ssl.", "enable.ssl.")


def _admin_config(source: _CompiledKafkaSource) -> dict[str, Any]:
    """Extract the broker/security subset of the consumer config for AdminClient.

    Only ``bootstrap.servers`` plus security-related keys are copied; other
    ``extra`` consumer keys are consumer-specific and never reach the
    metadata client.
    """
    full = source.settings.to_confluent_config()
    admin: dict[str, Any] = {"bootstrap.servers": full["bootstrap.servers"]}
    for key, value in full.items():
        if key.startswith(_ADMIN_KEY_PREFIXES):
            admin[key] = value
    return admin


def _warn_on_stale_resume_state(
    topic: str,
    partition: int,
    resume_state: int | None,
    committed: int | None,
) -> None:
    """Warn when a recovery snapshot is behind the committed group offset.

    Not an error: the replay is safe because commits below the floor are
    suppressed. It is worth a warning because the records between the two
    offsets are reprocessed, which is visible as duplicate downstream effects.
    """
    if resume_state is None or committed is None or resume_state >= committed:
        return
    logger.warning(
        "resume state %d is behind the committed group offset %d for %s:%d "
        "(distance %d): replaying from the recovery snapshot; commits below "
        "the floor are suppressed",
        resume_state,
        committed,
        topic,
        partition,
        committed - resume_state,
    )


class KafkaPartitionedSource(FixedPartitionedSource[KafkaRecord[bytes], "int | None"]):
    """One Bytewax input partition per Kafka partition.

    Partition keys are ``"{topic}:{index}"`` — a durable contract: they are
    also the Bytewax recovery-state keys, so the format must stay stable
    (``:`` is not a legal topic character; parse with ``rpartition``).

    Consumers are created lazily per partition inside :meth:`build_part` and
    pinned with ``assign`` — no ``subscribe``, no group membership: the
    consumer group acts purely as an offset store. Start-offset precedence::

        resume_state (Bytewax recovery) > committed group offset > auto_offset_reset
    """

    def __init__(
        self,
        source: _CompiledKafkaSource,
        commit_tracker: KafkaCommitTracker | None = None,
        observability: ObservabilityRuntime | None = None,
    ) -> None:
        self._source = source
        self._commit_tracker = commit_tracker
        self._observability = observability
        if source.settings.poll_timeout_ms != _DEFAULT_POLL_TIMEOUT_MS:
            logger.warning(
                "kafka consumer poll_timeout_ms is deprecated for the partitioned "
                "source and ignored; use batch_size and poll_backoff_ms instead"
            )

    def list_parts(self) -> list[str]:
        """List one partition key per Kafka partition of every source topic.

        Note: with ``auto.create.topics.enable=true`` on the broker, asking
        for a missing topic may auto-create it (confluent metadata caveat).
        """
        admin = AdminClient(_admin_config(self._source))
        parts: list[str] = []
        for topic in self._source.topics:
            topic_meta = self._await_topic_metadata(admin, topic)
            parts.extend(f"{topic}:{index}" for index in sorted(topic_meta.partitions))
        return parts

    def _await_topic_metadata(self, admin: AdminClient, topic: str) -> Any:
        """Return a topic's metadata once the cluster reports it without error.

        A topic that is absent or has no leader yet is fetched again until the
        visibility timeout expires. Broker metadata converges after a topic is
        created, after a leader election and after a restart, so one fetch can
        answer a question the cluster has not settled yet. Any other error is
        raised immediately, because no amount of waiting resolves it.
        """
        deadline = monotonic() + _METADATA_VISIBILITY_TIMEOUT_S
        warned = False
        while True:
            metadata = admin.list_topics(topic, timeout=_METADATA_FETCH_TIMEOUT_S)
            topic_meta = metadata.topics.get(topic)
            if topic_meta is not None and topic_meta.error is None:
                return topic_meta

            error = topic_meta.error if topic_meta is not None else None
            reason = error if error is not None else "topic not found"
            if error is not None and error.code() not in _RETRIABLE_METADATA_CODES:
                raise KafkaPollError(f"cannot list partitions for topic '{topic}': {reason}")
            if monotonic() >= deadline:
                raise KafkaPollError(
                    f"cannot list partitions for topic '{topic}' after "
                    f"{_METADATA_VISIBILITY_TIMEOUT_S:g}s: {reason}"
                )
            if not warned:
                logger.warning(
                    "kafka topic '%s' is not visible in cluster metadata yet: %s; "
                    "retrying until %gs elapse",
                    topic,
                    reason,
                    _METADATA_VISIBILITY_TIMEOUT_S,
                )
                warned = True
            sleep(_METADATA_RETRY_INTERVAL_S)

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: int | None,
    ) -> _KafkaSourcePartition:
        """Build one partition: lazy consumer, assign, floor, tracker binding."""
        del step_id
        topic, partition = self._parse_partition_key(for_part)
        client = KafkaConsumerClient.unassigned(self._source.settings, self._observability)
        committed = client.committed_offset(
            topic, partition, timeout_ms=_COMMITTED_FETCH_TIMEOUT_MS
        )
        _warn_on_stale_resume_state(topic, partition, resume_state, committed)
        client.assign_partition(topic, partition, self._start_offset(resume_state, committed))
        if self._commit_tracker is not None:
            self._commit_tracker.attach_partition(topic, partition, client, committed)
        return _KafkaSourcePartition(
            client=client,
            topic=topic,
            partition=partition,
            commit_tracker=self._commit_tracker,
            settings=self._source.settings,
            start_position=resume_state if resume_state is not None else committed,
        )

    def _parse_partition_key(self, for_part: str) -> tuple[str, int]:
        """Split a ``"{topic}:{index}"`` recovery key and reject foreign topics.

        Raises:
            ValueError: If the key names a topic this flow does not read.
        """
        topic, _, raw_index = for_part.rpartition(":")
        if topic not in self._source.topics:
            raise ValueError(
                f"partition key '{for_part}' does not belong to this flow's topics "
                f"{self._source.topics}; cannot resume from a different topic set"
            )
        return topic, int(raw_index)

    def _start_offset(self, resume_state: int | None, committed: int | None) -> int:
        if resume_state is not None:
            return resume_state
        if committed is not None:
            return committed
        reset = self._source.settings.auto_offset_reset
        return int(OFFSET_BEGINNING) if reset == "earliest" else int(OFFSET_END)

    def bind_commit_tracker(self, commit_tracker: KafkaCommitTracker | None) -> None:
        """Bind or clear the source commit tracker (adapter duck-typed hook)."""
        self._commit_tracker = commit_tracker


class _KafkaSourcePartition(StatefulSourcePartition[KafkaRecord[bytes], "int | None"]):
    """One assigned Kafka partition: batch consume, register, coalesced flush."""

    def __init__(
        self,
        *,
        client: KafkaConsumerClient,
        topic: str,
        partition: int,
        commit_tracker: KafkaCommitTracker | None,
        settings: ConsumerSettings,
        start_position: int | None = None,
    ) -> None:
        self._client = client
        self._topic = topic
        self._partition = partition
        self._commit_tracker = commit_tracker
        self._batch_size = settings.batch_size
        self._backoff = timedelta(milliseconds=settings.poll_backoff_ms)
        self._keepalive = timedelta(milliseconds=settings.commit_keepalive_ms)
        self._next_awake: datetime | None = None
        # Seeded so an epoch with no data snapshots the true read position
        # instead of overwriting a prior resume_state with None.
        self._next_offset: int | None = start_position
        self._last_commit_at = datetime.now(UTC)

    def next_batch(self) -> list[KafkaRecord[bytes]]:
        """Return buffered records; the batch is validated before registering."""
        records = self._client.consume_batch(self._batch_size)
        now = datetime.now(UTC)
        if records:
            if self._commit_tracker is not None:
                for record in records:
                    self._commit_tracker.register_record(record)
            last_offset = records[-1].offset
            if last_offset is not None:
                self._next_offset = last_offset + 1
            self._next_awake = None
        else:
            self._next_awake = now + self._backoff
        self._flush_commits(now)
        return records

    def _flush_commits(self, now: datetime) -> None:
        """Commit the coalesced watermark, or re-commit it as a keep-alive."""
        if self._commit_tracker is None:
            return
        if now - self._last_commit_at >= self._keepalive:
            self._commit_tracker.keepalive_partition(self._topic, self._partition)
            self._last_commit_at = now
            return
        if self._commit_tracker.flush_partition(self._topic, self._partition):
            self._last_commit_at = now

    def next_awake(self) -> datetime | None:
        return self._next_awake

    def snapshot(self) -> int | None:
        return self._next_offset

    def close(self) -> None:
        """Flush the final watermark, then release the consumer unconditionally.

        The final flush is synchronous and deliberately re-raises: a broker that
        rejects the closing commit must not fail silently. It must not leak the
        consumer either — without the ``finally``, a broker that is down at
        shutdown leaves the client, its sockets and its group state alive.
        """
        try:
            if self._commit_tracker is not None:
                self._commit_tracker.close_partition(self._topic, self._partition)
        finally:
            self._client.close()


class _KafkaSinkPartitionBase:
    """Shared Kafka producer lifecycle and commit-tracker management."""

    def __init__(
        self,
        sink: CompiledSink,
        commit_tracker: CommitCompletionPort | None = None,
    ) -> None:
        self._sink = sink
        self._producer = KafkaProducerClient(sink.settings)
        self._message_producer: KafkaMessageProducer[Any] = KafkaMessageProducer(
            self._producer, MsgspecCodec()
        )
        self._commit_tracker = commit_tracker

    def close(self) -> None:
        self._message_producer.close()

    def bind_commit_tracker(self, tracker: CommitCompletionPort | None) -> None:
        self._commit_tracker = tracker


class _KafkaMessageSinkPartition(
    _KafkaSinkPartitionBase, StatelessSinkPartition[Message[StreamPayload]]
):
    """Write typed runtime messages to Kafka."""

    _message_producer: KafkaMessageProducer[StreamPayload]

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        self.write_batch_outcome(items)

    def write_batch_outcome(self, items: list[Message[StreamPayload]]) -> _BatchOutcome:
        """Write one batch to the outbound topic and report how it ended.

        ``StatelessSinkPartition.write_batch`` must return ``None``, so this
        sibling exists to carry the outcome to a wrapper that needs it — a DLQ
        diversion returns normally and is otherwise indistinguishable from a
        delivered batch.

        Args:
            items: Messages Bytewax delivered for the current epoch.

        Returns:
            The outcome of the write.

        Raises:
            KafkaDeliveryError: If delivery failed and either no DLQ topic is
                declared or the DLQ topic did not receive every record.
        """
        return _write_kafka_batch(
            sink=self._sink,
            producer=self._producer,
            message_producer=self._message_producer,
            items=items,
            item_to_send=lambda msg: _message_to_send_with_policy(msg, self._sink.partition_policy),
            item_to_commit=_message_to_commit,
            commit_tracker=self._commit_tracker,
            dlq_lineage=_keep_message_lineage,
        )


@dataclass(frozen=True, slots=True)
class _TerminalTracing:
    """Everything the terminal span of an outbound-topic write needs.

    Bound once, at wiring time, by the adapter sites that run only for a real
    terminal — never for the inline ``WithAsync`` sink, whose messages go on
    living.

    Args:
        observer: Runtime the terminal and batch spans are opened on.
        flow: Name of the enclosing flow.
        flow_run_id: Identifier of the flow run the spans belong to.
    """

    observer: ObservabilityRuntime
    flow: str
    flow_run_id: str


def _terminal_failure_attributes(dlq_topic: str | None) -> dict[str, object]:
    """Return the attributes stamped on terminal spans of a failed batch.

    ``terminal.failure_scope`` is stamped on failure only. ``KafkaProducerClient``
    keeps one pending delivery error and ``flush()`` raises it for the whole
    batch, so one bad record fails all N spans and the attribute says so. On
    success ``flush()`` waited for every record and raised nothing, which makes
    success per-message truth that needs no qualification.
    """
    attributes: dict[str, object] = {"terminal.failure_scope": "batch"}
    if dlq_topic is not None:
        attributes["terminal.dlq_topic"] = dlq_topic
    return attributes


class _TracedMessageSinkPartition(StatelessSinkPartition[Message[StreamPayload]]):
    """Record an outbound-topic write as the death of every message in it.

    Wraps :class:`_KafkaMessageSinkPartition` and emits the N+1 shape of
    :mod:`loom.streaming.bytewax._batch_spans`: one ``terminal:sink_write`` span
    per message, in that message's own trace, plus one ``Scope.WRITE`` span for
    the flush itself, in a trace of its own, linking back to all of them.

    A flow ending in ``IntoTopic`` used to produce a trace that stopped at its
    last node span. Wrapping rather than branching inside the write keeps the
    untraced path — the framework's highest-throughput path — free of the extra
    branch: an untraced sink builds the bare partition instead.

    Args:
        partition: Kafka partition that performs the write.
        tracing: Runtime and flow identity the spans are opened with.
        topic: Outbound topic, reported as the sink name on every span.
    """

    def __init__(
        self,
        partition: _KafkaMessageSinkPartition,
        tracing: _TerminalTracing,
        *,
        topic: str,
    ) -> None:
        self._partition = partition
        self._tracing = tracing
        self._topic = topic

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        """Write the batch, then close every message's trace with its death.

        Empty epochs are forwarded without a span, so an idle flow emits
        nothing. A batch diverted to a DLQ closes *failed*: it never reached
        its outbound topic.

        Args:
            items: Messages Bytewax delivered for the current epoch.

        Raises:
            Exception: Whatever the underlying partition raises, after the
                failure has been recorded on every span of the batch.
        """
        if not items:
            self._partition.write_batch(items)
            return
        started_ns = time_ns()
        try:
            outcome = self._partition.write_batch_outcome(items)
        except Exception as exc:
            self._emit_spans(items, BatchWindow.since(started_ns), exc, None)
            raise
        self._emit_spans(items, BatchWindow.since(started_ns), outcome.error, outcome.dlq_topic)

    def _emit_spans(
        self,
        items: list[Message[StreamPayload]],
        window: BatchWindow,
        error: BaseException | None,
        dlq_topic: str | None,
    ) -> None:
        """Emit the per-message terminal spans and the batch write span."""
        failure = _terminal_failure_attributes(dlq_topic) if error is not None else {}

        def open_terminal(
            meta: MessageMeta,
            batch_attributes: Mapping[str, object],
            started_ns: int,
        ) -> LoomSpan:
            return open_terminal_span(
                self._tracing.observer,
                meta,
                TerminalReason.SINK_WRITE,
                start_time_ns=started_ns,
                attributes={
                    "flow": self._tracing.flow,
                    "sink": self._topic,
                    **failure,
                    **batch_attributes,
                },
            )

        emit_batch_spans(
            self._tracing.observer,
            [item.meta for item in items],
            batch=BatchSpan(
                scope=Scope.WRITE,
                name=f"{self._tracing.flow}:{self._topic}",
                attributes={
                    "flow": self._tracing.flow,
                    "sink": self._topic,
                    "batch_size": len(items),
                    "loom.flow_run_id": self._tracing.flow_run_id,
                },
            ),
            open_participation=open_terminal,
            window=window,
            error=error,
        )

    def close(self) -> None:
        """Delegate close to the wrapped partition."""
        self._partition.close()


class _KafkaErrorEnvelopeSinkPartition(
    _KafkaSinkPartitionBase, StatelessSinkPartition[ErrorEnvelope[StreamPayload]]
):
    """Write Loom error envelopes to Kafka."""

    _message_producer: KafkaMessageProducer[ErrorEnvelope[StreamPayload]]

    def write_batch(self, items: list[ErrorEnvelope[StreamPayload]]) -> None:
        _write_kafka_batch(
            sink=self._sink,
            producer=self._producer,
            message_producer=self._message_producer,
            items=items,
            item_to_send=lambda envelope: _error_item_to_send(
                envelope, self._sink.partition_policy
            ),
            item_to_commit=_error_item_to_commit,
            commit_tracker=self._commit_tracker,
        )


class _KafkaDecodeErrorSinkPartition(_KafkaSinkPartitionBase, StatelessSinkPartition[DecodeError]):
    """Write Kafka wire decode failures to Kafka."""

    _message_producer: KafkaMessageProducer[DecodeError]

    def write_batch(self, items: list[DecodeError]) -> None:
        _write_kafka_batch(
            sink=self._sink,
            producer=self._producer,
            message_producer=self._message_producer,
            items=items,
            item_to_send=lambda error: _decode_error_to_send(error, self._sink.partition_policy),
            item_to_commit=_decode_error_to_commit,
            commit_tracker=self._commit_tracker,
        )


class _KafkaDynamicSinkBase:
    """Shared init and commit-tracker management for Kafka dynamic sinks."""

    def __init__(self, sink: CompiledSink) -> None:
        self._sink = sink
        self._commit_tracker: CommitCompletionPort | None = None

    def bind_commit_tracker(self, tracker: CommitCompletionPort | None) -> None:
        """Bind a commit tracker used by all partitions built from this sink."""
        self._commit_tracker = tracker


class _KafkaMessageSink(_KafkaDynamicSinkBase, DynamicSink[Message[StreamPayload]]):
    """Build Kafka sink partitions for the runtime output topic.

    Terminal tracing is optional and off by default. The adapter binds it only
    at the wiring sites that run for a real terminal, so this sink is the only
    one that can emit a death span: the error sinks structurally cannot receive
    the binding, and an inline ``WithAsync`` sink is refused it.

    Args:
        sink: Compiled Kafka sink configuration.
    """

    def __init__(self, sink: CompiledSink) -> None:
        super().__init__(sink)
        self._terminal_tracing: _TerminalTracing | None = None
        self._built_inline_partition = False

    def bind_terminal_tracing(
        self,
        observer: ObservabilityRuntime,
        flow: str,
        flow_run_id: str,
    ) -> None:
        """Make the partitions built from this sink emit terminal spans.

        Args:
            observer: Runtime the terminal and batch spans are opened on.
            flow: Name of the enclosing flow.
            flow_run_id: Identifier of the flow run.

        Raises:
            RuntimeError: If an inline ``WithAsync`` partition was already built
                from this sink, which would make it report a death that a still
                living message never had.
        """
        if self._built_inline_partition:
            raise RuntimeError(
                f"cannot bind terminal tracing to the sink for topic '{self._sink.topic}': "
                "an inline WithAsync partition was already built from it. A message written "
                "by an inline IntoTopic goes on living, so a terminal span there would "
                "report a death that never happened."
            )
        self._terminal_tracing = _TerminalTracing(
            observer=observer, flow=flow, flow_run_id=flow_run_id
        )

    def mark_inline_partition(self) -> None:
        """Record that an inline, non-terminal partition is built from this sink.

        Raises:
            RuntimeError: If terminal tracing is already bound. Nothing fixes
                the order the two wiring sites run in, so the collision is
                refused from both directions.
        """
        if self._terminal_tracing is not None:
            raise RuntimeError(
                f"cannot build an inline WithAsync partition from the sink for topic "
                f"'{self._sink.topic}': terminal tracing is already bound to it, so the "
                "inline and terminal wiring paths collided on one compiled sink."
            )
        self._built_inline_partition = True

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> StatelessSinkPartition[Message[StreamPayload]]:
        """Build one worker's partition, traced only when tracing is bound.

        Args:
            step_id:      Bytewax step identifier (unused).
            worker_index: Zero-based index of the calling worker.
            worker_count: Total number of workers in this run.

        Returns:
            The bare Kafka partition, or one wrapped to emit terminal spans.
        """
        del step_id, worker_index, worker_count
        # An inline write does not end the message's path, so it completes nothing.
        tracker = None if self._built_inline_partition else self._commit_tracker
        partition = _KafkaMessageSinkPartition(self._sink, tracker)
        if self._terminal_tracing is None:
            return partition
        return _TracedMessageSinkPartition(
            partition, self._terminal_tracing, topic=self._sink.topic
        )


class _KafkaErrorEnvelopeSink(_KafkaDynamicSinkBase, DynamicSink[ErrorEnvelope[StreamPayload]]):
    """Build Kafka sink partitions for routed error envelopes."""

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _KafkaErrorEnvelopeSinkPartition:
        del step_id, worker_index, worker_count
        return _KafkaErrorEnvelopeSinkPartition(self._sink, self._commit_tracker)


class _KafkaDecodeErrorSink(_KafkaDynamicSinkBase, DynamicSink[DecodeError]):
    """Build Kafka sink partitions for wire decode failures."""

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _KafkaDecodeErrorSinkPartition:
        del step_id, worker_index, worker_count
        return _KafkaDecodeErrorSinkPartition(self._sink, self._commit_tracker)


_ErrorSink: TypeAlias = _KafkaErrorEnvelopeSink | _KafkaDecodeErrorSink


def build_runtime_source(
    source: CompiledSource,
    commit_tracker: KafkaCommitTracker | None = None,
    observability: Any | None = None,
) -> KafkaPartitionedSource | MongoCDCSource:
    """Build the runtime source for one compiled input."""
    if isinstance(source, CompiledMongoCDCSource):
        return MongoCDCSource(source)
    return KafkaPartitionedSource(source, commit_tracker, observability)


def build_runtime_sink(
    sink: CompiledSink,
    commit_tracker: CommitCompletionPort | None = None,
) -> _KafkaMessageSink:
    """Build the runtime sink for one compiled Kafka output."""
    runtime_sink = _KafkaMessageSink(sink)
    runtime_sink.bind_commit_tracker(commit_tracker)
    return runtime_sink


def build_runtime_error_sinks(
    error_routes: dict[ErrorKind, CompiledSink],
    commit_tracker: CommitCompletionPort | None = None,
) -> dict[ErrorKind, _ErrorSink]:
    """Build runtime sinks for explicit error routes.

    ``WIRE`` errors carry ``DecodeError`` items and require ``_KafkaDecodeErrorSink``.
    All other kinds carry ``ErrorEnvelope`` items and use ``_KafkaErrorEnvelopeSink``.
    Keeping these as distinct typed sinks is the invariant that prevents routing a
    ``DecodeError`` into a function expecting an ``ErrorEnvelope``.
    """
    runtime_sinks: dict[ErrorKind, _ErrorSink] = {}
    for kind, sink in error_routes.items():
        if kind is ErrorKind.WIRE:
            runtime_sinks[kind] = _KafkaDecodeErrorSink(sink)
        else:
            runtime_sinks[kind] = _KafkaErrorEnvelopeSink(sink)
    for error_sink in runtime_sinks.values():
        error_sink.bind_commit_tracker(commit_tracker)
    return runtime_sinks


def build_runtime_terminal_sinks(
    terminal_sinks: dict[tuple[int, ...], CompiledSink],
    commit_tracker: CommitCompletionPort | None = None,
) -> dict[tuple[int, ...], _KafkaMessageSink]:
    """Build runtime sinks for terminal branch outputs."""
    return {path: build_runtime_sink(sink, commit_tracker) for path, sink in terminal_sinks.items()}


def build_commit_tracker(source: CompiledSource) -> KafkaCommitTracker | None:
    """Build a commit tracker when explicit source commits are required."""
    if not isinstance(source, (CompiledSingleSource, CompiledMultiSource)):
        return None
    if source.settings.effective_delivery() == "at_most_once":
        return None
    return KafkaCommitTracker()


def _resolve_partition_key(
    message: Message[StreamPayload],
    partition_policy: PartitionPolicy[Any] | None,
) -> bytes | str | None:
    """Resolve the outgoing Kafka key for one message.

    The transport key is preserved when the incoming message already carries
    one and the policy does not explicitly allow repartitioning. When no
    transport key is present, the declared policy strategy may derive one.

    Args:
        message: Transport-neutral message to publish.
        partition_policy: Optional output partitioning policy.

    Returns:
        Kafka key to use for the outgoing record, or ``None``.
    """
    if partition_policy is None:
        return message.meta.key

    incoming_key = message.meta.key
    policy_key = partition_policy.strategy.partition_key(message)

    if incoming_key is None:
        return policy_key
    if partition_policy.allow_repartition:
        return policy_key if policy_key is not None else incoming_key
    return incoming_key


def _message_to_send_with_policy(
    message: Message[StreamPayload],
    partition_policy: PartitionPolicy[Any] | None,
) -> _KafkaSendRequest[StreamPayload]:
    incoming_trace_id = message.meta.trace_id
    return _KafkaSendRequest(
        payload=message.payload,
        descriptor=MessageDescriptor(
            message_type=message.meta.message_type or "loom.streaming.message",
            message_version=message.meta.message_version or 1,
        ),
        key=_resolve_partition_key(message, partition_policy),
        headers=message.meta.headers,
        correlation_id=message.meta.correlation_id,
        parent_trace_id=incoming_trace_id,
        causation_id=message.meta.causation_id,
        trace_id=generate_trace_id(),
        produced_at_ms=message.meta.produced_at_ms,
    )


def _message_to_commit(
    message: Message[StreamPayload],
) -> tuple[str | None, int | None, int | None]:
    return message.meta.topic, message.meta.partition, message.meta.offset


def _error_item_to_send(
    item: ErrorEnvelope[StreamPayload],
    partition_policy: PartitionPolicy[Any] | None,
) -> _KafkaSendRequest[ErrorEnvelope[StreamPayload]]:
    original = item.original_message
    headers: dict[str, bytes] = {
        "x-error-kind": item.kind.value.encode("utf-8"),
        "x-error-reason": item.reason.encode("utf-8"),
    }
    descriptor = MessageDescriptor(
        message_type=f"loom.streaming.error.{item.kind.value}",
        message_version=1,
    )
    if original is None:
        return _KafkaSendRequest(
            payload=item,
            descriptor=descriptor,
            key=None,
            headers=headers,
            correlation_id=None,
            parent_trace_id=None,
            causation_id=None,
            trace_id=generate_trace_id(),
            produced_at_ms=None,
        )
    key = _resolve_partition_key(cast(Message[StreamPayload], original), partition_policy)
    return _KafkaSendRequest(
        payload=item,
        descriptor=descriptor,
        key=key,
        headers={**original.meta.headers, **headers},
        correlation_id=original.meta.correlation_id,
        parent_trace_id=original.meta.parent_trace_id,
        causation_id=original.meta.causation_id,
        trace_id=original.meta.trace_id,
        produced_at_ms=original.meta.produced_at_ms,
    )


def _error_item_to_commit(
    item: ErrorEnvelope[StreamPayload],
) -> tuple[str | None, int | None, int | None]:
    original = item.original_message
    if original is None:
        return (None, None, None)
    return original.meta.topic, original.meta.partition, original.meta.offset


def _decode_str_header(headers: dict[str, bytes], key: str) -> str | None:
    raw = headers.get(key)
    return raw.decode() if raw is not None else None


def _decode_error_to_send(
    item: DecodeError,
    partition_policy: PartitionPolicy[Any] | None,
) -> _KafkaSendRequest[DecodeError]:
    del partition_policy
    return _KafkaSendRequest(
        payload=item,
        descriptor=MessageDescriptor(
            message_type=DecodeError.loom_message_type(), message_version=1
        ),
        key=item.key,
        headers={
            **item.headers,
            "x-error-kind": item.error.kind.value.encode("utf-8"),
            "x-error-reason": item.error.reason.encode("utf-8"),
        },
        correlation_id=_decode_str_header(item.headers, HEADER_CORRELATION_ID),
        parent_trace_id=_decode_str_header(item.headers, HEADER_PARENT_TRACE_ID),
        causation_id=_decode_str_header(item.headers, HEADER_CAUSATION_ID),
        trace_id=_decode_str_header(item.headers, HEADER_TRACE_ID),
        produced_at_ms=item.timestamp_ms,
    )


def _decode_error_to_commit(
    item: DecodeError,
) -> tuple[str | None, int | None, int | None]:
    return item.topic, item.partition, item.offset


def drop_item_to_commit(item: object) -> tuple[str | None, int | None, int | None]:
    """Resolve the commit triple for any droppable runtime item.

    Dropped streams carry ``Message`` (router/filter non-matches),
    ``ErrorEnvelope`` (unrouted error kinds), or ``DecodeError`` (unrouted
    WIRE errors). Every drop must still complete its record or the commit
    watermark of its partition freezes under at-least-once delivery.
    """
    if isinstance(item, Message):
        return _message_to_commit(item)
    if isinstance(item, ErrorEnvelope):
        return _error_item_to_commit(item)
    if isinstance(item, DecodeError):
        return _decode_error_to_commit(item)
    return (None, None, None)
