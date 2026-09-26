"""Reference flow built with loom's streaming DSL and run on bytewax.

The flow goes through the same compile and wiring path as production
(``compile_flow`` and the runner's ``_prepare_run``, which is also what
``loom.streaming.testing.StreamingTestRunner`` uses); only the Kafka source and
sinks are replaced by in-memory ones. What it exercises of bytewax:

* ``FixedPartitionedSource``/``StatefulSourcePartition`` with resume state,
  emitting ``KafkaRecord[bytes]`` that loom decodes with msgspec (``map`` +
  ``branch`` on the decode result);
* ``map`` for the record step;
* ``key_on`` + ``collect`` for ``CollectBatch`` (loom groups by
  ``topic:partition`` when the record carries a partition, as Kafka records do);
* ``flat_map`` for the batch fan-out back to records and for ``Drain``;
* ``branch`` for ``Fork.by``;
* ``DynamicSink`` per terminal branch, and ``cli_main`` with workers,
  processes and ``RecoveryConfig``.
"""

from __future__ import annotations

import importlib.metadata
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.run import cli_main

from benchmarks.streaming import gc_policy
from benchmarks.streaming.load import Event, LoadGenerator, Pacer, now_ns
from benchmarks.streaming.metrics import Recorder, SinkItem
from benchmarks.streaming.params import Mode, RunSpec
from loom.core.config import ConfigContext
from loom.core.model import LoomFrozenStruct
from loom.streaming import (
    BatchExpandStep,
    CollectBatch,
    Drain,
    Fork,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    StreamFlow,
    compile_flow,
    msg,
)
from loom.streaming.bytewax.runner import BytewaxRuntimeConfig, _prepare_run
from loom.streaming.kafka._codec import MsgspecCodec
from loom.streaming.kafka._message import MessageDescriptor, MessageEnvelope, MessageMetadata
from loom.streaming.kafka._record import KafkaRecord

SOURCE_TOPIC = "bench.in"
BRANCH_TOPICS = {"a": "bench.a", "b": "bench.b"}
_BROKER = "unused:9092"
_DESCRIPTOR = MessageDescriptor(message_type="bench.event", message_version=1)


class BenchEvent(LoomFrozenStruct, frozen=True):
    """Input payload."""

    key: str
    lane: str
    amount: int
    emitted_ns: int
    blob: bytes


class ScoredEvent(LoomFrozenStruct, frozen=True):
    """Output payload."""

    key: str
    lane: str
    score: int
    emitted_ns: int
    blob: bytes


class Normalize(RecordStep[BenchEvent, BenchEvent]):
    """Record step: clamp the amount (bytewax ``map``)."""

    def execute(self, message: Message[BenchEvent], **kwargs: object) -> BenchEvent:
        del kwargs
        event = message.payload
        return BenchEvent(
            key=event.key,
            lane=event.lane,
            amount=min(event.amount, 9_000),
            emitted_ns=event.emitted_ns,
            blob=event.blob,
        )


class ScoreBatch(BatchExpandStep[BenchEvent, ScoredEvent]):
    """Batch step after ``collect``: score each event against its batch, back to records."""

    def execute(self, messages: list[Message[BenchEvent]], **kwargs: object) -> list[ScoredEvent]:
        del kwargs
        total = sum(m.payload.amount for m in messages)
        return [
            ScoredEvent(
                key=m.payload.key,
                lane=m.payload.lane,
                score=(m.payload.amount * 31 + total) % 997,
                emitted_ns=m.payload.emitted_ns,
                blob=m.payload.blob,
            )
            for m in messages
        ]


def build_flow(batch_max: int, batch_timeout_ms: int) -> StreamFlow[Any, Any]:
    """Return the reference flow."""
    return StreamFlow(
        name="bench_reference",
        source=FromTopic(SOURCE_TOPIC, payload=BenchEvent),
        process=Process(
            Normalize,
            CollectBatch(max_records=batch_max, timeout_ms=batch_timeout_ms),
            ScoreBatch,
            Fork.by(
                msg.payload.lane,
                branches={
                    lane: Process(IntoTopic(topic, payload=ScoredEvent))
                    for lane, topic in BRANCH_TOPICS.items()
                },
                default=Process(Drain()),
            ),
        ),
    )


def flow_config() -> dict[str, Any]:
    """Return the Kafka section the compiler resolves; nothing connects to it."""
    producers = {topic: {"brokers": [_BROKER], "topic": topic} for topic in BRANCH_TOPICS.values()}
    return {
        "kafka": {
            "consumer": {"brokers": [_BROKER], "group_id": "bench", "topics": [SOURCE_TOPIC]},
            "producers": producers,
        }
    }


class _GeneratedPartition(StatefulSourcePartition[KafkaRecord[bytes], int]):
    """One source partition replaying the deterministic load from an offset."""

    def __init__(
        self, generator: LoadGenerator, partition: int, start: int, recorder: Recorder
    ) -> None:
        self._generator = generator
        self._partition = partition
        self._start = start
        self._next = start
        self._size = generator.partition_size(partition)
        self._recorder = recorder
        self._pacer = Pacer(generator.params)
        self._codec: MsgspecCodec[BenchEvent] = MsgspecCodec()
        self._marked = False

    def next_batch(self) -> list[KafkaRecord[bytes]]:
        if self._next >= self._size:
            raise StopIteration
        stamp = now_ns()
        due = min(self._pacer.due(self._next - self._start, stamp), self._size - self._next)
        if due == 0:
            return []
        if not self._marked:
            self._recorder.mark_emit(stamp)
            self._marked = True
        batch = [
            self._record(self._generator.event(self._partition, offset), stamp)
            for offset in range(self._next, self._next + due)
        ]
        self._next += due
        return batch

    def _record(self, event: Event, stamp: int) -> KafkaRecord[bytes]:
        envelope = MessageEnvelope(
            meta=MessageMetadata(
                descriptor=_DESCRIPTOR,
                trace_id=f"{event.partition}-{event.offset}",
                produced_at_ms=stamp // 1_000_000,
            ),
            payload=BenchEvent(
                key=event.key,
                lane=event.lane,
                amount=event.amount,
                emitted_ns=stamp,
                blob=event.blob,
            ),
        )
        return KafkaRecord(
            topic=SOURCE_TOPIC,
            key=event.key,
            value=self._codec.encode(envelope),
            partition=event.partition,
            offset=event.offset,
            timestamp_ms=stamp // 1_000_000,
        )

    def next_awake(self) -> datetime | None:
        due_ns = self._pacer.next_awake_ns(self._next - self._start)
        if due_ns is None:
            return None
        return datetime.fromtimestamp(due_ns / 1e9, tz=UTC)

    def snapshot(self) -> int:
        return self._next


class GeneratedSource(FixedPartitionedSource[KafkaRecord[bytes], int]):
    """Partitioned, resumable in-memory stand-in for ``KafkaPartitionedSource``."""

    def __init__(self, generator: LoadGenerator, recorder: Recorder) -> None:
        self._generator = generator
        self._recorder = recorder

    def list_parts(self) -> list[str]:
        return self._generator.partition_names()

    def build_part(
        self, step_id: str, for_part: str, resume_state: int | None
    ) -> _GeneratedPartition:
        del step_id
        partition = int(for_part.removeprefix("p"))
        start = resume_state if resume_state is not None else 0
        return _GeneratedPartition(self._generator, partition, start, self._recorder)


class _RecorderPartition(StatelessSinkPartition[Message[ScoredEvent]]):
    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    def write_batch(self, items: Sequence[Message[ScoredEvent]]) -> None:
        self._recorder.record_batch(
            SinkItem(
                partition=item.meta.partition if item.meta.partition is not None else -1,
                offset=item.meta.offset if item.meta.offset is not None else -1,
                emitted_ns=item.payload.emitted_ns,
            )
            for item in items
        )


class RecorderSink(DynamicSink[Message[ScoredEvent]]):
    """Terminal sink reporting every output to the recorder."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    def build(self, step_id: str, worker_index: int, worker_count: int) -> _RecorderPartition:
        del step_id, worker_index, worker_count
        return _RecorderPartition(self._recorder)


class LoomBytewaxEngine:
    """Run the reference flow with loom on bytewax."""

    def describe(self) -> dict[str, str]:
        """Name the distribution that provides ``bytewax``.

        ``loom-bytewax`` installs the same ``bytewax`` package as PyPI's
        ``bytewax``; more than one provider means the environment is mixed and
        its numbers are not attributable, so the run is refused.
        """
        providers = importlib.metadata.packages_distributions().get("bytewax", [])
        if len(providers) != 1:
            raise RuntimeError(f"expected one distribution providing bytewax, got {providers}")
        name = providers[0]
        return {
            "engine": "loom-bytewax",
            "engine_distribution": name,
            "engine_version": importlib.metadata.version(name),
            "loom_version": importlib.metadata.version("loom-kernel"),
        }

    def run(self, spec: RunSpec, generator: LoadGenerator, recorder: Recorder) -> None:
        logging.getLogger().setLevel(logging.WARNING)
        plan = compile_flow(
            build_flow(spec.flow.batch_max, spec.flow.batch_timeout_ms),
            config=ConfigContext.from_dict(flow_config()),
        )
        sink = RecorderSink(recorder)
        prepared = _prepare_run(
            plan,
            source=GeneratedSource(generator, recorder),
            terminal_sinks=dict.fromkeys(plan.terminal_sinks, sink),
            error_sinks={},
            runtime=BytewaxRuntimeConfig(workers_per_process=spec.flow.workers),
        )
        gc_policy.apply(spec.gc_policy)
        try:
            cli_main(  # type: ignore[no-untyped-call]
                prepared.dataflow,
                workers_per_process=spec.flow.workers,
                process_id=spec.process_id if spec.addresses else None,
                addresses=list(spec.addresses) or None,
                epoch_interval=timedelta(milliseconds=spec.flow.epoch_interval_ms),
                recovery_config=_recovery(spec),
            )
        finally:
            prepared.shutdown()


def _recovery(spec: RunSpec) -> RecoveryConfig | None:
    if not spec.recovery_dir:
        return None
    db_dir = Path(spec.recovery_dir)
    if spec.mode is Mode.CRASH and spec.process_id == 0:
        db_dir.mkdir(parents=True, exist_ok=True)
        init_db_dir(db_dir, max(1, spec.flow.processes))  # type: ignore[no-untyped-call]
    return RecoveryConfig(db_dir)  # type: ignore[no-untyped-call]


ENGINE = LoomBytewaxEngine()
