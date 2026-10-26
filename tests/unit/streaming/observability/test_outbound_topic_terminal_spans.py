"""A message written to an outbound Kafka topic dies where it is written.

``TerminalReason.SINK_WRITE`` documented "a storage sink **or an outbound
topic**" while the Kafka write path emitted nothing, so a flow ending in
``IntoTopic`` — the most common streaming topology — produced a trace that
stopped at its last node span.

Every assertion here reads exported span structure or recorded lifecycle events
from a real SDK exporter and a real observer. Kafka is faked at the
confluent-producer level, so ``build_runtime_terminal_sinks`` builds the real
``_KafkaMessageSink`` and the wiring under test is the wiring exercised.
"""

from __future__ import annotations

from collections.abc import Sequence
from time import sleep, time_ns
from typing import Any

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from bytewax.run import cli_main  # noqa: E402
from bytewax.testing import TestingSource  # noqa: E402
from opentelemetry.sdk.trace import ReadableSpan  # noqa: E402
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult  # noqa: E402
from opentelemetry.trace import SpanKind, StatusCode  # noqa: E402
from opentelemetry.trace.span import TraceState  # noqa: E402
from opentelemetry.util.types import Attributes  # noqa: E402

from loom.core.config import ConfigContext  # noqa: E402
from loom.core.observability.event import EventKind, Scope  # noqa: E402
from loom.streaming import (  # noqa: E402
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    StreamFlow,
    WithAsync,
)
from loom.streaming.bytewax import _adapter  # noqa: E402
from loom.streaming.bytewax import _batch_spans as _batch_spans_mod  # noqa: E402
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown  # noqa: E402
from loom.streaming.bytewax._runtime_io import (  # noqa: E402
    _KafkaMessageSink,
    _KafkaMessageSinkPartition,
    _TracedMessageSinkPartition,
    build_runtime_error_sinks,
    build_runtime_sink,
    build_runtime_terminal_sinks,
)
from loom.streaming.compiler import compile_flow  # noqa: E402
from loom.streaming.compiler._plan import CompiledSink  # noqa: E402
from loom.streaming.kafka._config import ProducerSettings  # noqa: E402
from loom.streaming.kafka._errors import KafkaDeliveryError  # noqa: E402
from tests.helpers.spans import SpanRecorder, build_recorder, hex_trace  # noqa: E402
from tests.unit.streaming.bytewax.cases import Item, UpperItem  # noqa: E402

pytestmark = pytest.mark.bytewax

_FLOW = "orders"
_OUT_TOPIC = "items.out"
_DLQ_TOPIC = "items.dlq"
_ERR_TOPIC = "items.err"

# Low 64 bits of 1: pins the batch span's own trace so a sampler decision about
# it never masks the invariant a test is really about.
_ALWAYS_SAMPLED = "f" * 16 + "0" * 15 + "1"

_TRACES = (
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
    "ccccccccccccccccccccccccccccccc3",
)

_CONFIG = {
    "kafka": {
        "consumer": {
            "brokers": ["localhost:9092"],
            "group_id": "g",
            "delivery": "at_most_once",
            "topics": ["items"],
        },
        "producer": {"brokers": ["localhost:9092"], "topic": _OUT_TOPIC},
    }
}


class _Boom(RecordStep[Item, Item]):
    """Always fail, so the message becomes an error envelope."""

    def execute(self, message: Message[Item], **kwargs: object) -> Item:
        del kwargs, message
        raise RuntimeError("node exploded")


class _FakeConfluentProducer:
    """Confluent-level producer double with a controllable ``flush``.

    Faking here rather than at ``KafkaProducerClient`` keeps the real client,
    the real codec and the real ``_write_kafka_batch`` in the path under test.
    """

    def __init__(self, config: dict[str, str] | None = None) -> None:
        self.config = config or {}
        self.produced: list[dict[str, Any]] = []
        self.flush_calls: list[float | None] = []
        self.calls: list[str] = []
        self.first_produce_ns: int | None = None
        self.flush_error: Exception | None = None
        self.flush_errors: list[Exception] = []
        self.flush_delay_s: float = 0.0

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]] | None = None,
        timestamp: int | None = None,
        on_delivery: Any = None,
    ) -> None:
        """Record one produced record."""
        del on_delivery
        if self.first_produce_ns is None:
            self.first_produce_ns = time_ns()
        self.calls.append(f"produce:{topic}")
        self.produced.append(
            {"topic": topic, "key": key, "value": value, "headers": headers, "ts": timestamp}
        )

    def poll(self, timeout: float) -> None:
        """Accept the client's post-produce poll."""
        del timeout

    def flush(self, timeout: float | None = None) -> None:
        """Flush, optionally slowly and optionally failing.

        Queued ``flush_errors`` are raised one per flush before ``flush_error``.
        """
        self.calls.append("flush")
        self.flush_calls.append(timeout)
        if self.flush_delay_s:
            sleep(self.flush_delay_s)
        if self.flush_errors:
            raise self.flush_errors.pop(0)
        if self.flush_error is not None:
            raise self.flush_error


class _RecordingTracker:
    """Commit tracker that records the records a batch completed."""

    def __init__(self) -> None:
        self.completed: list[tuple[str, int, int]] = []

    def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
        """Accept the fan-out hook; no branch under test forks."""

    def complete(self, topic: str, partition: int, offset: int) -> None:
        """Record one completed record."""
        self.completed.append((topic, partition, offset))


class _OnlyTheBatchTrace(Sampler):
    """Ratio 0.0 for message traces; keeps only the pinned batch trace.

    A plain ``TraceIdRatioBased(0.0)`` would also drop the batch span, leaving
    nothing to read its link list from. This drops every message trace exactly
    as ratio 0.0 does, and keeps the one span whose links are under assertion.
    """

    def should_sample(
        self,
        parent_context: Any = None,
        trace_id: int = 0,
        name: str = "",
        kind: SpanKind | None = None,
        attributes: Attributes = None,
        links: Any = None,
        trace_state: TraceState | None = None,
    ) -> SamplingResult:
        """Keep the pinned batch trace and drop everything else."""
        del parent_context, name, kind, links
        keep = trace_id == int(_ALWAYS_SAMPLED, 16)
        decision = Decision.RECORD_AND_SAMPLE if keep else Decision.DROP
        return SamplingResult(decision, attributes, trace_state)

    def get_description(self) -> str:
        """Describe the sampler for the SDK."""
        return "only-the-batch-trace"


def install_fake_producer(monkeypatch: pytest.MonkeyPatch) -> _FakeConfluentProducer:
    """Install one shared confluent-level producer double and return it."""
    fake = _FakeConfluentProducer()
    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer",
        lambda config: fake,
    )
    return fake


def _compiled_sink(*, dlq_topic: str | None = None, topic: str = _OUT_TOPIC) -> CompiledSink:
    return CompiledSink(
        settings=ProducerSettings(
            brokers=("localhost:9092",), client_id="test-producer", topic=topic
        ),
        topic=topic,
        partition_policy=None,
        dlq_topic=dlq_topic,
    )


def _terminal_sink(
    recorder: SpanRecorder | None,
    *,
    dlq_topic: str | None = None,
    tracker: _RecordingTracker | None = None,
) -> _KafkaMessageSink:
    """Build the production sink for one terminal path, optionally traced."""
    sinks = build_runtime_terminal_sinks({(0,): _compiled_sink(dlq_topic=dlq_topic)}, tracker)
    sink = sinks[(0,)]
    if recorder is not None:
        sink.bind_terminal_tracing(recorder.runtime, _FLOW, "run-1")
    return sink


def _messages(count: int = 3) -> list[Message[Any]]:
    return [
        Message(
            payload=Item(value=f"row-{index}"),
            meta=MessageMeta(
                message_id=f"msg-{index}",
                trace_id=_TRACES[index],
                topic="items",
                partition=0,
                offset=index,
            ),
        )
        for index in range(count)
    ]


def _terminal_spans(recorder: SpanRecorder) -> list[ReadableSpan]:
    return recorder.named("terminal:sink_write")


def _batch_span(recorder: SpanRecorder) -> ReadableSpan:
    return recorder.one(f"write:{_FLOW}:{_OUT_TOPIC}")


def _attribute(span: ReadableSpan, key: str) -> object:
    assert span.attributes is not None
    return span.attributes.get(key)


def _run_flow(
    recorder: SpanRecorder,
    flow: StreamFlow[Item, Item],
    messages: Sequence[Message[Any]],
    *,
    with_error_sinks: bool = False,
) -> None:
    """Compile and run a flow on the real Bytewax runtime with real sinks."""
    plan = compile_flow(flow, config=ConfigContext.from_dict(_CONFIG))
    error_sinks = build_runtime_error_sinks(plan.error_routes) if with_error_sinks else None
    built = build_dataflow_with_shutdown(
        plan,
        observability_runtime=recorder.runtime,
        source=TestingSource(list(messages)),
        sink=build_runtime_sink(plan.output) if plan.output is not None else None,
        error_sinks=error_sinks,
    )
    try:
        cli_main(built.dataflow, workers_per_process=1)  # type: ignore[no-untyped-call]
    finally:
        built.shutdown()


def _terminal_flow() -> StreamFlow[Item, Item]:
    return StreamFlow(
        name=_FLOW,
        source=FromTopic("items", payload=Item),
        process=Process(UpperItem(), IntoTopic(_OUT_TOPIC, payload=Item)),
    )


def _inline_then_terminal_flow() -> StreamFlow[Item, Item]:
    """``WithAsync(process=[step, IntoTopic])`` followed by a real terminal.

    ``WithAsync(process=...)`` must be the last node of its process, so the real
    terminal is the flow output — which is exactly the production shape: the
    message survives the inline write and continues to the output topic.
    """
    return StreamFlow(
        name=_FLOW,
        source=FromTopic("items", payload=Item),
        process=Process(
            WithAsync(process=Process(UpperItem(), IntoTopic(_OUT_TOPIC, payload=Item)))
        ),
        output=IntoTopic(_OUT_TOPIC, payload=Item),
    )


def _error_route_flow() -> StreamFlow[Item, Item]:
    return StreamFlow(
        name=_FLOW,
        source=FromTopic("items", payload=Item),
        process=Process(_Boom(), IntoTopic(_OUT_TOPIC, payload=Item)),
        errors=IntoTopic(_ERR_TOPIC, payload=Item),
    )


class TestOneDeathPerMessage:
    """AC1: three messages, three deaths, each in its own trace."""

    def test_each_message_gets_exactly_one_terminal_event_pair_in_its_own_trace(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Construction is pinned: the adapter builds the real sinks itself.

        No ``terminal_sinks=`` override, so the sink the flow writes through is
        the one ``build_runtime_terminal_sinks`` made and the binding site is
        the site under test.
        """
        install_fake_producer(monkeypatch)
        recorder = build_recorder()
        messages = _messages()

        _run_flow(recorder, _terminal_flow(), messages)

        terminal = recorder.collector.scoped(Scope.TERMINAL)
        starts = [event for event in terminal if event.kind is EventKind.START]
        ends = [event for event in terminal if event.kind is EventKind.END]
        assert len(starts) == 3, "a message written to an outbound topic must record its death"
        assert len(ends) == 3, "every terminal START must be closed"
        assert {event.name for event in terminal} == {"sink_write"}
        assert sorted(event.trace_id or "" for event in starts) == sorted(_TRACES), (
            "the deaths must be labelled with three distinct message trace ids, "
            "not with one message's id repeated"
        )


class TestTheBatchSpanBelongsToNoMessage:
    """AC2: the flush is one span, in a trace of its own, linked to the deaths."""

    def test_batch_span_is_a_root_of_its_own_trace_and_every_death_points_at_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The write runs inside an enclosing host span, as it does in production.

        Without an ambient current span, opening as a root and opening as a
        child are indistinguishable, and the assertion would be vacuous.
        """
        install_fake_producer(monkeypatch)
        recorder = build_recorder()
        messages = _messages()
        partition = _terminal_sink(recorder).build("step", 0, 1)

        with recorder.runtime.span(Scope.FLOW, "host"):
            partition.write_batch(messages)

        batch = _batch_span(recorder)
        assert batch.parent is None, "the batch span must be a root, not a child of a host span"
        assert hex_trace(batch) not in _TRACES, (
            "borrowing a message's trace would tell that message a story about N-1 others"
        )
        assert _attribute(batch, "loom.batch_size") == 3
        batch_id = _attribute(batch, "loom.batch_id")
        assert hex_trace(batch) == batch_id, (
            "the batch span must be the root of the trace its batch id names; "
            "opened as a child it inherits somebody else's trace instead"
        )
        deaths = _terminal_spans(recorder)
        assert len(deaths) == 3
        assert {hex_trace(span) for span in deaths} == set(_TRACES)
        assert {_attribute(span, "loom.batch_id") for span in deaths} == {batch_id}


class TestLinksFollowRecording:
    """AC3: a link to a span the sampler dropped points at nothing."""

    def test_no_links_when_no_participation_span_is_recorded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_fake_producer(monkeypatch)
        monkeypatch.setattr(_batch_spans_mod, "generate_trace_id", lambda: _ALWAYS_SAMPLED)
        recorder = build_recorder(sampler=_OnlyTheBatchTrace())

        _terminal_sink(recorder).build("step", 0, 1).write_batch(_messages())

        assert _terminal_spans(recorder) == [], "the message traces were meant to be dropped"
        assert len(_batch_span(recorder).links) == 0, (
            "the batch span advertised an edge to a span nobody exported"
        )

    def test_one_link_per_recorded_participation_span(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_fake_producer(monkeypatch)
        recorder = build_recorder()

        _terminal_sink(recorder).build("step", 0, 1).write_batch(_messages())

        assert len(_batch_span(recorder).links) == 3


class TestInlineIntoTopicDoesNotKillTheMessage:
    """AC4: the criterion the wiring-site choice exists to satisfy."""

    def test_a_withasync_inline_intotopic_adds_no_second_death(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The inline write is not a death — the message goes on to its terminal.

        Binding at ``build_runtime_terminal_sinks`` would emit a terminal span
        for a message that is still alive, so each message would die twice.
        """
        install_fake_producer(monkeypatch)
        recorder = build_recorder()
        messages = _messages()

        _run_flow(recorder, _inline_then_terminal_flow(), messages)

        starts = [
            event
            for event in recorder.collector.scoped(Scope.TERMINAL)
            if event.kind is EventKind.START
        ]
        assert len(starts) == 3, (
            "each message must die exactly once; the inline IntoTopic write is not a death"
        )
        assert sorted(event.trace_id or "" for event in starts) == sorted(_TRACES)


class TestErrorSinksEmitNoTerminalSpan:
    """AC5: the envelope travels in the inherited trace; it must not die twice."""

    def test_a_failed_message_has_exactly_one_terminal_span_in_its_trace(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        install_fake_producer(monkeypatch)
        recorder = build_recorder()
        messages = _messages(1)

        _run_flow(recorder, _error_route_flow(), messages, with_error_sinks=True)

        terminal = [
            event
            for event in recorder.collector.scoped(Scope.TERMINAL)
            if event.kind is EventKind.START and event.trace_id == _TRACES[0]
        ]
        assert [event.name for event in terminal] == ["error_envelope"], (
            "the envelope's own write must not add a second death to the trace of "
            "the message it carries"
        )


class TestReRaiseExit:
    """AC6: delivery failed and no DLQ is declared."""

    def test_every_span_closes_failed_and_the_error_propagates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = install_fake_producer(monkeypatch)
        fake.flush_error = RuntimeError("broker down")
        recorder = build_recorder()
        partition = _terminal_sink(recorder).build("step", 0, 1)

        messages = _messages()

        with pytest.raises(KafkaDeliveryError):
            partition.write_batch(messages)

        deaths = _terminal_spans(recorder)
        assert len(deaths) == 3, "a failed write must still record where each message ended"
        for span in deaths:
            assert span.status.status_code is StatusCode.ERROR
            assert _attribute(span, "terminal.failure_scope") == "batch"
            assert _attribute(span, "terminal.dlq_topic") is None
        assert _batch_span(recorder).status.status_code is StatusCode.ERROR

    def test_every_terminal_start_is_matched_by_a_close(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A span opened before the write and abandoned on failure leaks."""
        fake = install_fake_producer(monkeypatch)
        fake.flush_error = RuntimeError("broker down")
        recorder = build_recorder()
        partition = _terminal_sink(recorder).build("step", 0, 1)

        messages = _messages()

        with pytest.raises(KafkaDeliveryError):
            partition.write_batch(messages)

        terminal = recorder.collector.scoped(Scope.TERMINAL)
        starts = [event for event in terminal if event.kind is EventKind.START]
        closes = [event for event in terminal if event.kind is not EventKind.START]
        assert len(starts) == 3
        assert len(closes) == len(starts), "a terminal span was opened and never closed"


class TestDivertedExit:
    """AC7: delivery failed, a DLQ topic is declared, the batch commits once it lands there."""

    def test_diverted_batches_close_failed_and_still_commit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The batch never reached its outbound topic, so the diversion is a failure."""
        fake = install_fake_producer(monkeypatch)
        fake.flush_errors = [RuntimeError("broker down")]
        recorder = build_recorder()
        tracker = _RecordingTracker()
        sink = _terminal_sink(recorder, dlq_topic=_DLQ_TOPIC, tracker=tracker)

        sink.build("step", 0, 1).write_batch(_messages())

        deaths = _terminal_spans(recorder)
        assert len(deaths) == 3
        for span in deaths:
            assert span.status.status_code is StatusCode.ERROR, (
                "a batch diverted away from its outbound topic is not a success"
            )
            assert _attribute(span, "terminal.dlq_topic") == _DLQ_TOPIC
            assert _attribute(span, "terminal.failure_scope") == "batch"
        assert tracker.completed == [("items", 0, 0), ("items", 0, 1), ("items", 0, 2)]
        assert any(call == f"produce:{_DLQ_TOPIC}" for call in fake.calls)

    def test_write_batch_outcome_reports_the_diversion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A wrapper cannot see a swallowed failure; the return value carries it."""
        fake = install_fake_producer(monkeypatch)
        fake.flush_errors = [RuntimeError("broker down")]
        partition = _terminal_sink(None, dlq_topic=_DLQ_TOPIC).build("step", 0, 1)
        assert isinstance(partition, _KafkaMessageSinkPartition)

        outcome = partition.write_batch_outcome(_messages())

        assert outcome.dlq_topic == _DLQ_TOPIC
        assert isinstance(outcome.error, KafkaDeliveryError)


class TestTheWindowIsHonest:
    """AC8: the span window is the write's real window, not the emitter's."""

    def test_duration_covers_the_flush_and_starts_before_the_first_send(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        delay_ms = 40.0
        fake = install_fake_producer(monkeypatch)
        fake.flush_delay_s = delay_ms / 1000
        recorder = build_recorder()

        _terminal_sink(recorder).build("step", 0, 1).write_batch(_messages())

        batch = _batch_span(recorder)
        duration = _attribute(batch, "duration_ms")
        assert isinstance(duration, float)
        assert duration >= delay_ms, (
            f"the flush blocked for {delay_ms}ms but the span reported {duration}ms: "
            "the window was measured after the work instead of around it"
        )
        assert fake.first_produce_ns is not None
        assert batch.start_time is not None
        assert batch.start_time <= fake.first_produce_ns, (
            "the span started after the first record was sent"
        )
        for span in _terminal_spans(recorder):
            assert span.start_time is not None
            assert span.start_time <= fake.first_produce_ns


class TestEmptyBatch:
    """AC9: an idle epoch is still forwarded, and is not a death."""

    def test_an_empty_batch_flushes_and_emits_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = install_fake_producer(monkeypatch)
        recorder = build_recorder()

        _terminal_sink(recorder).build("step", 0, 1).write_batch([])

        assert fake.produced == [], "an empty batch must not produce records"
        assert fake.flush_calls == [None], "the write must still be forwarded"
        assert recorder.spans() == (), "an idle epoch is not a batch of zero deaths"


class TestTheProductionInlinePathIsInert:
    """AC10: the inline partition is the undecorated object, and stays silent."""

    def test_inline_partition_is_bare_and_writes_without_spans(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake = install_fake_producer(monkeypatch)
        recorder = build_recorder()
        plan = compile_flow(_inline_then_terminal_flow(), config=ConfigContext.from_dict(_CONFIG))
        ctx = _adapter._BuildContext(
            plan=plan,
            bridge=None,
            flow_runtime=recorder.runtime,
            flow_run_id="run-1",
            terminal_sinks=build_runtime_terminal_sinks(plan.terminal_sinks),
        )

        partition = ctx.inline_sink_partition_for((0, 1))

        assert isinstance(partition, _KafkaMessageSinkPartition)
        assert not isinstance(partition, _TracedMessageSinkPartition)
        partition.write_batch(_messages(1))
        assert recorder.spans() == (), "an inline write is not a death"
        assert fake.calls == [f"produce:{_OUT_TOPIC}", "flush"]
