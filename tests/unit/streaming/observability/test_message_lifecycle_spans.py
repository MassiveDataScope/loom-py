"""A message traced from ingestion, through every node, to its death.

One trace id, continuous from the inbound Kafka header to the terminal span.
Every assertion reads exported span structure from a real SDK exporter: trace
ids, parents, links, and counts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from random import Random
from typing import Any, cast

import pytest
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.observability.event import Scope, TerminalReason  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.core.observability.span import LoomSpan  # noqa: E402
from loom.streaming.bytewax import _batch_spans as _batch_spans_mod  # noqa: E402
from loom.streaming.bytewax import _error_boundary as _boundary_mod  # noqa: E402
from loom.streaming.bytewax._batch_spans import (  # noqa: E402
    BatchSpan,
    BatchWindow,
    emit_batch_spans,
)
from loom.streaming.bytewax.handlers import _shared as _shared_mod  # noqa: E402
from loom.streaming.bytewax.handlers import scopes as _scopes  # noqa: E402
from loom.streaming.bytewax.handlers import storage as _storage  # noqa: E402
from loom.streaming.core._errors import ErrorKind  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.core._tracing import open_terminal_span  # noqa: E402
from tests.helpers.spans import (  # noqa: E402
    SpanRecorder,
    build_recorder,
    hex_trace,
    linked_span_ids,
    span_ids,
)
from tests.unit.streaming.compiler.cases import Order, Result  # noqa: E402

pytestmark = pytest.mark.bytewax

# Low 64 bits of 1: ``TraceIdRatioBased`` keeps this trace at any usable ratio.
_ALWAYS_SAMPLED = "f" * 16 + "0" * 15 + "1"

_TRACES = (
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
    "ccccccccccccccccccccccccccccccc3",
)


class _RecordingPartition:
    def __init__(self, *, fail: bool = False) -> None:
        self.batches: list[Sequence[Any]] = []
        self._fail = fail

    def write_batch(self, items: Sequence[Any]) -> int:
        self.batches.append(list(items))
        if self._fail:
            raise RuntimeError("sink down")
        return len(items)

    def close(self) -> int:
        return 0


class _UpperStep:
    def execute(self, message: Message[Any], **kwargs: object) -> Result:
        del kwargs
        return Result(value=str(message.payload.value).upper())


class _NoResources:
    """Resource lifecycle that owns nothing, so the node runs with no deps."""

    def open_batch(self) -> dict[str, object]:
        return {}

    def close_batch(self) -> None:
        return None


def _message(index: int, *, trace_id: str | None) -> Message[Result]:
    return Message(
        payload=Result(value=f"row-{index}"),
        meta=MessageMeta(
            message_id=f"msg-{index}",
            trace_id=trace_id,
            topic="orders.in",
            partition=0,
            offset=index,
        ),
    )


def _sink(recorder: SpanRecorder, partition: Any) -> _storage._StorageSinkPartition:
    return _storage._StorageSinkPartition(
        partition,
        node_name="results_sink",
        flow_name="orders",
        flow_run_id="run-1",
        observer=recorder.runtime,
    )


class TestOneTracePerMessage:
    """Criterion 1: birth, N node spans, and exactly one terminal, in one trace."""

    def test_birth_nodes_and_sink_death_share_the_inbound_trace_id(self) -> None:
        recorder = build_recorder()
        trace_id = _TRACES[0]
        message = _message(0, trace_id=trace_id)

        # Birth — as the Kafka consumer opens it, from the header trace id.
        recorder.runtime.open_span(
            Scope.TRANSPORT, "kafka_consume", trace_id=trace_id, root=True
        ).end()
        # Two nodes.
        for idx in (0, 1):
            with _shared_mod._observe_node(
                recorder.runtime, "orders", idx, "Upper", trace_id=trace_id
            ):
                pass
        # Death.
        _sink(recorder, _RecordingPartition()).write_batch([message])

        message_trace = {
            name: ids for name, ids in recorder.traces().items() if "terminal:sink_write" in ids
        }
        assert list(message_trace) == [trace_id]
        assert sorted(message_trace[trace_id]) == [
            "node:orders:0",
            "node:orders:1",
            "terminal:sink_write",
            "transport:kafka_consume",
        ]
        assert recorder.name_counts()["terminal:sink_write"] == 1, "a message must die exactly once"
        for span in recorder.spans():
            if hex_trace(span) == trace_id:
                assert span.parent is None, "flat roots: no fabricated parent edges"


class TestDeathByError:
    """Criterion 2: a node failure ends the message's trace with an error envelope."""

    def test_the_original_message_dies_where_its_envelope_is_built(self) -> None:
        recorder = build_recorder()
        message = _message(0, trace_id=_TRACES[0])
        boundary = _boundary_mod.ErrorBoundary(observer=recorder.runtime, flow="orders")

        def _fail() -> Message[Any]:
            raise ValueError("kaboom")

        result = _boundary_mod._execute_in_boundary(
            _boundary_mod._classify_task, message, _fail, boundary
        )

        assert isinstance(result, _boundary_mod.ErrorEnvelope)
        span = recorder.one("terminal:error_envelope")
        assert hex_trace(span) == _TRACES[0]
        assert span.parent is None
        assert span.attributes is not None
        assert span.attributes["terminal.reason"] == TerminalReason.ERROR_ENVELOPE.value
        assert span.attributes["error.kind"] == ErrorKind.TASK.value
        assert span.attributes["error.reason"] == "kaboom"
        # The envelope's onward journey is the envelope's life, not the
        # original continuing: exactly one death for the original.
        assert recorder.name_counts()["terminal:error_envelope"] == 1

    def test_the_terminal_carries_the_other_messages_lineage_as_attributes(self) -> None:
        recorder = build_recorder()
        message = Message(
            payload=Result(value="x"),
            meta=MessageMeta(
                message_id="msg-0",
                trace_id=_TRACES[0],
                parent_trace_id=_TRACES[1],
                causation_id="cause-1",
            ),
        )

        open_terminal_span(recorder.runtime, message.meta, TerminalReason.ERROR_ENVELOPE).end()

        span = recorder.one("terminal:error_envelope")
        assert span.attributes is not None
        assert span.attributes["loom.parent_trace_id"] == _TRACES[1]
        assert span.attributes["loom.causation_id"] == "cause-1"
        assert hex_trace(span) == _TRACES[0], (
            "another message's trace is an attribute, never the OTEL parent"
        )


class TestSilentDrop:
    """Criterion 3: a message expanded to zero rows is recorded, not lost."""

    def test_zero_rows_across_every_route_closes_the_trace(self) -> None:
        recorder = build_recorder()
        expanded = Message(
            payload={},  # type: ignore[arg-type]
            meta=MessageMeta(message_id="msg-0", trace_id=_TRACES[0]),
        )

        _shared_mod._register_row_fanout(
            expanded, None, frozenset({Order, Result}), False, recorder.runtime, "orders"
        )

        span = recorder.one("terminal:dropped_no_route")
        assert hex_trace(span) == _TRACES[0]
        assert span.parent is None
        assert span.attributes is not None
        assert span.attributes["loom.declared_routes"] == "Order,Result"
        assert span.attributes["loom.has_default_route"] is False

    def test_a_routed_message_costs_nothing(self) -> None:
        recorder = build_recorder()
        expanded = Message(
            payload={Order: [Order(order_id="a")]},  # type: ignore[arg-type]
            meta=MessageMeta(message_id="msg-0", trace_id=_TRACES[0]),
        )

        _shared_mod._register_row_fanout(
            expanded, None, frozenset({Order}), False, recorder.runtime, "orders"
        )

        assert recorder.names() == [], "the drop branch fired on a message that was routed"


class TestBatchNPlusOne:
    """Criterion 4: a 3-message batch write produces exactly 4 traces."""

    def test_three_message_traces_plus_one_batch_trace_with_three_links(self) -> None:
        recorder = build_recorder()
        messages = [_message(i, trace_id=_TRACES[i]) for i in range(3)]

        for index, message in enumerate(messages):
            with _shared_mod._observe_node(
                recorder.runtime, "orders", index, "Upper", trace_id=message.meta.trace_id
            ):
                pass
        _sink(recorder, _RecordingPartition()).write_batch(messages)

        traces = recorder.traces()
        assert len(traces) == 4, f"expected 3 message traces plus 1 batch trace, got {traces}"
        for index, trace_id in enumerate(_TRACES):
            assert sorted(traces[trace_id]) == [
                f"node:orders:{index}",
                "terminal:sink_write",
            ]

        batch = recorder.one("write:orders:results_sink")
        assert hex_trace(batch) not in _TRACES
        participations = recorder.named("terminal:sink_write")
        assert len(participations) == 3
        assert linked_span_ids(batch) == span_ids(participations), (
            "the batch must link to the very participation spans it just created"
        )
        assert batch.attributes is not None
        assert batch.attributes["loom.links_truncated"] is False
        batch_ids = {
            span.attributes["loom.batch_id"]
            for span in participations
            if span.attributes is not None
        }
        assert batch_ids == {batch.attributes["loom.batch_id"]}

    def test_the_link_bound_is_honoured_and_announced(self) -> None:
        recorder = build_recorder(max_span_links=2)
        messages = [_message(i, trace_id=_TRACES[i]) for i in range(3)]

        _sink(recorder, _RecordingPartition()).write_batch(messages)

        batch = recorder.one("write:orders:results_sink")
        assert len(batch.links) == 2
        assert batch.attributes is not None
        assert batch.attributes["loom.links_truncated"] is True
        assert recorder.name_counts()["terminal:sink_write"] == 3, (
            "truncating links must not drop anybody's death"
        )


class TestBatchGranularityNode:
    """Criterion 7: the same N+1 shape holds for batch-shaped node execution."""

    def test_each_message_gets_its_own_node_span_plus_one_batch_node_span(self) -> None:
        recorder = build_recorder()
        messages = [_message(i, trace_id=_TRACES[i]) for i in range(3)]

        result = _scopes._execute_with_step(
            recorder.runtime,
            "orders",
            4,
            "With",
            _NoResources(),  # type: ignore[arg-type]
            {},
            [_UpperStep()],  # type: ignore[list-item]
            None,
            cast("list[Message[Any]]", messages),
        )

        assert [str(cast(Result, message.payload).value) for message in result] == [
            "ROW-0",
            "ROW-1",
            "ROW-2",
        ]
        node_spans = recorder.named("node:orders:4")
        assert len(node_spans) == 4, "three participations plus the batch span"
        traces = recorder.traces()
        assert len(traces) == 4
        for trace_id in _TRACES:
            assert traces[trace_id] == ["node:orders:4"]

        batch = next(span for span in node_spans if hex_trace(span) not in _TRACES)
        participations = [span for span in node_spans if hex_trace(span) in _TRACES]
        assert linked_span_ids(batch) == span_ids(participations)


class TestSampling:
    """Criteria 5 and 6: complete traces for a subset, never partial traces for all."""

    def _sampled_split(self, ratio: float) -> tuple[SpanRecorder, list[Message[Result]]]:
        recorder = build_recorder(sampler=ParentBased(TraceIdRatioBased(ratio)))
        # A seeded RNG, not a counter: the ratio sampler decides on the trace
        # id's low bits, so sequential ids would all fall below the threshold
        # and the split under test would never happen.
        rng = Random(20240517)
        messages = [
            _message(index, trace_id=f"{rng.getrandbits(128):032x}") for index in range(200)
        ]
        return recorder, messages

    def test_every_exported_batch_span_links_to_exactly_its_exported_participants(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The batch span is a root of its own, so the sampler judges it too.

        What must hold at any ratio is that a batch span never advertises an
        edge to a span nobody exported, and never omits one that was.
        """
        recorder, messages = self._sampled_split(0.05)
        # The batch span is a root in a trace of its own, so the ratio sampler
        # judges it on its own id. Pinning that id to one the sampler always
        # keeps isolates the invariant under test from the batch's own coin
        # flip — which the module docstring documents as a real caveat.
        monkeypatch.setattr(_batch_spans_mod, "generate_trace_id", lambda: _ALWAYS_SAMPLED)

        _sink(recorder, _RecordingPartition()).write_batch(messages)

        participations = recorder.named("terminal:sink_write")
        assert 0 < len(participations) < len(messages), (
            "the ratio sampler kept everything or nothing; the split is not exercised"
        )
        batch = recorder.one("write:orders:results_sink")
        assert linked_span_ids(batch) == span_ids(participations), (
            "a link to an unexported span advertises an edge to nothing"
        )
        assert len(batch.links) == len(participations)

    def test_a_sampled_message_yields_its_whole_trace_and_an_unsampled_one_yields_none(
        self,
    ) -> None:
        recorder, messages = self._sampled_split(0.05)
        sink = _sink(recorder, _RecordingPartition())

        for index, message in enumerate(messages):
            with _shared_mod._observe_node(
                recorder.runtime, "orders", index, "Upper", trace_id=message.meta.trace_id
            ):
                pass
            sink.write_batch([message])

        per_message = {
            trace_id: names
            for trace_id, names in recorder.traces().items()
            if trace_id in {message.meta.trace_id for message in messages}
        }
        assert per_message, "the sampler dropped every message trace"
        for trace_id, names in per_message.items():
            assert "terminal:sink_write" in names, f"{trace_id} is a partial trace"
            assert any(name.startswith("node:") for name in names), (
                f"{trace_id} kept its death but lost its node — a partial trace"
            )
        assert len(per_message) < len(messages), "nothing was left unsampled"


class TestBatchWindow:
    def test_spans_opened_after_the_work_still_cover_the_real_window(self) -> None:
        recorder = build_recorder()
        window = BatchWindow(started_ns=1_000_000_000, ended_ns=1_500_000_000)

        def _open(meta: MessageMeta, attributes: Mapping[str, object], started: int) -> LoomSpan:
            return open_terminal_span(
                recorder.runtime,
                meta,
                TerminalReason.SINK_WRITE,
                start_time_ns=started,
                attributes=attributes,
            )

        emit_batch_spans(
            recorder.runtime,
            [_message(0, trace_id=_TRACES[0]).meta],
            batch=BatchSpan(scope=Scope.WRITE, name="orders:sink", attributes={}),
            open_participation=_open,
            window=window,
            error=None,
        )

        for span in recorder.spans():
            assert span.start_time == window.started_ns
            assert span.end_time == window.ended_ns
            assert span.attributes is not None
            assert span.attributes["duration_ms"] == pytest.approx(500.0)


class TestFailedBatch:
    def test_a_failed_write_records_the_failure_on_every_span_of_the_batch(self) -> None:
        recorder = build_recorder()
        messages = [_message(i, trace_id=_TRACES[i]) for i in range(2)]

        sink = _sink(recorder, _RecordingPartition(fail=True))

        with pytest.raises(RuntimeError, match="sink down"):
            sink.write_batch(messages)

        assert len(recorder.spans()) == 3
        for span in recorder.spans():
            assert span.status.is_ok is False
            assert span.status.description == "sink down"
            assert [event.name for event in span.events] == ["exception"]


class TestNoopRuntimeControl:
    """The control: spans opened on a no-op runtime reach no exporter.

    Without it, an assertion that some span *is* exported could be satisfied by
    an exporter that records everything indiscriminately.
    """

    def test_a_span_on_the_noop_runtime_never_reaches_the_exporter(self) -> None:
        recorder = build_recorder()

        _sink(recorder, _RecordingPartition()).write_batch([_message(0, trace_id=_TRACES[0])])
        with ObservabilityRuntime.noop().span(Scope.NODE, "orders:0", trace_id=_TRACES[0]):
            pass

        assert recorder.name_counts()["terminal:sink_write"] == 1
        assert recorder.name_counts()["node:orders:0"] == 0
