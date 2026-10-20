"""Commit accounting across nodes that change the record count.

Every node that turns one input record into N outputs shares one hazard: all N
outputs carry the same source offset, and each one completes that offset when it
reaches a terminal. The tracker expects exactly one completion per record, so a
node that fans out must raise that expectation before any output can be
completed, and a node that produces nothing must release the record itself.

Both failures are silent, which is why the unit classes assert them against a
real ``KafkaCommitTracker`` and its committed offsets rather than against a spy:

- fan-out without a fork commits the offset after the *first* output, losing the
  remaining N-1 on a crash — the consumer group has already moved past them;
- zero outputs without a completion freezes the partition's watermark forever.

``TestEveryPathCompletesOnce`` runs whole flows on the Bytewax runtime instead,
with a logging tracker and a testing source in place of the Kafka ones.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from confluent_kafka import TopicPartition

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

import bytewax.operators as op  # noqa: E402
from bytewax.dataflow import Dataflow  # noqa: E402
from bytewax.run import cli_main  # noqa: E402
from bytewax.testing import TestingSink, TestingSource  # noqa: E402

from loom.core.config import ConfigContext  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming import (  # noqa: E402
    Backend,
    BatchExpandStep,
    BatchStep,
    Broadcast,
    BroadcastRoute,
    CollectBatch,
    Drain,
    ExpandRoutes,
    ExpandStep,
    Explode,
    ForEach,
    Fork,
    ForkRoute,
    FromMultiTypeTopic,
    FromTopic,
    IntoTable,
    IntoTopic,
    Process,
    RecordStep,
    Route,
    Router,
    Selector,
    StreamFlow,
    With,
    WithAsync,
    msg,
)
from loom.streaming.bytewax import RuntimeConfigurationError, _adapter  # noqa: E402
from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker  # noqa: E402
from loom.streaming.bytewax._runtime_io import (  # noqa: E402
    build_runtime_error_sinks,
    build_runtime_sink,
)
from loom.streaming.bytewax.handlers import _shared  # noqa: E402
from loom.streaming.bytewax.handlers import routing as _routing  # noqa: E402
from loom.streaming.bytewax.handlers import steps as _steps  # noqa: E402
from loom.streaming.bytewax.handlers import storage as _storage  # noqa: E402
from loom.streaming.compiler import (  # noqa: E402
    CompilationError,
    StreamingErrorCode,
    compile_flow,
)
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.kafka import MessageDescriptor, MsgspecCodec, build_message  # noqa: E402
from loom.streaming.kafka._record import KafkaRecord  # noqa: E402
from tests.unit.streaming.bytewax.cases import (  # noqa: E402
    Item,
    Order,
    Result,
    UpperItem,
    build_compiled_plan,
)
from tests.unit.streaming.kafka.fakes import ProducerBackendStub  # noqa: E402

pytestmark = pytest.mark.bytewax

TOPIC = "orders.in"
PARTITION = 0
OFFSET = 7


class _Committer:
    """Minimal committer capturing what the tracker sent to the broker."""

    def __init__(self) -> None:
        self.commits: list[list[TopicPartition]] = []

    def commit_offset(
        self, partitions: list[TopicPartition], *, asynchronous: bool = False
    ) -> None:
        del asynchronous
        self.commits.append(list(partitions))


def _sourced_message(payload: Order, offset: int = OFFSET) -> Message[Order]:
    """Build a message carrying real Kafka coordinates, as the source emits."""
    return Message(
        payload=payload,
        meta=MessageMeta(message_id="m-1", topic=TOPIC, partition=PARTITION, offset=offset),
    )


def _tracker(*offsets: int) -> tuple[KafkaCommitTracker, _Committer]:
    tracker = KafkaCommitTracker()
    committer = _Committer()
    tracker.attach_partition(TOPIC, PARTITION, committer, None)
    for offset in offsets:
        tracker.register_record(
            KafkaRecord(topic=TOPIC, key=None, value=b"raw", partition=PARTITION, offset=offset)
        )
    return tracker, committer


def _complete(tracker: KafkaCommitTracker, results: list[Any]) -> None:
    """Complete every produced output, as its terminal sink would."""
    for item in results:
        tracker.complete(item.meta.topic, item.meta.partition, item.meta.offset)


def _ctx(tracker: KafkaCommitTracker | None) -> Any:
    return SimpleNamespace(
        plan=SimpleNamespace(name="orders"),
        current_path=(),
        flow_runtime=ObservabilityRuntime.noop(),
        commit_tracker=tracker,
    )


def _drive(monkeypatch: pytest.MonkeyPatch, module: Any, inputs: Any) -> list[Any]:
    """Run one handler's map step directly and return what it produced."""
    produced: list[Any] = []

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        del step_id, stream
        result = fn(inputs)
        produced.append(result)
        return result

    monkeypatch.setattr(module, "bw_map", _bw_map)
    monkeypatch.setattr("bytewax.operators.flat_map", lambda step_id, stream, fn: fn(stream))
    monkeypatch.setattr(module, "_split_node_result", lambda stream, *_: stream, raising=False)
    return produced


class _ThreeWayExpand:
    """Expand step turning one order into three results."""

    def execute(self, message: Message[Order], **_: object) -> list[Result]:
        return [Result(value=f"{message.payload.order_id}-{index}") for index in range(3)]


class _EmptyExpand:
    """Expand step that legitimately produces nothing for this record."""

    def execute(self, message: Message[Order], **_: object) -> list[Result]:
        del message
        return []


class TestExpandingStepAccounting:
    """One record in, N records out."""

    def test_offset_is_not_committed_until_every_output_completes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tracker, committer = _tracker(OFFSET)
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _ThreeWayExpand(), 1, _ctx(tracker))
        results = produced[0]
        assert len(results) == 3

        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "the offset was released after the first of three outputs; a crash here "
            "loses the other two permanently"
        )

        _complete(tracker, results[1:])
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]
        assert len(committer.commits) == 1

    def test_a_record_producing_nothing_is_released(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tracker, _ = _tracker(OFFSET)
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _EmptyExpand(), 1, _ctx(tracker))

        assert produced[0] == []
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "a record that produced no output was never completed: the watermark froze"

    def test_accounting_is_inert_under_at_most_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """With no tracker the node must behave exactly as before."""
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _ThreeWayExpand(), 1, _ctx(None))

        assert len(produced[0]) == 3


class TestExpandRoutesAccounting:
    """One record in, one message per produced ROW out — not per declared route.

    The two counts are unrelated: a flow may declare three routes and receive a
    record that yields a single row, or declare one route that yields twenty.
    Accounting by the declared count froze the partition in the first case and
    released the offset early in the second.
    """

    @staticmethod
    def _expanded(rows_by_type: dict[type, list[Any]]) -> Message[Any]:
        return Message(
            payload=rows_by_type,
            meta=MessageMeta(message_id="m-1", topic=TOPIC, partition=PARTITION, offset=OFFSET),
        )

    def test_fewer_rows_than_routes_still_commits(self) -> None:
        tracker, _ = _tracker(OFFSET)
        declared = frozenset({Order, Result})

        _routing._register_row_fanout(
            self._expanded({Order: [Order(order_id="a")]}),
            tracker,
            declared,
            False,
            ObservabilityRuntime.noop(),
            "orders",
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "two routes were declared but one row was produced: the watermark froze"

    def test_more_rows_than_routes_waits_for_every_row(self) -> None:
        tracker, _ = _tracker(OFFSET)
        declared = frozenset({Order})
        rows = [Order(order_id=str(index)) for index in range(4)]

        _routing._register_row_fanout(
            self._expanded({Order: rows}),
            tracker,
            declared,
            False,
            ObservabilityRuntime.noop(),
            "orders",
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "one route produced four rows and the offset was released after the first"
        )
        for _ in range(3):
            tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]

    def test_no_rows_releases_the_record(self) -> None:
        tracker, _ = _tracker(OFFSET)

        _routing._register_row_fanout(
            self._expanded({}),
            tracker,
            frozenset({Order}),
            False,
            ObservabilityRuntime.noop(),
            "orders",
        )

        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "the expander produced nothing and nobody ever completed the record"

    def test_default_route_rows_are_counted(self) -> None:
        """Undeclared types reach the default route and must be accounted for."""
        tracker, _ = _tracker(OFFSET)

        _routing._register_row_fanout(
            self._expanded({Order: [Order(order_id="a")], Result: [Result(value="x")]}),
            tracker,
            frozenset({Order}),
            True,
            ObservabilityRuntime.noop(),
            "orders",
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "the default route's row was not counted, so the offset was released early"
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]

    def test_undeclared_rows_are_ignored_without_a_default_route(self) -> None:
        """With no default route those rows reach nothing and must not be expected."""
        tracker, _ = _tracker(OFFSET)

        _routing._register_row_fanout(
            self._expanded({Order: [Order(order_id="a")], Result: [Result(value="x")]}),
            tracker,
            frozenset({Order}),
            False,
            ObservabilityRuntime.noop(),
            "orders",
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)

        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ], "a row with no route was counted as a pending completion: the watermark froze"


class TestSinkTrackingContract:
    """A sink that cannot receive the tracker must stop the flow, not the commits."""

    def test_untrackable_sink_is_rejected_at_assembly(self) -> None:
        class _UserSink:
            """A sink a user wrote without knowing about commit tracking."""

        tracker, _ = _tracker()
        sink = _UserSink()

        with pytest.raises(RuntimeConfigurationError, match="bind_commit_tracker") as exc_info:
            _adapter._bind_commit_tracker_object(sink, tracker)

        issue = exc_info.value.issues[0]
        assert issue.code is StreamingErrorCode.SINK_CANNOT_TRACK_COMMITS
        assert (issue.component, issue.field) == ("_UserSink", "streaming.delivery")

    def test_untrackable_sink_is_fine_under_at_most_once(self) -> None:
        class _UserSink:
            """No tracker means nothing to bind and nothing to guarantee."""

        _adapter._bind_commit_tracker_object(_UserSink(), None)

    def test_a_compiled_into_topic_without_a_runtime_sink_is_refused(self) -> None:
        """A partial terminal_sinks override must not turn an IntoTopic into a drop sink."""
        flow: StreamFlow[Any, Any] = StreamFlow(
            name="paths",
            source=FromTopic("items", payload=Item),
            process=Process(
                Router.by(
                    msg.payload.value,
                    routes={"item-0": Process(_topic("t.a"))},
                    default=Process(_topic("t.b")),
                )
            ),
        )
        plan = compile_flow(flow, config=ConfigContext.from_dict(_PATH_CONFIG))
        covered, missing = sorted(plan.terminal_sinks)
        assert plan.terminal_sinks[missing].topic == "t.b"

        with pytest.raises(RuntimeConfigurationError, match="no runtime sink") as exc_info:
            _adapter.build_dataflow_with_shutdown(
                plan, source=TestingSource([]), terminal_sinks={covered: TestingSink([])}
            )

        issue = exc_info.value.issues[0]
        assert issue.code is StreamingErrorCode.TERMINAL_SINK_MISSING
        assert "(topic t.b)" in issue.message

    def test_a_compiled_inline_into_topic_without_a_runtime_sink_is_refused(self) -> None:
        """Without the check, the inline write was skipped and nothing was written."""
        plan = build_compiled_plan(terminal_sinks={(0, 1): _compiled_topic("t.inline")})
        ctx = _adapter._BuildContext(
            plan=plan,
            bridge=None,
            flow_runtime=ObservabilityRuntime.noop(),
            flow_run_id="run-1",
            terminal_sinks={},
        )

        with pytest.raises(RuntimeConfigurationError, match=r"\(topic t\.inline\)") as exc_info:
            ctx.inline_sink_partition_for((0, 1))

        assert exc_info.value.issues[0].code is StreamingErrorCode.TERMINAL_SINK_MISSING

    def test_an_inline_sink_that_cannot_skip_commits_is_rejected(self) -> None:
        """Its partitions would complete offsets of messages that go on after the write."""

        class _UserSink:
            """A tracker-aware sink that cannot be told its partitions write inline."""

        tracker, _ = _tracker()
        ctx = _adapter._BuildContext(
            plan=cast(Any, SimpleNamespace(name="orders", terminal_sinks={})),
            bridge=None,
            flow_runtime=ObservabilityRuntime.noop(),
            flow_run_id="run-1",
            terminal_sinks={(0, 1): _UserSink()},
            commit_tracker=tracker,
        )

        with pytest.raises(RuntimeConfigurationError, match="build_inline_partition") as exc_info:
            ctx.inline_sink_partition_for((0, 1))

        assert exc_info.value.issues[0].code is StreamingErrorCode.INLINE_SINK_CANNOT_SKIP_COMMITS


def _guard_context(**plan_parts: Any) -> _adapter._BuildContext:
    """Build a context around a hand-built plan, with no validation in front of it."""
    sink = TestingSink([])
    plan = build_compiled_plan(**plan_parts)
    return _adapter._BuildContext(
        plan=plan,
        bridge=None,
        flow_runtime=ObservabilityRuntime.noop(),
        flow_run_id="run-1",
        sink=sink,
        terminal_sinks=dict.fromkeys(plan.terminal_sinks, sink),
    )


def _compiled_topic(topic: str) -> Any:
    return build_compiled_plan(output=IntoTopic(topic, payload=Result)).output


class TestWiringGuards:
    """Wiring refuses the shapes validation rejects, if a plan ever skips validation."""

    @pytest.mark.parametrize(
        ("wire", "message"),
        [
            pytest.param(
                lambda: _guard_context(
                    terminal_sinks={(0,): _compiled_topic("t.a")}
                ).wire_branch_terminal("route", None, (0,)),
                r"output compiled for path \(0,\) is never written",
                id="branch output after a path end",
            ),
            pytest.param(
                lambda: _guard_context(output=IntoTopic("t.out", payload=Result)).wire_flow_output(
                    None
                ),
                "flow.output is never written",
                id="flow output after a path end",
            ),
            pytest.param(
                lambda: _adapter._wire_process(object(), (Drain(), UpperItem()), _guard_context()),
                "Drain ended every path, so UpperItem after it would receive nothing",
                id="node after a path end",
            ),
            pytest.param(
                lambda: _adapter._wire_process(
                    op.input("in", Dataflow("guard"), TestingSource([])),
                    (
                        Router.by(
                            msg.payload.value,
                            routes={"item-0": Process(Drain())},
                            default=Process(Drain()),
                        ),
                        UpperItem(),
                    ),
                    _guard_context(),
                ),
                "Router ended every path, so UpperItem after it would receive nothing",
                id="node after a router that ends every path",
            ),
        ],
    )
    def test_a_guard_refuses_wiring_after_a_path_end(
        self, wire: Callable[[], object], message: str
    ) -> None:
        with pytest.raises(RuntimeError, match=message):
            wire()


class TestErrorEnvelopeAccounting:
    """A failed record still owes exactly one completion, from its error route."""

    def test_an_error_envelope_counts_as_the_records_single_output(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The envelope carries the original offset and its sink completes it.

        Counting only successful messages would have completed the record here
        as "produced nothing", releasing the offset before the error route had
        written it anywhere.
        """

        class _FailingExpand:
            def execute(self, message: Message[Order], **_: object) -> list[Result]:
                del message
                raise RuntimeError("expander-boom")

        tracker, _ = _tracker(OFFSET)
        produced = _drive(monkeypatch, _steps, _sourced_message(Order(order_id="ab")))
        _steps._apply_expand_step("in", _FailingExpand(), 1, _ctx(tracker))
        results = produced[0]

        assert len(results) == 1
        assert not isinstance(results[0], Message)
        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "the record was released before its error envelope reached a sink"
        )

        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]


class TestExpandRoutesWiring:
    """The accounting is only correct if the node actually wires it in."""

    def test_the_node_registers_row_fanout_before_extracting_routes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Order matters: the fork must be registered before any row can complete."""

        class _Expander:
            @staticmethod
            def expand(payload: Order) -> dict[type, list[Any]]:
                del payload
                return {Result: [Result(value="a"), Result(value="b")]}

        node = ExpandRoutes(
            expander=cast(Any, _Expander),
            routes={Result: cast(Any, SimpleNamespace(nodes=()))},
        )
        tracker, _ = _tracker(OFFSET)
        steps_run: list[str] = []

        def _bw_map(step_id: str, stream: object, fn: Any) -> object:
            steps_run.append(step_id)
            return fn(stream)

        def _bw_flat_map(step_id: str, stream: object, fn: Any) -> object:
            steps_run.append(step_id)
            return fn(stream)

        monkeypatch.setattr(_routing, "bw_map", _bw_map)
        monkeypatch.setattr(_routing, "bw_flat_map", _bw_flat_map)
        ctx = _ctx(tracker)
        ctx.wire_branch = lambda *args, **kwargs: None

        _routing._apply_expand_routes(_sourced_message(Order(order_id="ab")), node, 4, ctx)

        assert any("fanout" in step for step in steps_run), "the fanout step was never wired"
        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [], (
            "two rows were produced but the offset was released after the first"
        )
        tracker.complete(TOPIC, PARTITION, OFFSET)
        assert tracker.flush_partition(TOPIC, PARTITION) == [
            TopicPartition(TOPIC, PARTITION, OFFSET + 1)
        ]


class TestCommitKeyGuards:
    """Items that account for no source offset must not be counted."""

    def test_an_envelope_without_its_original_is_not_counted(self) -> None:
        from loom.streaming.core._errors import ErrorEnvelope, ErrorKind

        envelope: Any = ErrorEnvelope(
            kind=ErrorKind.TASK, reason="boom", payload_type="Order", original_message=None
        )

        assert _shared._commit_key(envelope) is None

    def test_an_unknown_item_type_is_not_counted(self) -> None:
        assert _shared._commit_key(object()) is None

    def test_a_message_without_kafka_coordinates_is_not_counted(self) -> None:
        assert (
            _shared._commit_key(
                Message(payload=Order(order_id="a"), meta=MessageMeta(message_id="m-1"))
            )
            is None
        )


# --- Each listed path shape ends in exactly one completion per share -------------
#
# The flows below run on the real Bytewax runtime with the real adapter, the real
# Kafka sinks and the real codec. The confluent producer and the storage
# partition are faked, the commit tracker is a logging stand-in and the source is
# a TestingSource; the producer, the partition and the tracker write into one
# ordered log of writes, flushes, forks and completions. They compile under
# delivery=at_least_once, except the one row that pins what an at-least-once
# compile rule refuses. _PATH_CASES is the list of shapes covered.

_PATH_CONFIG = {
    "kafka": {
        "consumer": {"brokers": ["localhost:9092"], "group_id": "g", "topics": ["items"]},
        # No default topic, so every IntoTopic writes to the topic it names.
        "producer": {"brokers": ["localhost:9092"]},
    },
    "streaming": {"sinks": {"items_table": {"uri": "memory://items"}}},
}
_ITEM_OFFSET = re.compile(rb"item-(\d+)", re.IGNORECASE)

Event = tuple[str, object, int | None]
"""``(kind, where, offset)``: produce, flush, write, fork or complete."""


class _Row(LoomStruct):
    value: str


class _Other(LoomStruct):
    value: str


class _FailOnItem1(RecordStep[Item, Item]):
    """Fail on the item at offset 1 and pass the others through."""

    def execute(self, message: Message[Item], **kwargs: object) -> Item:
        del kwargs
        if message.payload.value == "item-1":
            raise RuntimeError("item-1 is rejected")
        return message.payload


class _PayloadType(Selector[Any]):
    """Select the payload type, for type-keyed routing."""

    def select(self, message: Message[Any]) -> object:
        return type(message.payload)


class _TwiceExceptItem1(ExpandStep[Item, Item]):
    """Expand every item into two, except item 1, which expands into none."""

    def execute(self, message: Message[Item], **kwargs: object) -> list[Item]:
        del kwargs
        return [] if message.payload.value == "item-1" else [message.payload] * 2


class _UpperBatch(BatchStep[Item, Item]):
    """Uppercase every item of a batch."""

    def execute(self, messages: list[Message[Item]], **kwargs: object) -> list[Item]:
        del kwargs
        return [Item(value=message.payload.value.upper()) for message in messages]


class _TwoNoneOneInBatch(BatchExpandStep[Item, Item]):
    """Expand item 0 of a batch into two, item 1 into none, and any other item into one."""

    def execute(self, messages: list[Message[Item]], **kwargs: object) -> list[list[Item]]:
        del kwargs
        counts = {"item-0": 2, "item-1": 0}
        return [[message.payload] * counts.get(message.payload.value, 1) for message in messages]


class _NothingFromBatch(BatchExpandStep[Item, Item]):
    """Expand every message of a batch into nothing."""

    def execute(self, messages: list[Message[Item]], **kwargs: object) -> list[list[Item]]:
        del kwargs
        return [[] for _ in messages]


class _TwoRows:
    """Expand every item into two rows of one type."""

    outputs: ClassVar[tuple[type, ...]] = (_Row,)

    @classmethod
    def expand(cls, event: Item) -> dict[type, list[Any]]:
        return {_Row: [_Row(value=event.value), _Row(value=event.value)]}


class _RowsByOffset:
    """Item 0 expands to a routed and an unrouted row, item 1 to none, item 2 to an unrouted one."""

    outputs: ClassVar[tuple[type, ...]] = (_Row, _Other)

    @classmethod
    def expand(cls, event: Item) -> dict[type, list[Any]]:
        if event.value == "item-0":
            return {_Row: [_Row(value=event.value)], _Other: [_Other(value=event.value)]}
        if event.value == "item-2":
            return {_Other: [_Other(value=event.value)]}
        return {}


def _offset_in(value: bytes | str) -> int:
    raw = value.encode() if isinstance(value, str) else value
    found = _ITEM_OFFSET.search(raw)
    assert found is not None, f"no item offset in {raw!r}"
    return int(found.group(1))


class _LoggingProducer(ProducerBackendStub):
    """One sink's confluent producer; its produces and flushes go to the shared log.

    A flush fails when it would deliver a record to ``t.unreachable``, so a sink
    with a DLQ topic diverts that batch.
    """

    def __init__(self, log: list[Event]) -> None:
        super().__init__({})
        self._log = log
        self._unflushed: set[str] = set()

    def produce(
        self,
        *,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]] | None,
        timestamp: int | None = None,
        on_delivery: Any = None,
    ) -> None:
        super().produce(
            topic=topic,
            key=key,
            value=value,
            headers=headers,
            timestamp=timestamp,
            on_delivery=on_delivery,
        )
        self._unflushed.add(topic)
        self._log.append(("produce", (id(self), topic), _offset_in(value)))

    def flush(self, timeout: float | None = None) -> None:
        super().flush(timeout)
        unreachable = "t.unreachable" in self._unflushed
        self._unflushed.clear()
        if unreachable:
            raise RuntimeError("t.unreachable is down")
        self._log.append(("flush", id(self), None))


class _LoggingTablePartition:
    """Storage partition that buffers every item until its final flush at close."""

    def __init__(self, log: list[Event]) -> None:
        self._log = log
        self._buffer: list[Any] = []

    def write_batch(self, items: Sequence[Any]) -> int:
        self._buffer.extend(items)
        return 0

    def close(self) -> int:
        for item in self._buffer:
            self._log.append(("write", "items_table", _offset_in(item.value)))
        flushed, self._buffer = len(self._buffer), []
        return flushed


class _LoggingTracker:
    """Commit tracker that logs the forks and completions it receives."""

    def __init__(self, log: list[Event]) -> None:
        self._log = log

    def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
        self._log.append(("fork", extra_outputs, offset))

    def complete(self, topic: str, partition: int, offset: int) -> None:
        self._log.append(("complete", topic, offset))


class _TrackedTestingSource(TestingSource):
    """Testing source that accepts the commit tracker the adapter binds."""

    def bind_commit_tracker(self, tracker: object) -> None:
        del tracker


def _item(offset: int) -> Message[Item]:
    return Message(
        payload=Item(value=f"item-{offset}"),
        meta=MessageMeta(message_id=f"m-{offset}", topic="items", partition=0, offset=offset),
    )


def _encoded(payload: Item | _Other, offset: int) -> KafkaRecord[bytes]:
    """Encode *payload* the way a producer writes it, with its own message type."""
    descriptor = MessageDescriptor(
        message_type=type(payload).loom_message_type(), message_version=1
    )
    codec: MsgspecCodec[Any] = MsgspecCodec()
    value = codec.encode(build_message(payload, descriptor, produced_at_ms=1000))
    return KafkaRecord(topic="items", key=None, value=value, partition=0, offset=offset)


def _undecodable(offset: int) -> KafkaRecord[bytes]:
    value = f"item-{offset} is not an envelope".encode()
    return KafkaRecord(topic="items", key=None, value=value, partition=0, offset=offset)


def _run_paths(
    process: Process[Any, Any],
    items: Sequence[object],
    monkeypatch: pytest.MonkeyPatch,
    *,
    output: IntoTopic[Any] | None = None,
    errors: IntoTopic[Any] | None = None,
    source: Any = None,
    delivery: str = "at_least_once",
) -> list[Event]:
    """Run one flow and return the ordered log of its writes and completions."""
    log: list[Event] = []
    monkeypatch.setattr(
        "loom.streaming.kafka.client._producer._Producer", lambda config: _LoggingProducer(log)
    )
    monkeypatch.setattr(
        IntoTable, "build_partition", lambda self, *args, **kwargs: _LoggingTablePartition(log)
    )
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="paths",
        source=source if source is not None else FromTopic("items", payload=Item),
        process=process,
        output=output,
        errors=errors,
    )
    config = {**_PATH_CONFIG, "kafka": {**_PATH_CONFIG["kafka"]}}
    config["kafka"]["consumer"] = {**_PATH_CONFIG["kafka"]["consumer"], "delivery": delivery}
    plan = compile_flow(flow, config=ConfigContext.from_dict(config))
    built = _adapter.build_dataflow_with_shutdown(
        plan,
        source=_TrackedTestingSource(list(items)),
        sink=build_runtime_sink(plan.output) if plan.output is not None else None,
        error_sinks=build_runtime_error_sinks(plan.error_routes),
        commit_tracker=cast(Any, _LoggingTracker(log)),
    )
    try:
        cli_main(built.dataflow, workers_per_process=1)  # type: ignore[no-untyped-call]
    finally:
        built.shutdown()
    return log


def _topic(name: str) -> IntoTopic[Any]:
    return IntoTopic(name, payload=Item)


def _table(payload: type[Any] = Item) -> Any:
    """Build the table sink; ``Process`` does not type ``IntoSink`` nodes."""
    return IntoTable(payload=payload, table="items", backend=Backend.DELTA, name="items_table")


@dataclass(frozen=True)
class _PathCase:
    """One flow shape and where each of its three messages must end up.

    Args:
        process: Process of the flow under test.
        writes: Every ``(topic or table, offset)`` the run must write, as a multiset.
        shares: Completions each offset owes: one, plus one per extra fork share;
            per offset when the offsets differ.
        output: Flow output, when the shape has one.
        errors: Flow error route, when the shape has one.
        items: Source items; three plain items unless the shape needs others.
        source: Flow source; ``FromTopic`` of ``Item`` unless the shape needs another.
        delivery: Consumer delivery the flow compiles under.
    """

    process: Callable[[], Process[Any, Any]]
    writes: tuple[tuple[str, int], ...]
    shares: int | dict[int, int] = 1
    output: str | None = None
    errors: str | None = None
    items: Callable[[], list[object]] = lambda: [_item(offset) for offset in range(3)]
    source: Callable[[], Any] | None = None
    delivery: str = "at_least_once"


def _each(topic: str, *offsets: int) -> tuple[tuple[str, int], ...]:
    return tuple((topic, offset) for offset in offsets)


_PATH_CASES = {
    "into_topic": _PathCase(
        lambda: Process(UpperItem(), _topic("t.a")),
        _each("t.a", 0, 1, 2),
    ),
    "dlq_diversion": _PathCase(
        lambda: Process(IntoTopic("t.unreachable", payload=Item, dlq="t.dlq")),
        _each("t.unreachable", 0, 1, 2) + _each("t.dlq", 0, 1, 2),
    ),
    "inline_then_output": _PathCase(
        lambda: Process(WithAsync(process=Process(UpperItem(), _topic("t.inline")))),
        _each("t.inline", 0, 1, 2) + _each("t.out", 0, 1, 2),
        output="t.out",
    ),
    "inline_only": _PathCase(
        lambda: Process(WithAsync(process=Process(UpperItem(), _topic("t.inline")))),
        _each("t.inline", 0, 1, 2),
    ),
    "fork_branch_ending_in_scope": _PathCase(
        lambda: Process(
            Fork.when(
                [
                    ForkRoute(
                        when=msg.payload.value == "item-0",
                        process=Process(WithAsync(process=Process(_topic("t.inline")))),
                    )
                ],
                default=Process(_topic("t.a")),
            )
        ),
        _each("t.inline", 0) + _each("t.a", 1, 2),
    ),
    "expand_routes_route_ending_in_scope": _PathCase(
        lambda: Process(
            ExpandRoutes(
                expander=_TwoRows,
                routes={
                    _Row: Process(WithAsync(process=Process(IntoTopic("t.inline", payload=_Row))))
                },
            )
        ),
        _each("t.inline", 0, 0, 1, 1, 2, 2),
        shares=2,
    ),
    "expand_routes_route_ending_in_into_topic": _PathCase(
        lambda: Process(
            ExpandRoutes(expander=_TwoRows, routes={_Row: Process(IntoTopic("t.a", payload=_Row))})
        ),
        _each("t.a", 0, 0, 1, 1, 2, 2),
        shares=2,
    ),
    "broadcast_routes_with_output": _PathCase(
        lambda: Process(
            Broadcast(
                BroadcastRoute(process=Process(UpperItem()), output=_topic("t.a")),
                BroadcastRoute(process=Process(UpperItem()), output=_topic("t.b")),
            )
        ),
        _each("t.a", 0, 1, 2) + _each("t.b", 0, 1, 2),
        shares=2,
    ),
    "broadcast_routes_without_output": _PathCase(
        lambda: Process(
            Broadcast(
                BroadcastRoute(process=Process(_topic("t.a"))),
                BroadcastRoute(process=Process(UpperItem(), _topic("t.b"))),
            )
        ),
        _each("t.a", 0, 1, 2) + _each("t.b", 0, 1, 2),
        shares=2,
    ),
    "router_terminal_branch": _PathCase(
        lambda: Process(
            Router.by(
                msg.payload.value,
                routes={"item-0": Process(UpperItem(), _topic("t.a"))},
                default=Process(_topic("t.b")),
            )
        ),
        _each("t.a", 0) + _each("t.b", 1, 2),
    ),
    "router_terminal_branch_with_output": _PathCase(
        lambda: Process(
            Router.by(
                msg.payload.value,
                routes={"item-0": Process(_topic("t.a"))},
                default=Process(UpperItem()),
            )
        ),
        _each("t.a", 0) + _each("t.out", 1, 2),
        output="t.out",
    ),
    "sync_with_inline_only": _PathCase(
        lambda: Process(With(process=Process(UpperItem(), _topic("t.inline")))),
        _each("t.inline", 0, 1, 2),
    ),
    "drain": _PathCase(lambda: Process(Drain()), ()),
    "error_route": _PathCase(
        lambda: Process(_FailOnItem1(), _topic("t.a")),
        _each("t.a", 0, 2) + _each("t.err", 1),
        errors="t.err",
    ),
    "error_without_route": _PathCase(
        lambda: Process(_FailOnItem1(), _topic("t.a")),
        _each("t.a", 0, 2),
    ),
    "decode_error_with_wire_route": _PathCase(
        lambda: Process(_topic("t.a")),
        _each("t.a", 0, 1) + _each("t.err", 2),
        errors="t.err",
        items=lambda: [_item(0), _item(1), _undecodable(2)],
    ),
    "fork_by": _PathCase(
        lambda: Process(
            Fork.by(
                msg.payload.value,
                {"item-0": Process(_topic("t.a"))},
                default=Process(_topic("t.b")),
            )
        ),
        _each("t.a", 0) + _each("t.b", 1, 2),
    ),
    "fork_without_default": _PathCase(
        lambda: Process(
            Fork.when(
                [ForkRoute(when=msg.payload.value == "item-0", process=Process(_topic("t.a")))]
            )
        ),
        _each("t.a", 0),
        # At least once refuses this shape (FORK_UNMATCHED_UNROUTED); the row pins
        # what that rule prevents: unmatched messages complete without a write.
        delivery="at_most_once",
    ),
    "fork_with_drain_default": _PathCase(
        lambda: Process(
            Fork.when(
                [ForkRoute(when=msg.payload.value == "item-0", process=Process(_topic("t.a")))],
                default=Process(Drain()),
            )
        ),
        _each("t.a", 0),
    ),
    "fork_nested_in_a_fork_branch": _PathCase(
        lambda: Process(
            Fork.when(
                [
                    ForkRoute(
                        when=msg.payload.value != "item-2",
                        process=Process(
                            Fork.when(
                                [
                                    ForkRoute(
                                        when=msg.payload.value == "item-0",
                                        process=Process(_topic("t.a")),
                                    )
                                ],
                                default=Process(_topic("t.b")),
                            )
                        ),
                    )
                ],
                default=Process(_topic("t.c")),
            )
        ),
        _each("t.a", 0) + _each("t.b", 1) + _each("t.c", 2),
    ),
    "expand_step": _PathCase(
        lambda: Process(_TwiceExceptItem1(), _topic("t.a")),
        _each("t.a", 0, 0, 2, 2),
        shares={0: 2, 1: 1, 2: 2},
    ),
    "collect_batch_batch_step_for_each": _PathCase(
        lambda: Process(
            CollectBatch(max_records=3, timeout_ms=50), _UpperBatch(), ForEach(), _topic("t.a")
        ),
        _each("t.a", 0, 1, 2),
    ),
    "collect_batch_into_sync_with": _PathCase(
        lambda: Process(
            CollectBatch(max_records=3, timeout_ms=50),
            With(process=Process(UpperItem(), _topic("t.inline"))),
        ),
        _each("t.inline", 0, 1, 2),
    ),
    "batch_expand_step": _PathCase(
        lambda: Process(
            CollectBatch(max_records=3, timeout_ms=50), _TwoNoneOneInBatch(), _topic("t.a")
        ),
        _each("t.a", 0, 0, 2),
        shares={0: 2, 1: 1, 2: 1},
        errors="t.err",
    ),
    "batch_expand_step_to_nothing": _PathCase(
        lambda: Process(
            CollectBatch(max_records=3, timeout_ms=50), _NothingFromBatch(), _topic("t.a")
        ),
        (),
        errors="t.err",
    ),
    "explode_then_router": _PathCase(
        lambda: Process(
            cast(Any, Explode(_TwoRows)),  # Process does not type Explode
            Router.by(_PayloadType(), routes={_Row: Process(IntoTopic("t.a", payload=_Row))}),
        ),
        _each("t.a", 0, 0, 1, 1, 2, 2),
        shares=2,
    ),
    "multi_type_source": _PathCase(
        lambda: Process(
            Router.by(
                _PayloadType(),
                routes={
                    Item: Process(_topic("t.a")),
                    _Other: Process(IntoTopic("t.b", payload=_Other)),
                },
            )
        ),
        _each("t.a", 0, 2) + _each("t.b", 1),
        source=lambda: FromMultiTypeTopic("items", payloads=(Item, _Other)),
        items=lambda: [
            _encoded(Item(value="item-0"), 0),
            _encoded(_Other(value="item-1"), 1),
            _encoded(Item(value="item-2"), 2),
        ],
    ),
    "with_async_inline_step_fails": _PathCase(
        lambda: Process(WithAsync(process=Process(_FailOnItem1(), _topic("t.inline")))),
        _each("t.inline", 0, 2) + _each("t.err", 1),
        errors="t.err",
    ),
    "fork_branch_into_table": _PathCase(
        lambda: Process(
            Fork.when(
                [ForkRoute(when=msg.payload.value == "item-0", process=Process(_table()))],
                default=Process(_topic("t.a")),
            )
        ),
        _each("items_table", 0) + _each("t.a", 1, 2),
    ),
    "router_nested_in_a_fork_branch": _PathCase(
        lambda: Process(
            Fork.when(
                [
                    ForkRoute(
                        when=msg.payload.value != "item-2",
                        process=Process(
                            Router.by(msg.payload.value, routes={"item-0": Process(_topic("t.a"))})
                        ),
                    )
                ],
                default=Process(_topic("t.b")),
            )
        ),
        _each("t.a", 0) + _each("t.b", 2),
    ),
    "broadcast_discard_route_and_drain": _PathCase(
        lambda: Process(
            Broadcast(
                BroadcastRoute(process=Process(UpperItem())),
                BroadcastRoute(process=Process(Drain())),
                BroadcastRoute(process=Process(_topic("t.a"))),
            )
        ),
        _each("t.a", 0, 1, 2),
        shares=3,
    ),
    "expand_routes_default_route_and_zero_rows": _PathCase(
        lambda: Process(
            ExpandRoutes(
                expander=_RowsByOffset,
                routes={_Row: Process(IntoTopic("t.a", payload=_Row))},
                default=Process(IntoTopic("t.b", payload=_Other)),
            )
        ),
        _each("t.a", 0) + _each("t.b", 0, 2),
        shares={0: 2, 1: 1, 2: 1},
    ),
    "expand_routes_route_into_table": _PathCase(
        lambda: Process(ExpandRoutes(expander=_TwoRows, routes={_Row: Process(_table(_Row))})),
        _each("items_table", 0, 0, 1, 1, 2, 2),
        shares=2,
    ),
    "router_when_into_a_terminal": _PathCase(
        lambda: Process(
            Router.when(
                [Route(when=msg.payload.value == "item-0", process=Process(_topic("t.a")))],
                default=Process(_topic("t.b")),
            )
        ),
        _each("t.a", 0) + _each("t.b", 1, 2),
    ),
    "router_branch_step_fails": _PathCase(
        lambda: Process(
            Router.by(
                msg.payload.value,
                routes={"item-1": Process(_FailOnItem1(), _topic("t.b"))},
                default=Process(_topic("t.a")),
            )
        ),
        _each("t.a", 0, 2) + _each("t.err", 1),
        errors="t.err",
    ),
    "router_without_default_unmatched_to_output": _PathCase(
        lambda: Process(Router.by(msg.payload.value, routes={"item-0": Process(_topic("t.a"))})),
        _each("t.a", 0) + _each("t.out", 1, 2),
        output="t.out",
    ),
    "router_unmatched": _PathCase(
        lambda: Process(Router.by(msg.payload.value, routes={"item-0": Process(_topic("t.a"))})),
        _each("t.a", 0),
    ),
    "router_drain_branch": _PathCase(
        lambda: Process(Router.by(msg.payload.value, routes={"item-0": Process(Drain())})),
        (),
    ),
    "router_into_table_branch": _PathCase(
        lambda: Process(
            Router.by(
                msg.payload.value,
                routes={"item-0": Process(UpperItem(), _table())},
                default=Process(_table()),
            )
        ),
        _each("items_table", 0, 1, 2),
    ),
    "into_table": _PathCase(
        lambda: Process(UpperItem(), _table()),
        _each("items_table", 0, 1, 2),
    ),
    "decode_error_without_wire_route": _PathCase(
        lambda: Process(_topic("t.a")),
        _each("t.a", 0, 1),
        items=lambda: [_item(0), _item(1), _undecodable(2)],
    ),
}


def _assert_each_offset_completes_once_after_its_writes(
    log: list[Event],
    writes: Sequence[tuple[str, int]],
    shares: int | dict[int, int],
    offsets: range,
) -> None:
    """Assert the writes, the completions per share, and that none precedes a flush."""
    written = Counter(
        (cast(tuple[int, str], where)[1] if kind == "produce" else where, offset)
        for kind, where, offset in log
        if kind in ("produce", "write")
    )
    assert written == Counter(writes), "the messages were written somewhere else"

    completed = Counter(offset for kind, _, offset in log if kind == "complete")
    forked = Counter[int | None]()
    for kind, extra, offset in log:
        if kind == "fork":
            forked[offset] += cast(int, extra)
    expected = shares if isinstance(shares, dict) else dict.fromkeys(offsets, shares)
    assert completed == expected, "completions per offset"
    assert all(completed[offset] == 1 + forked[offset] for offset in offsets), (
        "every fork share completes exactly once"
    )

    for offset in offsets:
        released = max(
            i for i, (kind, _, o) in enumerate(log) if kind == "complete" and o == offset
        )
        for i, (kind, where, o) in enumerate(log):
            if o != offset or kind not in ("produce", "write"):
                continue
            assert i < released, f"offset {offset} was released before a write"
            if kind == "produce":
                producer = cast(tuple[int, str], where)[0]
                assert ("flush", producer, None) in log[i:released], (
                    f"offset {offset} was released before its write was flushed"
                )


_KNOWN_BUGS_WITH_FOLLOW_UP: dict[str, tuple[type[BaseException], str, str]] = {
    "collect_batch_batch_step_for_each": (
        CompilationError,
        "shape mismatch: expected many but got batch before ForEach",
        "follow-up hotfix: ForEach must accept the batch a BatchStep produces",
    ),
}
"""Shapes outside the completion invariant that fail today, pinned to their failure.

Each is ``(exception, message pattern, follow-up)``. When a follow-up fixes the
shape, ``pytest.raises`` fails and the entry must go.
"""


class TestEveryPathCompletesOnce:
    """Each offset completes once per share, and only after its writes are flushed."""

    @pytest.mark.parametrize("name", list(_PATH_CASES))
    def test_each_offset_completes_once_per_share_after_its_last_flush(
        self, name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        known_bug = _KNOWN_BUGS_WITH_FOLLOW_UP.get(name)
        if known_bug is None:
            self._assert_case(_PATH_CASES[name], monkeypatch)
            return
        exception, pattern, follow_up = known_bug
        with pytest.raises(exception, match=pattern):
            self._assert_case(_PATH_CASES[name], monkeypatch)
        pytest.xfail(follow_up)

    @staticmethod
    def _assert_case(case: _PathCase, monkeypatch: pytest.MonkeyPatch) -> None:
        log = _run_paths(
            case.process(),
            case.items(),
            monkeypatch,
            output=_topic(case.output) if case.output is not None else None,
            errors=_topic(case.errors) if case.errors is not None else None,
            source=case.source() if case.source is not None else None,
            delivery=case.delivery,
        )

        _assert_each_offset_completes_once_after_its_writes(log, case.writes, case.shares, range(3))

    def test_a_router_keeps_the_source_order_of_the_messages_that_continue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        even: dict[object, Process[Any, Any]] = {
            f"item-{offset}": Process(UpperItem()) for offset in range(0, 8, 2)
        }
        log = _run_paths(
            Process(Router.by(msg.payload.value, routes=even, default=Process(UpperItem()))),
            [_item(offset) for offset in range(8)],
            monkeypatch,
            output=_topic("t.out"),
        )

        assert [offset for kind, _, offset in log if kind == "produce"] == list(range(8))
        _assert_each_offset_completes_once_after_its_writes(
            log, _each("t.out", *range(8)), 1, range(8)
        )


class _BufferingPartition:
    """Storage partition that buffers until ``flush_at`` items, like the table partitions."""

    def __init__(self, flush_at: int, *, fail: bool = False) -> None:
        self._flush_at = flush_at
        self._buffered = 0
        self._fail = fail

    def write_batch(self, items: Sequence[Any]) -> int:
        self._buffered += len(items)
        if self._fail:
            raise RuntimeError("table down")
        if self._buffered < self._flush_at:
            return 0
        flushed, self._buffered = self._buffered, 0
        return flushed

    def close(self) -> int:
        flushed, self._buffered = self._buffered, 0
        return flushed


class TestStorageCompletesAfterItsFlush:
    """A storage sink completes an offset only once the flush holding it returned."""

    @staticmethod
    def _sink(
        partition: object, *offsets: int
    ) -> tuple[_storage._StorageSinkPartition, KafkaCommitTracker]:
        """Wrap *partition* in the storage sink, tracking *offsets*."""
        tracker, _ = _tracker(*offsets)
        sink = _storage._StorageSinkPartition(
            cast(Any, partition),
            node_name="table",
            flow_name="orders",
            flow_run_id="run-1",
            observer=ObservabilityRuntime.noop(),
            commit_tracker=tracker,
        )
        return sink, tracker

    @staticmethod
    def _orders(*offsets: int) -> list[Message[Any]]:
        return [_sourced_message(Order(order_id=str(offset)), offset) for offset in offsets]

    def test_buffered_rows_complete_only_with_the_flush_that_writes_them(self) -> None:
        """No close() at the end is the crash: offset 3 was never written, so it is replayed."""
        sink, tracker = self._sink(_BufferingPartition(flush_at=3), *range(5))

        sink.write_batch(self._orders(0, 1))
        assert tracker.flush_partition(TOPIC, PARTITION) == [], "buffered rows were completed"

        sink.write_batch(self._orders(2))
        sink.write_batch(self._orders(3))
        assert tracker.flush_partition(TOPIC, PARTITION) == [TopicPartition(TOPIC, PARTITION, 3)]

    def test_the_final_flush_at_close_completes_the_rest(self) -> None:
        sink, tracker = self._sink(_BufferingPartition(flush_at=10), 0, 1)
        sink.write_batch(self._orders(0, 1))

        sink.close()

        assert tracker.flush_partition(TOPIC, PARTITION) == [TopicPartition(TOPIC, PARTITION, 2)]

    def test_after_a_failed_write_a_later_count_completes_nothing(self) -> None:
        """The close count cannot say which pending rows it covers once a write failed."""
        sink, tracker = self._sink(_BufferingPartition(flush_at=10, fail=True), 0, 1)

        with pytest.raises(RuntimeError, match="table down"):
            sink.write_batch(self._orders(0, 1))
        sink.close()

        assert tracker.flush_partition(TOPIC, PARTITION) == []

    def test_after_a_refused_count_a_later_count_completes_nothing(self) -> None:
        class _CountsLate(_BufferingPartition):
            def write_batch(self, items: Sequence[Any]) -> int:
                super().write_batch(items)
                return cast(int, None)

        sink, tracker = self._sink(_CountsLate(flush_at=10), 0, 1)
        with pytest.raises(TypeError, match="must return how many"):
            sink.write_batch(self._orders(0, 1))

        sink.close()

        assert tracker.flush_partition(TOPIC, PARTITION) == []

    def test_events_a_close_leaves_unflushed_are_logged_and_stay_uncompleted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A partition that does not flush at close leaves its buffer unwritten."""

        class _NoFlushOnClose(_BufferingPartition):
            def close(self) -> int:
                return 0

        sink, tracker = self._sink(_NoFlushOnClose(flush_at=10), 0, 1)
        sink.write_batch(self._orders(0, 1))

        sink.close()

        assert tracker.flush_partition(TOPIC, PARTITION) == []
        assert "storage_sink_closed_with_pending_events" in caplog.text

    def test_a_bad_count_from_close_completes_nothing_and_does_not_raise(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """close() runs during Bytewax shutdown, so the bad count is logged, not raised."""

        class _BadClose(_BufferingPartition):
            def close(self) -> int:
                return cast(int, None)

        sink, tracker = self._sink(_BadClose(flush_at=10), 0, 1)
        sink.write_batch(self._orders(0, 1))

        sink.close()

        assert tracker.flush_partition(TOPIC, PARTITION) == []
        assert "storage_sink_close_count_invalid" in caplog.text

    @pytest.mark.parametrize("count", [None, True])
    def test_a_partition_that_does_not_report_durability_is_refused(self, count: object) -> None:
        class _Silent:
            def write_batch(self, items: Sequence[Any]) -> object:
                del items
                return count

            def close(self) -> object:
                return count

        sink, _ = self._sink(_Silent(), 0)

        with pytest.raises(TypeError, match="must return how many of the 1 pending items"):
            sink.write_batch(self._orders(0))
