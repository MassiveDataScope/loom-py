"""Commit accounting across nodes that change the record count.

Every node that turns one input record into N outputs shares one hazard: all N
outputs carry the same source offset, and each one completes that offset when it
reaches a terminal. The tracker expects exactly one completion per record, so a
node that fans out must raise that expectation before any output can be
completed, and a node that produces nothing must release the record itself.

Both failures are silent, which is why they are asserted here against a real
``KafkaCommitTracker`` and its committed offsets rather than against a spy:

- fan-out without a fork commits the offset after the *first* output, losing the
  remaining N-1 on a crash — the consumer group has already moved past them;
- zero outputs without a completion freezes the partition's watermark forever.
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

from bytewax.run import cli_main  # noqa: E402
from bytewax.testing import TestingSource  # noqa: E402

from loom.core.config import ConfigContext  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming import (  # noqa: E402
    Backend,
    Broadcast,
    BroadcastRoute,
    Drain,
    ExpandRoutes,
    Fork,
    ForkRoute,
    FromTopic,
    IntoTable,
    IntoTopic,
    Process,
    RecordStep,
    Router,
    StreamFlow,
    WithAsync,
    msg,
)
from loom.streaming.bytewax import RuntimeConfigurationError, _adapter  # noqa: E402
from loom.streaming.bytewax._commit_tracker import KafkaCommitTracker  # noqa: E402
from loom.streaming.bytewax._runtime_io import build_runtime_sink  # noqa: E402
from loom.streaming.bytewax.handlers import _shared  # noqa: E402
from loom.streaming.bytewax.handlers import routing as _routing  # noqa: E402
from loom.streaming.bytewax.handlers import steps as _steps  # noqa: E402
from loom.streaming.bytewax.handlers import storage as _storage  # noqa: E402
from loom.streaming.compiler import StreamingErrorCode, compile_flow  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.kafka._record import KafkaRecord  # noqa: E402
from tests.unit.streaming.bytewax.cases import Order, Result  # noqa: E402
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
    from loom.core.observability.runtime import ObservabilityRuntime

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

        from loom.streaming.nodes._expand_routes import ExpandRoutes

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
        ctx.wire_process = lambda *args, **kwargs: "route-stream"
        ctx.wire_branch_terminal = lambda *args, **kwargs: None

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


# --- Every path a message can travel ends in exactly one completion -------------
#
# The flows below run on the real Bytewax runtime with the real adapter, the real
# Kafka sinks and the real codec; only the confluent producer and the storage
# partition are faked, and both write into one ordered log next to every fork
# and completion the commit tracker receives.

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


class _Item(LoomStruct):
    value: str


class _Row(LoomStruct):
    value: str


class _Upper(RecordStep[_Item, _Item]):
    """Uppercase the payload."""

    def execute(self, message: Message[_Item], **kwargs: object) -> _Item:
        del kwargs
        return _Item(value=message.payload.value.upper())


class _TwoRows:
    """Expand every item into two rows of one type."""

    outputs: ClassVar[tuple[type, ...]] = (_Row,)

    @classmethod
    def expand(cls, event: _Item) -> dict[type, list[Any]]:
        return {_Row: [_Row(value=event.value), _Row(value=event.value)]}


def _offset_in(value: bytes | str) -> int:
    raw = value.encode() if isinstance(value, str) else value
    found = _ITEM_OFFSET.search(raw)
    assert found is not None, f"no item offset in {raw!r}"
    return int(found.group(1))


class _LoggingProducer(ProducerBackendStub):
    """One sink's confluent producer; its produces and flushes go to the shared log."""

    def __init__(self, log: list[Event]) -> None:
        super().__init__({})
        self._log = log

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
        self._log.append(("produce", (id(self), topic), _offset_in(value)))

    def flush(self, timeout: float | None = None) -> None:
        super().flush(timeout)
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


def _item(offset: int) -> Message[_Item]:
    return Message(
        payload=_Item(value=f"item-{offset}"),
        meta=MessageMeta(message_id=f"m-{offset}", topic="items", partition=0, offset=offset),
    )


def _undecodable(offset: int) -> KafkaRecord[bytes]:
    return KafkaRecord(
        topic="items", key=None, value=b"not-an-envelope", partition=0, offset=offset
    )


def _run_paths(
    process: Process[Any, Any],
    items: Sequence[object],
    monkeypatch: pytest.MonkeyPatch,
    *,
    output: IntoTopic[Any] | None = None,
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
        name="paths", source=FromTopic("items", payload=_Item), process=process, output=output
    )
    plan = compile_flow(flow, config=ConfigContext.from_dict(_PATH_CONFIG))
    built = _adapter.build_dataflow_with_shutdown(
        plan,
        source=_TrackedTestingSource(list(items)),
        sink=build_runtime_sink(plan.output) if plan.output is not None else None,
        commit_tracker=cast(Any, _LoggingTracker(log)),
    )
    try:
        cli_main(built.dataflow, workers_per_process=1)  # type: ignore[no-untyped-call]
    finally:
        built.shutdown()
    return log


def _topic(name: str) -> IntoTopic[Any]:
    return IntoTopic(name, payload=_Item)


def _table() -> Any:
    """Build the table sink; ``Process`` does not type ``IntoSink`` nodes."""
    return IntoTable(payload=_Item, table="items", backend=Backend.DELTA, name="items_table")


@dataclass(frozen=True)
class _PathCase:
    """One flow shape and where each of its three messages must end up.

    Args:
        process: Process of the flow under test.
        writes: Every ``(topic or table, offset)`` the run must write, as a multiset.
        shares: Completions each offset owes: one, plus one per extra fork share.
        output: Flow output, when the shape has one.
        items: Source items; three plain items unless the shape needs others.
    """

    process: Callable[[], Process[Any, Any]]
    writes: tuple[tuple[str, int], ...]
    shares: int = 1
    output: str | None = None
    items: Callable[[], list[object]] = lambda: [_item(offset) for offset in range(3)]


def _each(topic: str, *offsets: int) -> tuple[tuple[str, int], ...]:
    return tuple((topic, offset) for offset in offsets)


_PATH_CASES = {
    "into_topic": _PathCase(
        lambda: Process(_Upper(), _topic("t.a")),
        _each("t.a", 0, 1, 2),
    ),
    "inline_then_output": _PathCase(
        lambda: Process(WithAsync(process=Process(_Upper(), _topic("t.inline")))),
        _each("t.inline", 0, 1, 2) + _each("t.out", 0, 1, 2),
        output="t.out",
    ),
    "inline_only": _PathCase(
        lambda: Process(WithAsync(process=Process(_Upper(), _topic("t.inline")))),
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
                BroadcastRoute(process=Process(_Upper()), output=_topic("t.a")),
                BroadcastRoute(process=Process(_Upper()), output=_topic("t.b")),
            )
        ),
        _each("t.a", 0, 1, 2) + _each("t.b", 0, 1, 2),
        shares=2,
    ),
    "broadcast_routes_without_output": _PathCase(
        lambda: Process(
            Broadcast(
                BroadcastRoute(process=Process(_topic("t.a"))),
                BroadcastRoute(process=Process(_Upper(), _topic("t.b"))),
            )
        ),
        _each("t.a", 0, 1, 2) + _each("t.b", 0, 1, 2),
        shares=2,
    ),
    "router_terminal_branch": _PathCase(
        lambda: Process(
            Router.by(
                msg.payload.value,
                routes={"item-0": Process(_Upper(), _topic("t.a"))},
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
                default=Process(_Upper()),
            )
        ),
        _each("t.a", 0) + _each("t.out", 1, 2),
        output="t.out",
    ),
    "router_non_terminal_branch": _PathCase(
        lambda: Process(
            Router.by(
                msg.payload.value,
                routes={"item-0": Process(_Upper())},
                default=Process(_Upper()),
            )
        ),
        _each("t.out", 0, 1, 2),
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
                routes={"item-0": Process(_Upper(), _table())},
                default=Process(_table()),
            )
        ),
        _each("items_table", 0, 1, 2),
    ),
    "into_table": _PathCase(
        lambda: Process(_Upper(), _table()),
        _each("items_table", 0, 1, 2),
    ),
    "decode_error_without_wire_route": _PathCase(
        lambda: Process(_topic("t.a")),
        _each("t.a", 0, 1),
        items=lambda: [_item(0), _item(1), _undecodable(2)],
    ),
}


class TestEveryPathCompletesOnce:
    """Each offset completes once per share, and only after its writes are flushed."""

    @pytest.mark.parametrize("case", list(_PATH_CASES.values()), ids=list(_PATH_CASES))
    def test_each_offset_completes_once_per_share_after_its_last_flush(
        self, case: _PathCase, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        output = _topic(case.output) if case.output is not None else None
        log = _run_paths(case.process(), case.items(), monkeypatch, output=output)

        written = Counter(
            (cast(tuple[int, str], where)[1] if kind == "produce" else where, offset)
            for kind, where, offset in log
            if kind in ("produce", "write")
        )
        assert written == Counter(case.writes), "the messages were written somewhere else"

        completed = Counter(offset for kind, _, offset in log if kind == "complete")
        forked = Counter[int | None]()
        for kind, extra, offset in log:
            if kind == "fork":
                forked[offset] += cast(int, extra)
        offsets = {offset for _, offset in case.writes} | set(range(3))
        assert completed == dict.fromkeys(offsets, case.shares), "completions per offset"
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

    def test_a_router_keeps_the_source_order_of_one_partition(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        even: dict[object, Process[Any, Any]] = {
            f"item-{offset}": Process(_Upper()) for offset in range(0, 8, 2)
        }
        log = _run_paths(
            Process(Router.by(msg.payload.value, routes=even, default=Process(_Upper()))),
            [_item(offset) for offset in range(8)],
            monkeypatch,
            output=_topic("t.out"),
        )

        assert [offset for kind, _, offset in log if kind == "produce"] == list(range(8))


class _BufferingPartition:
    """Storage partition that buffers until ``flush_at`` items, like the table partitions."""

    def __init__(self, flush_at: int) -> None:
        self._flush_at = flush_at
        self._buffered = 0

    def write_batch(self, items: Sequence[Any]) -> int:
        self._buffered += len(items)
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
    def _sink(flush_at: int) -> tuple[_storage._StorageSinkPartition, KafkaCommitTracker]:
        tracker, _ = _tracker(*range(5))
        sink = _storage._StorageSinkPartition(
            _BufferingPartition(flush_at),
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

    def test_buffered_rows_complete_with_the_flush_that_writes_them(self) -> None:
        sink, tracker = self._sink(flush_at=3)

        sink.write_batch(self._orders(0, 1))
        assert tracker.flush_partition(TOPIC, PARTITION) == [], "buffered rows were completed"

        sink.write_batch(self._orders(2))
        assert tracker.flush_partition(TOPIC, PARTITION) == [TopicPartition(TOPIC, PARTITION, 3)]

    def test_rows_still_buffered_when_the_worker_dies_stay_uncompleted(self) -> None:
        """No close() is the crash: the rows were never written, so they are consumed again."""
        sink, tracker = self._sink(flush_at=3)

        sink.write_batch(self._orders(0, 1, 2))
        sink.write_batch(self._orders(3))

        assert tracker.flush_partition(TOPIC, PARTITION) == [TopicPartition(TOPIC, PARTITION, 3)], (
            "offset 3 is still buffered, so the commit stops before it"
        )

    def test_the_final_flush_at_close_completes_the_rest(self) -> None:
        sink, tracker = self._sink(flush_at=10)
        sink.write_batch(self._orders(0, 1))

        sink.close()

        assert tracker.flush_partition(TOPIC, PARTITION) == [TopicPartition(TOPIC, PARTITION, 2)]

    def test_a_partition_that_does_not_report_durability_is_refused(self) -> None:
        class _Silent:
            def write_batch(self, items: Sequence[Any]) -> None:
                del items

            def close(self) -> None:
                return None

        sink = _storage._StorageSinkPartition(
            cast(Any, _Silent()),
            node_name="table",
            flow_name="orders",
            flow_run_id="run-1",
            observer=ObservabilityRuntime.noop(),
        )

        with pytest.raises(TypeError, match="must return how many of the 1 pending items"):
            sink.write_batch(self._orders(0))
