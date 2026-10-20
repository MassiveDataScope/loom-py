"""The storage sink's batch trace attribution — one death per message.

``_StorageSinkPartition.write_batch`` used to label the whole epoch flush with
``items[0].meta.trace_id``: one arbitrary message donated its trace to N, and
the other N-1 got no write span at all.

It now emits the N+1 shape. Every message gets its own ``terminal:sink_write``
span in its own trace, and the flush gets one ``Scope.WRITE`` span in a trace
of its own. These assertions are the inverse of the ones this file used to pin.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.observability.event import EventKind, LifecycleEvent, Scope  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming.bytewax.handlers import storage as _storage  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from tests.unit.streaming.compiler.cases import Result  # noqa: E402

pytestmark = pytest.mark.bytewax

_TRACE_IDS = (
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
    "ccccccccccccccccccccccccccccccc3",
)


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


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


def _message(index: int, *, trace_id: str | None = "") -> Message[Result]:
    resolved = _TRACE_IDS[index] if trace_id == "" else trace_id
    return Message(
        payload=Result(value=f"row-{index}"),
        meta=MessageMeta(message_id=f"msg-{index}", trace_id=resolved),
    )


def _sink(partition: Any, recorder: _RecordingObserver) -> _storage._StorageSinkPartition:
    return _storage._StorageSinkPartition(
        partition,
        node_name="results_sink",
        flow_name="orders",
        flow_run_id="run-1",
        observer=ObservabilityRuntime([recorder]),
    )


def _of_scope(recorder: _RecordingObserver, scope: Scope) -> list[LifecycleEvent]:
    return [event for event in recorder.events if event.scope is scope]


class TestStorageBatchTraceAttribution:
    def test_every_message_gets_its_own_terminal_span_in_its_own_trace(self) -> None:
        recorder = _RecordingObserver()
        partition = _RecordingPartition()

        _sink(partition, recorder).write_batch([_message(0), _message(1), _message(2)])

        assert partition.batches == [[Result(value=f"row-{i}") for i in range(3)]]

        terminals = _of_scope(recorder, Scope.TERMINAL)
        assert [event.kind for event in terminals] == [
            EventKind.START,
            EventKind.END,
            EventKind.START,
            EventKind.END,
            EventKind.START,
            EventKind.END,
        ]
        # One death per message, each in that message's own trace — the exact
        # inverse of the old behaviour, where two of the three had no span.
        assert [event.trace_id for event in terminals] == [
            _TRACE_IDS[0],
            _TRACE_IDS[0],
            _TRACE_IDS[1],
            _TRACE_IDS[1],
            _TRACE_IDS[2],
            _TRACE_IDS[2],
        ]
        assert {event.name for event in terminals} == {"sink_write"}
        for event in terminals:
            assert event.meta["terminal.reason"] == "sink_write"
            assert event.meta["sink"] == "results_sink"

    def test_the_batch_span_belongs_to_no_message_and_names_the_batch(self) -> None:
        recorder = _RecordingObserver()

        _sink(_RecordingPartition(), recorder).write_batch([_message(0), _message(1), _message(2)])

        writes = _of_scope(recorder, Scope.WRITE)
        assert [event.kind for event in writes] == [EventKind.START, EventKind.END]
        batch_trace = writes[0].trace_id
        assert batch_trace is not None
        assert batch_trace not in _TRACE_IDS, (
            "the batch borrowed a message's trace and told it a story about the others"
        )
        assert writes[0].meta["batch_size"] == 3
        assert writes[0].meta["loom.batch_size"] == 3
        assert writes[0].meta["loom.flow_run_id"] == "run-1"

        # Navigable both ways: every terminal carries the batch id the batch
        # span is identified by.
        batch_ids = {event.meta["loom.batch_id"] for event in _of_scope(recorder, Scope.TERMINAL)}
        assert batch_ids == {batch_trace}

    def test_an_untraced_message_does_not_borrow_anybody_elses_trace(self) -> None:
        recorder = _RecordingObserver()

        _sink(_RecordingPartition(), recorder).write_batch(
            [_message(0, trace_id=None), _message(1)]
        )

        terminals = _of_scope(recorder, Scope.TERMINAL)
        assert [event.trace_id for event in terminals] == [
            None,
            None,
            _TRACE_IDS[1],
            _TRACE_IDS[1],
        ], "an untraced message minted an id and claimed a trace it does not belong to"
        assert [event.meta["loom.message_id"] for event in terminals] == [
            "msg-0",
            "msg-0",
            "msg-1",
            "msg-1",
        ]

    def test_a_failed_write_still_closes_every_message_and_re_raises(self) -> None:
        recorder = _RecordingObserver()

        sink = _sink(_RecordingPartition(fail=True), recorder)
        messages = [_message(0), _message(1)]

        with pytest.raises(RuntimeError, match="sink down"):
            sink.write_batch(messages)

        terminals = _of_scope(recorder, Scope.TERMINAL)
        assert [event.kind for event in terminals] == [
            EventKind.START,
            EventKind.ERROR,
            EventKind.START,
            EventKind.ERROR,
        ]
        assert [event.trace_id for event in terminals] == [
            _TRACE_IDS[0],
            _TRACE_IDS[0],
            _TRACE_IDS[1],
            _TRACE_IDS[1],
        ]
        assert [event.kind for event in _of_scope(recorder, Scope.WRITE)] == [
            EventKind.START,
            EventKind.ERROR,
        ]

    def test_an_empty_epoch_stays_silent(self) -> None:
        recorder = _RecordingObserver()
        partition = _RecordingPartition()

        _sink(partition, recorder).write_batch([])

        assert partition.batches == [[]]
        assert recorder.events == []
