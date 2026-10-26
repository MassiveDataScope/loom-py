"""Trace continuity under the real multi-worker runtime, not the single-threaded harness.

``bytewax.testing.run_main`` is single-threaded, so a streaming test built on
it passes with plain contextvars and never exercises the path production takes.
These tests drive ``cli_main`` with more than one worker per process, which is
where a context that does not propagate would show up as spans landing on
random trace ids.
"""

from __future__ import annotations

import threading
from typing import ClassVar

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from bytewax.run import cli_main  # noqa: E402
from bytewax.testing import TestingSink, TestingSource  # noqa: E402

from loom.core.config import ConfigContext  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.streaming import (  # noqa: E402
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    StreamFlow,
)
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown  # noqa: E402
from loom.streaming.compiler import compile_flow  # noqa: E402
from tests.helpers.spans import SpanRecorder, build_recorder, hex_trace  # noqa: E402

pytestmark = pytest.mark.bytewax

_WORKERS = 2
_TRACES = tuple(f"{index:032x}".replace("0", "a", 1) for index in range(1, 9))

_CONFIG = {
    "kafka": {
        "consumer": {
            "brokers": ["localhost:9092"],
            "group_id": "g",
            "delivery": "at_most_once",
            "topics": ["items"],
        },
        "producer": {"brokers": ["localhost:9092"], "topic": "items.out"},
    }
}


class _Item(LoomStruct):
    value: str


class _Upper(RecordStep[_Item, _Item]):
    """Step that also reports which OS thread executed it."""

    threads: ClassVar[set[int]] = set()

    def execute(self, message: Message[_Item], **kwargs: object) -> _Item:
        del kwargs
        _Upper.threads.add(threading.get_ident())
        return _Item(value=message.payload.value.upper())


def _flow() -> StreamFlow[_Item, _Item]:
    return StreamFlow(
        name="orders",
        source=FromTopic("items", payload=_Item),
        process=Process(_Upper()),
        output=IntoTopic("items.out", payload=_Item),
    )


def _messages() -> list[Message[_Item]]:
    return [
        Message(
            payload=_Item(value=f"row-{index}"),
            meta=MessageMeta(message_id=f"msg-{index}", trace_id=trace_id),
        )
        for index, trace_id in enumerate(_TRACES)
    ]


def _run(recorder: SpanRecorder, messages: list[Message[_Item]]) -> list[object]:
    plan = compile_flow(_flow(), config=ConfigContext.from_dict(_CONFIG))
    captured: list[object] = []
    built = build_dataflow_with_shutdown(
        plan,
        observability_runtime=recorder.runtime,
        source=TestingSource(list(messages)),
        sink=TestingSink(captured),
    )
    try:
        cli_main(built.dataflow, workers_per_process=_WORKERS)  # type: ignore[no-untyped-call]
    finally:
        built.shutdown()
    return captured


class TestMultiWorkerTraceContinuity:
    def test_every_node_span_lands_in_its_own_messages_trace(self) -> None:
        recorder = build_recorder()
        messages = _messages()

        captured = _run(recorder, messages)

        assert len(captured) == len(messages)
        node_spans = [span for span in recorder.spans() if span.name.startswith("node:")]
        assert len(node_spans) == len(messages), (
            "each message must contribute exactly one node span, whichever worker ran it"
        )
        assert {hex_trace(span) for span in node_spans} == set(_TRACES), (
            "a node span landed on a random trace: the message trace id did not "
            "reach the worker thread that opened the span"
        )
        for span in node_spans:
            assert span.parent is None
            assert span.attributes is not None
            assert span.attributes["flow"] == "orders"

    def test_the_flow_ran_off_the_calling_thread(self) -> None:
        """Guards the guard: a run on the test's own thread proves nothing.

        ``cli_main`` drives Bytewax workers on their own threads, so a trace id
        held in a plain contextvar of the calling thread would not be visible
        where the spans are opened.
        """
        recorder = build_recorder()
        _Upper.threads.clear()

        _run(recorder, _messages())

        assert _Upper.threads, "the step never ran"
        assert threading.get_ident() not in _Upper.threads, (
            "the dataflow ran inline on the test thread, so worker context "
            "propagation was never exercised"
        )
