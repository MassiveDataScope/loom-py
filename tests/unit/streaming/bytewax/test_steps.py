"""Direct coverage for Bytewax step handler wiring."""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.observability.event import LifecycleEvent  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming.bytewax.handlers import steps as _steps  # noqa: E402
from loom.streaming.core._exceptions import UnsupportedNodeError  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.nodes._step import (  # noqa: E402
    BatchExpandStep,
    BatchStep,
    ExpandStep,
    RecordStep,
)
from tests.unit.streaming.bytewax.cases import Order, Result, build_message  # noqa: E402

pytestmark = pytest.mark.bytewax


class _RecordStep(RecordStep[Order, Result]):
    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id.upper())


class _BatchStep(BatchStep[Order, Result]):
    def execute(self, messages: list[Message[Order]], **kwargs: object) -> list[Result]:
        del kwargs
        return [Result(value=message.payload.order_id.upper()) for message in messages]


class _ExpandStep(ExpandStep[Order, Result]):
    def execute(self, message: Message[Order], **kwargs: object) -> list[Result]:
        del kwargs
        return [
            Result(value=message.payload.order_id.upper()),
            Result(value=message.payload.order_id.lower()),
        ]


class _BatchExpandStep(BatchExpandStep[Order, Result]):
    def execute(self, messages: list[Message[Order]], **kwargs: object) -> list[list[Result]]:
        del kwargs
        return [[Result(value=message.payload.order_id.upper())] for message in messages]


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(
        plan=SimpleNamespace(name="orders"),
        current_path=(1, 2),
        flow_runtime=ObservabilityRuntime.noop(),
        commit_tracker=None,
    )


def test_apply_record_step_executes_and_splits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, Any] = {}

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        recorded["step_id"] = step_id
        recorded["stream"] = stream
        result = fn(build_message(Order(order_id="ab")))
        recorded["result"] = result
        return result

    monkeypatch.setattr(_steps, "bw_map", _bw_map)
    monkeypatch.setattr(_steps, "_split_node_result", lambda stream, *_: stream)

    result = _steps._apply_record_step("input-stream", _RecordStep(), 3, _ctx())

    assert recorded["step_id"] == "1_2_record_3__RecordStep"
    assert recorded["stream"] == "input-stream"
    assert isinstance(recorded["result"], Message)
    assert recorded["result"].payload.value == "AB"
    assert result.payload.value == "AB"


def test_apply_record_step_emits_trace_id_from_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[LifecycleEvent] = []

    class _RecordingObserver:
        def on_event(self, event: LifecycleEvent) -> None:
            events.append(event)

    runtime = ObservabilityRuntime([_RecordingObserver()])
    ctx = SimpleNamespace(
        plan=SimpleNamespace(name="orders"),
        current_path=(1, 2),
        flow_runtime=runtime,
        commit_tracker=None,
    )

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        del step_id, stream
        return fn(
            Message(
                payload=Order(order_id="ab"),
                meta=MessageMeta(
                    message_id="test-1",
                    trace_id="trace-step",
                    correlation_id="corr-step",
                ),
            )
        )

    monkeypatch.setattr(_steps, "bw_map", _bw_map)
    monkeypatch.setattr(_steps, "_split_node_result", lambda stream, *_: stream)

    result = _steps._apply_record_step("input-stream", _RecordStep(), 3, ctx)

    assert result.payload.value == "AB"
    assert len(events) >= 2, f"Expected >= 2 lifecycle events, got {len(events)}"
    assert events[0].trace_id == "trace-step"
    assert events[0].correlation_id == "corr-step"
    assert events[1].trace_id == "trace-step"
    assert events[1].correlation_id == "corr-step"


def test_apply_batch_step_executes_and_splits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, Any] = {}

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        recorded["step_id"] = step_id
        recorded["stream"] = stream
        messages = [build_message(Order(order_id="ab")), build_message(Order(order_id="cd"))]
        result = fn(messages)
        recorded["result"] = result
        return result

    monkeypatch.setattr(_steps, "bw_map", _bw_map)
    monkeypatch.setattr(_steps, "_split_batch_node_result", lambda stream, *_: stream)

    result = _steps._apply_batch_step("input-stream", _BatchStep(), 4, _ctx())

    assert recorded["step_id"] == "1_2_batch_4__BatchStep"
    assert recorded["stream"] == "input-stream"
    assert [item.payload.value for item in recorded["result"]] == ["AB", "CD"]
    assert [item.payload.value for item in result] == ["AB", "CD"]


def test_apply_expand_step_executes_and_flattens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, Any] = {}

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        recorded["step_id"] = step_id
        recorded["stream"] = stream
        result = fn(build_message(Order(order_id="Ab")))
        recorded["mapped"] = result
        return result

    def _flat_map(step_id: str, stream: object, fn: Any) -> object:
        recorded["flat_step_id"] = step_id
        recorded["flat_stream"] = stream
        return fn(stream)

    monkeypatch.setattr(_steps, "bw_map", _bw_map)
    monkeypatch.setattr(_steps, "flat_map", _flat_map)
    monkeypatch.setattr(_steps, "_split_node_result", lambda stream, *_: stream)

    result = _steps._apply_expand_step("input-stream", _ExpandStep(), 5, _ctx())

    assert recorded["step_id"] == "1_2_expand_5__ExpandStep"
    assert recorded["flat_step_id"] == "1_2_flatten_expand_5"
    assert recorded["flat_stream"] == recorded["mapped"]
    assert [item.payload.value for item in result] == ["AB", "ab"]


def test_apply_batch_expand_step_executes_and_flattens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, Any] = {}

    def _bw_map(step_id: str, stream: object, fn: Any) -> object:
        recorded["step_id"] = step_id
        recorded["stream"] = stream
        messages = [build_message(Order(order_id="ab")), build_message(Order(order_id="cd"))]
        result = fn(messages)
        recorded["mapped"] = result
        return result

    def _flat_map(step_id: str, stream: object, fn: Any) -> object:
        recorded["flat_step_id"] = step_id
        recorded["flat_stream"] = stream
        return fn(stream)

    monkeypatch.setattr(_steps, "bw_map", _bw_map)
    monkeypatch.setattr(_steps, "flat_map", _flat_map)
    monkeypatch.setattr(_steps, "_split_node_result", lambda stream, *_: stream)

    result = _steps._apply_batch_expand_step("input-stream", _BatchExpandStep(), 6, _ctx())

    assert recorded["step_id"] == "1_2_batch_expand_6__BatchExpandStep"
    assert recorded["flat_step_id"] == "1_2_flatten_batch_expand_6"
    assert recorded["flat_stream"] == recorded["mapped"]
    assert [item.payload.value for item in result] == ["AB", "CD"]


@pytest.mark.parametrize(
    ("handler", "raw", "message"),
    [
        (_steps._apply_record_step, object(), "Unsupported record step object."),
        (_steps._apply_batch_step, object(), "Unsupported batch step object."),
        (_steps._apply_expand_step, object(), "Unsupported expand step object."),
        (_steps._apply_batch_expand_step, object(), "Unsupported batch-expand step object."),
    ],
)
def test_apply_step_rejects_unknown_shape(
    handler: Callable[[object, object, int, Any], object],
    raw: object,
    message: str,
) -> None:
    ctx = _ctx()
    with pytest.raises(UnsupportedNodeError, match=message):
        handler("input-stream", raw, 1, ctx)
