"""Direct tests for Bytewax node handler helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest
from structlog.contextvars import get_contextvars

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.errors.errors import RuleViolation  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.core.observability.event import EventKind, LifecycleEvent, Scope  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming.bytewax._error_boundary import ErrorBoundary  # noqa: E402
from loom.streaming.bytewax.handlers import _shared as _shared  # noqa: E402
from loom.streaming.bytewax.handlers import dispatcher as _dispatcher  # noqa: E402
from loom.streaming.bytewax.handlers import routing as _routing  # noqa: E402
from loom.streaming.bytewax.handlers import scopes as _scopes  # noqa: E402
from loom.streaming.bytewax.handlers import shapes as _shapes  # noqa: E402
from loom.streaming.bytewax.handlers import steps as _steps  # noqa: E402
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402
from loom.streaming.core._typing import StreamPayload  # noqa: E402
from loom.streaming.graph._flow import Process  # noqa: E402
from loom.streaming.nodes._boundary import IntoTopic  # noqa: E402
from loom.streaming.nodes._broadcast import Broadcast, BroadcastRoute  # noqa: E402
from loom.streaming.nodes._shape import CollectBatch, Drain  # noqa: E402
from loom.streaming.nodes._step import (  # noqa: E402
    BatchExpandStep,
    BatchStep,
    ExpandStep,
    RecordStep,
)
from loom.streaming.nodes._with import WithAsync  # noqa: E402

pytestmark = pytest.mark.bytewax


def _boundary() -> ErrorBoundary:
    """Build the error boundary the production wiring passes to scoped nodes."""
    return ErrorBoundary(observer=ObservabilityRuntime.noop(), flow="orders")


class _Payload(LoomStruct):
    value: str


class _FakeNode:
    pass


def _message(
    value: str = "v",
    *,
    message_id: str = "m-1",
    topic: str = "t",
    partition: int = 0,
    offset: int = 0,
) -> Message[_Payload]:
    return Message(
        payload=_Payload(value=value),
        meta=MessageMeta(
            message_id=message_id,
            topic=topic,
            partition=partition,
            offset=offset,
        ),
    )


class _RecordingObserver:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, int, str | None]] = []

    def emit(self, event: LifecycleEvent) -> None:
        if event.scope is Scope.NODE and event.kind is EventKind.START:
            node_idx = cast(int, event.meta.get("node_idx", 0))
            self.events.append(
                (
                    "start",
                    str(event.meta.get("flow", "")),
                    node_idx,
                    str(event.meta.get("node_type")),
                )
            )
        elif event.scope is Scope.NODE and event.kind is EventKind.END:
            node_idx = cast(int, event.meta.get("node_idx", 0))
            self.events.append(
                (
                    "end",
                    str(event.meta.get("flow", "")),
                    node_idx,
                    f"{event.meta.get('node_type')}:"
                    f"{event.status.value if event.status is not None else None}",
                )
            )
        elif event.scope is Scope.NODE and event.kind is EventKind.ERROR:
            # ``error_type`` is a structured meta field now; the error text is
            # ``str(exc)``, which no longer starts with the exception class.
            error_type = str(event.meta.get("error_type", "Exception"))
            node_idx = cast(int, event.meta.get("node_idx", 0))
            self.events.append(
                (
                    "error",
                    str(event.meta.get("flow", "")),
                    node_idx,
                    f"{event.meta.get('node_type')}:{error_type}",
                )
            )

    def on_event(self, event: LifecycleEvent) -> None:
        self.emit(event)


class _UpperRecordStep(RecordStep[_Payload, _Payload]):
    def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del kwargs
        payload = _message_payload(message)
        return _Payload(value=payload.value.upper())


class _MessageReturningRecordStep(RecordStep[_Payload, _Payload]):
    def execute(self, message: Message[StreamPayload], **kwargs: object) -> Message[_Payload]:
        del kwargs
        payload = _message_payload(message)
        return Message(
            payload=_Payload(value=payload.value.upper()),
            meta=MessageMeta(
                message_id="override",
                topic=message.meta.topic,
                partition=message.meta.partition,
                offset=message.meta.offset,
            ),
        )


class _BoomRecordStep(RecordStep[_Payload, _Payload]):
    def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del message, kwargs
        raise RuleViolation("value", "boom")


class _AsyncUpperRecordStep(RecordStep[_Payload, _Payload]):
    def __init__(self, probe: _ConcurrencyProbe | None = None) -> None:
        self._probe = probe

    async def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del kwargs
        payload = _message_payload(message)
        if self._probe is not None:
            self._probe.enter()
        try:
            await anyio.sleep(0.01)
            return _Payload(value=payload.value.upper())
        finally:
            if self._probe is not None:
                self._probe.exit()


class _ConditionalAsyncRecordStep(RecordStep[_Payload, _Payload]):
    async def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del kwargs
        payload = _message_payload(message)
        await anyio.sleep(0.001)
        if payload.value == "bad":
            raise RuleViolation("value", "boom")
        return _Payload(value=payload.value.upper())


class _UpperBatchStep(BatchStep[_Payload, _Payload]):
    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> list[StreamPayload]:
        del kwargs
        return [
            cast(StreamPayload, _Payload(value=_message_payload(message).value.upper()))
            for message in messages
        ]


class _UpperExpandStep(ExpandStep[_Payload, _Payload]):
    def execute(
        self,
        message: Message[StreamPayload],
        **kwargs: object,
    ) -> list[StreamPayload]:
        del kwargs
        payload = _message_payload(message)
        return [
            cast(StreamPayload, _Payload(value=payload.value.upper())),
            cast(StreamPayload, _Payload(value=payload.value.lower())),
        ]


class _MessageReturningExpandStep(ExpandStep[_Payload, _Payload]):
    def execute(
        self,
        message: Message[StreamPayload],
        **kwargs: object,
    ) -> list[Message[_Payload]]:
        del kwargs
        payload = _message_payload(message)
        return [
            Message(
                payload=_Payload(value=payload.value.upper()),
                meta=MessageMeta(
                    message_id="override",
                    topic=message.meta.topic,
                    partition=message.meta.partition,
                    offset=message.meta.offset,
                ),
            )
        ]


class _UpperBatchExpandStep(BatchExpandStep[_Payload, _Payload]):
    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> list[list[StreamPayload]]:
        del kwargs
        return [
            [cast(StreamPayload, _Payload(value=_message_payload(message).value.upper()))]
            for message in messages
        ]


class _InvalidBatchStep(BatchStep[_Payload, _Payload]):
    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> Any:
        del kwargs
        del messages
        return _Payload(value="bad")


class _InvalidExpandStep(ExpandStep[_Payload, _Payload]):
    def execute(
        self,
        message: Message[StreamPayload],
        **kwargs: object,
    ) -> Any:
        del kwargs
        del message
        return _Payload(value="bad")


class _ResultBatchExpandStep(BatchExpandStep[_Payload, _Payload]):
    def __init__(self, result: object) -> None:
        self.result = result

    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> Any:
        del kwargs, messages
        return self.result


class _InvalidBatchExpandStep(BatchExpandStep[_Payload, _Payload]):
    def execute(
        self,
        messages: list[Message[StreamPayload]],
        **kwargs: object,
    ) -> Any:
        del kwargs
        del messages
        return _Payload(value="bad")


class _ContextAwareRecordStep(RecordStep[_Payload, _Payload]):
    def __init__(self) -> None:
        self.context: dict[str, object] | None = None

    def execute(self, message: Message[StreamPayload], **kwargs: object) -> _Payload:
        del kwargs
        self.context = dict(get_contextvars())
        return _message_payload(message)


class _CommitTracker:
    def __init__(self) -> None:
        self.forks: list[tuple[str, int, int, int]] = []
        self.completes: list[tuple[str, int, int]] = []

    def fork(self, topic: str, partition: int, offset: int, extra_outputs: int) -> None:
        self.forks.append((topic, partition, offset, extra_outputs))

    def complete(self, topic: str, partition: int, offset: int) -> None:
        self.completes.append((topic, partition, offset))


class _ConcurrencyProbe:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0

    def enter(self) -> None:
        self.active += 1
        if self.active > self.max_active:
            self.max_active = self.active

    def exit(self) -> None:
        self.active -= 1


class _WithAsyncLifecycle:
    def open_worker(self) -> dict[str, object]:
        return {}

    def open_batch(self) -> dict[str, object]:
        return {}

    def close_batch(self) -> None:
        return None

    def shutdown(self) -> None:
        return None


class _WithAsyncBridge:
    def run(self, coro: object) -> object:
        return asyncio.run(cast(Any, coro))


class _RecordingSinkPartition:
    def __init__(self) -> None:
        self.writes: list[list[Message[StreamPayload]]] = []

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        self.writes.append(items)


class _FailingSinkPartition:
    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        del items
        raise RuntimeError("sink-boom")


class _ConditionalFailingSinkPartition:
    def __init__(self) -> None:
        self.writes: list[list[Message[StreamPayload]]] = []

    def write_batch(self, items: list[Message[StreamPayload]]) -> None:
        values = [_message_payload(item).value for item in items]
        if "BAD" in values:
            raise RuntimeError("sink-boom")
        self.writes.append(items)


def test_require_message_rejects_non_message() -> None:
    with pytest.raises(TypeError, match="Expected Message"):
        _shared._require_message(object())


def test_replace_payloads_rejects_mismatched_lengths() -> None:
    messages: list[Message[StreamPayload]] = [_message("a")]
    with pytest.raises(RuntimeError, match="must match input length"):
        _shared._replace_payloads(messages, [])


def test_wire_node_dispatches_registered_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[object, object, int, object]] = []
    node = _FakeNode()
    ctx: Any = SimpleNamespace()

    def _handler(stream: object, node: object, idx: int, ctx: object) -> object:
        seen.append((stream, node, idx, ctx))
        return "wired"

    monkeypatch.setattr(_dispatcher, "_NODE_HANDLERS", {_FakeNode: _handler})

    result = _dispatcher._wire_node("input-stream", node, 7, ctx)

    assert result == "wired"
    assert seen == [("input-stream", node, 7, ctx)]


def test_apply_collect_batch_default_wires_timeout_and_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}
    node = CollectBatch(max_records=3, timeout_ms=250)

    def _key_on(step_id: str, stream: object, fn: object) -> object:
        calls["key_on"] = (step_id, stream, fn)
        return "keyed-stream"

    def _collect(
        step_id: str,
        keyed: object,
        *,
        timeout: timedelta,
        max_size: int,
    ) -> object:
        calls["collect"] = (step_id, keyed, timeout, max_size)
        return "collected-stream"

    def _key_rm(step_id: str, collected: object) -> object:
        calls["key_rm"] = (step_id, collected)
        return "result-stream"

    monkeypatch.setattr(_shapes, "key_on", _key_on)
    monkeypatch.setattr(_shapes, "collect", _collect)
    monkeypatch.setattr(_shapes, "key_rm", _key_rm)

    result = _shapes._apply_collect_batch_default(
        "input-stream",
        node,
        "step-1",
    )

    assert result == "result-stream"
    assert calls["key_on"] == ("collect_key_step-1", "input-stream", _shapes._batch_key)
    assert calls["collect"] == (
        "collect_step-1",
        "keyed-stream",
        timedelta(milliseconds=250),
        3,
    )
    assert calls["key_rm"] == ("collect_unkey_step-1", "collected-stream")


def test_execute_record_step_replaces_payload_and_observes() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _steps._execute_record_step(
        runtime,
        "orders",
        0,
        "Upper",
        _UpperRecordStep(),
        _message("abc"),
    )

    payload = _message_payload(result)
    assert payload.value == "ABC"
    assert observer.events == [
        ("start", "orders", 0, "Upper"),
        ("end", "orders", 0, "Upper:success"),
    ]


def test_execute_record_step_accepts_replacement_message() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _steps._execute_record_step(
        runtime,
        "orders",
        0,
        "Upper",
        _MessageReturningRecordStep(),
        _message("abc"),
    )

    assert isinstance(result, Message)
    assert _message_payload(result).value == "ABC"
    assert result.meta.message_id == "override"


def test_execute_record_step_binds_flow_context() -> None:
    step = _ContextAwareRecordStep()

    _steps._execute_record_step(
        ObservabilityRuntime.noop(),
        "orders",
        3,
        "ContextAware",
        step,
        _message("abc"),
    )

    assert step.context == {
        "flow_name": "orders",
        "node_idx": 3,
        "node_type": "ContextAware",
        "method": "execute",
    }


def test_execute_record_step_propagates_business_error_and_observes() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    step = _BoomRecordStep()
    message = _message("abc")
    with pytest.raises(RuleViolation, match="value: boom"):
        _steps._execute_record_step(
            runtime,
            "orders",
            1,
            "Boom",
            step,
            message,
        )

    assert observer.events == [
        ("start", "orders", 1, "Boom"),
        ("error", "orders", 1, "Boom:RuleViolation"),
    ]


def test_execute_batch_step_replaces_payloads_and_observes() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _steps._execute_batch_step(
        runtime,
        "orders",
        2,
        "UpperBatch",
        _UpperBatchStep(),
        [_message("a"), _message("b")],
    )

    assert [_message_payload(item).value for item in result] == ["A", "B"]
    assert observer.events == [
        ("start", "orders", 2, "UpperBatch"),
        ("end", "orders", 2, "UpperBatch:success"),
    ]


def test_execute_batch_step_rejects_non_list_result() -> None:
    noop = ObservabilityRuntime.noop()
    step = _InvalidBatchStep()
    messages: list[Message[StreamPayload]] = [_message("a"), _message("b")]
    with pytest.raises(TypeError, match="UpperBatch must return a list of payloads."):
        _steps._execute_batch_step(
            noop,
            "orders",
            2,
            "UpperBatch",
            step,
            messages,
        )


def test_execute_expand_step_replaces_payloads_and_observes() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _steps._execute_expand_step(
        runtime,
        "orders",
        3,
        "UpperExpand",
        _UpperExpandStep(),
        _message("ab"),
    )

    assert [_message_payload(item).value for item in result] == ["AB", "ab"]
    assert observer.events == [
        ("start", "orders", 3, "UpperExpand"),
        ("end", "orders", 3, "UpperExpand:success"),
    ]


def test_execute_expand_step_rejects_non_iterable_result() -> None:
    noop = ObservabilityRuntime.noop()
    step = _InvalidExpandStep()
    message = _message("ab")
    with pytest.raises(TypeError, match="UpperExpand must return an iterable of payloads."):
        _steps._execute_expand_step(
            noop,
            "orders",
            3,
            "UpperExpand",
            step,
            message,
        )


def test_execute_expand_step_accepts_replacement_messages() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _steps._execute_expand_step(
        runtime,
        "orders",
        3,
        "UpperExpand",
        _MessageReturningExpandStep(),
        _message("ab"),
    )

    assert len(result) == 1
    assert isinstance(result[0], Message)
    assert _message_payload(result[0]).value == "AB"
    assert result[0].meta.message_id == "override"


def test_execute_batch_expand_step_replaces_payloads_and_observes() -> None:
    observer = _RecordingObserver()
    runtime = ObservabilityRuntime([observer])
    result = _steps._execute_batch_expand_step(
        runtime,
        "orders",
        4,
        "UpperBatchExpand",
        _UpperBatchExpandStep(),
        [_message("a"), _message("b")],
    )

    assert [_message_payload(item).value for item in result] == ["A", "B"]
    assert observer.events == [
        ("start", "orders", 4, "UpperBatchExpand"),
        ("end", "orders", 4, "UpperBatchExpand:success"),
    ]


def test_execute_batch_expand_step_rejects_non_iterable_result() -> None:
    noop = ObservabilityRuntime.noop()
    step = _InvalidBatchExpandStep()
    messages: list[Message[StreamPayload]] = [_message("a"), _message("b")]
    with pytest.raises(
        TypeError,
        match="UpperBatchExpand must return one list of payloads per message.",
    ):
        _steps._execute_batch_expand_step(
            noop,
            "orders",
            4,
            "UpperBatchExpand",
            step,
            messages,
        )


def test_execute_batch_expand_step_gives_each_output_the_metadata_of_its_message() -> None:
    messages: list[Message[StreamPayload]] = [
        _message("a", offset=0),
        _message("b", offset=1),
        _message("c", offset=2),
    ]
    step = _ResultBatchExpandStep(
        [[_Payload(value="a1"), _Payload(value="a2")], [], [_Payload(value="c1")]]
    )

    result = _steps._execute_batch_expand_step(
        ObservabilityRuntime.noop(), "orders", 4, "UpperBatchExpand", step, messages
    )

    assert [(_message_payload(item).value, item.meta.offset) for item in result] == [
        ("a1", 0),
        ("a2", 0),
        ("c1", 2),
    ]


def test_execute_batch_expand_step_expands_an_empty_batch_into_nothing() -> None:
    result = _steps._execute_batch_expand_step(
        ObservabilityRuntime.noop(), "orders", 4, "UpperBatchExpand", _ResultBatchExpandStep([]), []
    )

    assert result == []


@pytest.mark.parametrize(
    ("result", "error"),
    [
        (
            [[_Payload(value="a1")], [_Payload(value="b1")]],
            "UpperBatchExpand returned 2 output lists for 3 messages",
        ),
        (
            [_Payload(value="a1"), _Payload(value="b1"), _Payload(value="c1")],
            "UpperBatchExpand must return one list of payloads per message",
        ),
        (
            [[_message("a1")], [], []],
            "UpperBatchExpand must return payloads, not messages",
        ),
    ],
    ids=["outer_length", "flat_payloads", "message_output"],
)
def test_execute_batch_expand_step_rejects_a_result_not_aligned_with_the_batch(
    result: object, error: str
) -> None:
    messages: list[Message[StreamPayload]] = [_message("a"), _message("b"), _message("c")]
    with pytest.raises(TypeError, match=error):
        _steps._execute_batch_expand_step(
            ObservabilityRuntime.noop(),
            "orders",
            4,
            "UpperBatchExpand",
            _ResultBatchExpandStep(result),
            messages,
        )


def test_execute_with_step_does_not_fork_commit_tracker_for_inline_sink() -> None:
    tracker = _CommitTracker()

    class _Lifecycle:
        def open_worker(self) -> dict[str, object]:
            return {}

        def open_batch(self) -> dict[str, object]:
            return {}

        def close_batch(self) -> None:
            return None

        def shutdown(self) -> None:
            return None

    class _SinkPartition:
        def __init__(self) -> None:
            self.writes: list[list[Message[StreamPayload]]] = []

        def write_batch(self, items: list[Message[StreamPayload]]) -> None:
            self.writes.append(items)

    sink_partition = _SinkPartition()
    result = _scopes._execute_with_step(
        ObservabilityRuntime.noop(),
        "orders",
        5,
        "With",
        _Lifecycle(),
        {},
        [_UpperRecordStep()],
        sink_partition,
        [_message("abc", message_id="m-1"), _message("def", message_id="m-2")],
    )

    assert [_message_payload(item).value for item in result] == ["ABC", "DEF"]
    assert tracker.forks == []
    assert len(sink_partition.writes) == 1


def test_execute_inner_process_writes_to_the_inline_sink() -> None:
    """The record is written inline and keeps flowing to its real terminal.

    Completion is that terminal's job. This used to complete here, releasing
    the offset while the message was still in flight — a data-loss window on
    any intermediate WithAsync. The guarantee is now structural rather than
    asserted: the function has no tracker parameter to complete through.
    """
    sink_partition = _RecordingSinkPartition()

    asyncio.run(
        _scopes._execute_inner_process(
            _message("abc", message_id="m-1"),
            [_UpperRecordStep()],
            sink_partition,
            {},
            None,
        )
    )

    assert [_message_payload(item).value for item in sink_partition.writes[0]] == ["ABC"]


def test_execute_inner_process_runs_without_a_sink_partition() -> None:
    """An inner process with no inline sink still applies its steps."""
    result = asyncio.run(
        _scopes._execute_inner_process(
            _message("abc", message_id="m-1"),
            [_UpperRecordStep()],
            None,
            {},
            None,
        )
    )

    assert _message_payload(result).value == "ABC"


def test_execute_inner_process_propagates_a_sink_failure() -> None:
    """An inline sink failure must surface, never be swallowed."""
    process = _scopes._execute_inner_process(
        _message("abc", message_id="m-1"),
        [_UpperRecordStep()],
        _FailingSinkPartition(),
        {},
        None,
    )
    with pytest.raises(RuntimeError, match="sink-boom"):
        asyncio.run(process)


class TestWithAsyncBatch:
    @pytest.fixture
    def lifecycle(self) -> _WithAsyncLifecycle:
        return _WithAsyncLifecycle()

    @pytest.fixture
    def bridge(self) -> _WithAsyncBridge:
        return _WithAsyncBridge()

    @pytest.fixture
    def observer(self) -> _RecordingObserver:
        return _RecordingObserver()

    @pytest.fixture
    def tracker(self) -> _CommitTracker:
        return _CommitTracker()

    @pytest.fixture
    def sink_partition(self) -> _RecordingSinkPartition:
        return _RecordingSinkPartition()

    @pytest.fixture
    def probe(self) -> _ConcurrencyProbe:
        return _ConcurrencyProbe()

    @pytest.fixture
    def with_async_node(self) -> WithAsync[StreamPayload, StreamPayload]:
        return cast(
            WithAsync[StreamPayload, StreamPayload],
            WithAsync(
                process=Process(_UpperRecordStep()),
                max_concurrency=7,
                task_timeout_ms=500,
            ),
        )

    @pytest.fixture
    def with_async_ctx(
        self,
        bridge: _WithAsyncBridge,
        observer: _RecordingObserver,
        tracker: _CommitTracker,
        lifecycle: _WithAsyncLifecycle,
    ) -> Any:
        return SimpleNamespace(
            bridge=bridge,
            commit_tracker=tracker,
            flow_runtime=ObservabilityRuntime([observer]),
            plan=SimpleNamespace(name="orders"),
            current_path=(),
            manager_for=lambda idx, node: lifecycle,
            wire_node_error=lambda *args, **kwargs: None,
        )

    @pytest.mark.parametrize(
        ("runtime_input", "expected_ids"),
        [
            (_message("abc", message_id="m-1"), ["m-1"]),
            (
                [_message("abc", message_id="m-1"), _message("def", message_id="m-2")],
                ["m-1", "m-2"],
            ),
        ],
    )
    def test_apply_with_async_process_accepts_record_and_batch_inputs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        with_async_node: WithAsync[StreamPayload, StreamPayload],
        with_async_ctx: Any,
        runtime_input: object,
        expected_ids: list[str],
    ) -> None:
        calls: dict[str, Any] = {}

        def _execute_with_async_batch(
            messages: list[Message[StreamPayload]],
            inner_steps: list[object],
            sink_partition: object,
            deps: dict[str, object],
            timeout_ms: int | None,
            max_concurrency: int,
            boundary: object,
        ) -> Any:
            del inner_steps, sink_partition, deps, timeout_ms, boundary
            calls["batch"] = [message.meta.message_id for message in messages]
            calls["max_concurrency"] = max_concurrency
            return asyncio.sleep(0, result=[])

        def _bw_map(
            step_id: str,
            stream: object,
            fn: Callable[[object], object],
        ) -> object:
            del step_id, stream
            assert callable(fn)
            calls["mapped"] = fn(runtime_input)
            return "mapped-stream"

        def _flat_map(step_id: str, stream: object, fn: object) -> object:
            del step_id, fn
            return stream

        def _branch(step_id: str, stream: object, predicate: object) -> object:
            del step_id, predicate
            return SimpleNamespace(trues=stream, falses=[])

        monkeypatch.setattr(_scopes, "bw_map", _bw_map)
        monkeypatch.setattr(_scopes, "flat_map", _flat_map)
        monkeypatch.setattr(_scopes, "branch", _branch)
        monkeypatch.setattr(_scopes, "_execute_with_async_batch", _execute_with_async_batch)

        result = _scopes._apply_with_async_process(
            "input-stream",
            with_async_node,
            0,
            with_async_ctx,
        )

        assert result == "mapped-stream"
        assert calls["batch"] == expected_ids
        assert calls["max_concurrency"] == 7
        assert calls["mapped"] == []

    @pytest.mark.asyncio
    async def test_execute_with_async_batch_writes_each_message_and_tracks_completion(
        self,
        tracker: _CommitTracker,
        sink_partition: _RecordingSinkPartition,
        probe: _ConcurrencyProbe,
    ) -> None:
        messages = [
            _message("abc", message_id="m-1", offset=0),
            _message("def", message_id="m-2", offset=1),
        ]

        results = await _scopes._execute_with_async_batch(
            messages,
            [_AsyncUpperRecordStep(probe)],
            sink_partition,
            {},
            None,
            2,
            _boundary(),
        )

        successes = [item for item in results if isinstance(item, Message)]
        assert len(successes) == 2
        assert [(_message_payload(item).value, item.meta.offset) for item in successes] == [
            ("ABC", 0),
            ("DEF", 1),
        ]
        assert sorted(
            _message_payload(item).value for batch in sink_partition.writes for item in batch
        ) == ["ABC", "DEF"]
        # completion belongs to the terminal; WithAsync cannot reach a tracker
        assert probe.max_active == 2

    @pytest.mark.asyncio
    async def test_execute_with_async_batch_returns_classified_error_for_failed_message(
        self,
        tracker: _CommitTracker,
        sink_partition: _RecordingSinkPartition,
    ) -> None:
        messages = [
            _message("ok", message_id="m-1", offset=0),
            _message("bad", message_id="m-2", offset=1),
        ]

        results = await _scopes._execute_with_async_batch(
            messages,
            [_ConditionalAsyncRecordStep()],
            sink_partition,
            {},
            None,
            2,
            _boundary(),
        )

        assert len(results) == 2
        assert any(isinstance(item, Message) for item in results)
        error = next(item for item in results if isinstance(item, ErrorEnvelope))
        assert error.kind == ErrorKind.BUSINESS
        assert error.original_message is not None
        assert error.original_message.meta.message_id == "m-2"
        assert error.original_message.meta.key is None
        assert sorted(
            _message_payload(item).value for batch in sink_partition.writes for item in batch
        ) == ["OK"]
        # completion belongs to the terminal; WithAsync cannot reach a tracker

    @pytest.mark.asyncio
    async def test_execute_with_async_batch_returns_classified_error_for_sink_failure(
        self,
        tracker: _CommitTracker,
    ) -> None:
        sink_partition = _ConditionalFailingSinkPartition()
        messages = [
            _message("ok", message_id="m-1", offset=0),
            _message("bad", message_id="m-2", offset=1),
        ]

        results = await _scopes._execute_with_async_batch(
            messages,
            [_AsyncUpperRecordStep()],
            sink_partition,
            {},
            None,
            2,
            _boundary(),
        )

        successes = [item for item in results if isinstance(item, Message)]
        errors = [item for item in results if isinstance(item, ErrorEnvelope)]
        assert len(successes) == 1
        assert len(errors) == 1
        assert errors[0].kind == ErrorKind.TASK
        assert errors[0].original_message is not None
        assert errors[0].original_message.meta.message_id == "m-2"
        assert errors[0].original_message.meta.key is None
        assert sorted(
            _message_payload(item).value for batch in sink_partition.writes for item in batch
        ) == ["OK"]
        # completion belongs to the terminal; WithAsync cannot reach a tracker

    @pytest.mark.asyncio
    async def test_execute_with_async_batch_respects_max_concurrency(
        self,
        sink_partition: _RecordingSinkPartition,
        tracker: _CommitTracker,
        probe: _ConcurrencyProbe,
    ) -> None:
        results = await _scopes._execute_with_async_batch(
            [
                _message("one", message_id="m-1", offset=0),
                _message("two", message_id="m-2", offset=1),
            ],
            [_AsyncUpperRecordStep(probe)],
            sink_partition,
            {},
            None,
            1,
            _boundary(),
        )

        successes = [item for item in results if isinstance(item, Message)]
        assert len(successes) == 2
        assert probe.max_active == 1


@pytest.mark.parametrize(
    ("node", "expected_value"),
    [
        (_UpperRecordStep(), "ABC"),
        (_UpperBatchStep(), "ABC"),
    ],
)
def test_execute_router_node_supports_allowed_branch_nodes(
    node: object,
    expected_value: str,
) -> None:
    result = _routing._execute_router_node(node, _message("abc"))

    assert _message_payload(result).value == expected_value


def test_apply_broadcast_increments_commit_tracker_for_fanout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _CommitTracker()
    branch_calls: list[tuple[str, object, tuple[object, ...], tuple[int, ...]]] = []

    def _bw_map(step_id: str, stream: object, fn: Callable[[object], object]) -> object:
        assert step_id == "1_broadcast_5_fanout"
        assert callable(fn)
        result = fn(_message("abc"))
        assert _message_payload(cast(Message[_Payload], result)).value == "abc"
        return "tracked-stream"

    def _wire_branch(
        step_id: str, stream: object, nodes: tuple[object, ...], path: tuple[int, ...]
    ) -> None:
        branch_calls.append((step_id, stream, nodes, path))

    monkeypatch.setattr(_routing, "bw_map", _bw_map)

    node: Broadcast[Any] = Broadcast(
        BroadcastRoute(
            process=Process(Drain()),
            output=IntoTopic("a", payload=_Payload),
        ),
        BroadcastRoute(
            process=Process(Drain()),
            output=IntoTopic("b", payload=_Payload),
        ),
    )
    ctx: Any = SimpleNamespace(
        current_path=(1,),
        commit_tracker=tracker,
        wire_branch=_wire_branch,
    )

    result = _routing._apply_broadcast("input-stream", node, 5, ctx)

    assert result is None, "a Broadcast ends the path; each route's stream ends on its own"
    assert tracker.forks == [("t", 0, 0, 1)]
    assert [(call[0], call[1], call[3]) for call in branch_calls] == [
        ("broadcast_5_out_0", "tracked-stream", (1, 0)),
        ("broadcast_5_out_1", "tracked-stream", (1, 1)),
    ]
    assert all(len(call[2]) == 1 and isinstance(call[2][0], Drain) for call in branch_calls)


def test_apply_drain_completes_commit_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _CommitTracker()

    def _flat_map(step_id: str, stream: object, fn: Callable[[object], object]) -> object:
        assert step_id == "drain_7"
        assert callable(fn)
        assert fn(_message("abc")) == ()
        return "dropped-stream"

    monkeypatch.setattr(_shapes, "flat_map", _flat_map)

    result = _shapes._apply_drain(
        "input-stream",
        object(),
        7,
        cast(Any, SimpleNamespace(current_path=(), commit_tracker=tracker)),
    )

    assert result is None, "a Drain ends the path; nothing continues after it"
    assert tracker.completes == [("t", 0, 0)]


def _message_payload(message: Message[StreamPayload]) -> _Payload:
    return cast(_Payload, message.payload)
