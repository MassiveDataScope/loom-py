"""Tests for Bytewax node error boundary helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import NoReturn, cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.errors import BoundaryValidationError, RuleViolation  # noqa: E402
from loom.core.model import LoomStruct  # noqa: E402
from loom.core.observability.runtime import ObservabilityRuntime  # noqa: E402
from loom.streaming.bytewax import _error_boundary  # noqa: E402
from loom.streaming.core._errors import ErrorEnvelope, ErrorKind  # noqa: E402
from loom.streaming.core._message import Message, MessageMeta  # noqa: E402

pytestmark = pytest.mark.bytewax


class _Payload(LoomStruct):
    value: str


class _Context:
    def __init__(self) -> None:
        self.calls: list[tuple[ErrorKind, str, object]] = []

    def wire_node_error(self, kind: ErrorKind, step_id: str, stream: object) -> None:
        self.calls.append((kind, step_id, stream))


class _LoggerProbe:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def warning(self, event: str, **fields: object) -> None:
        self.calls.append(("warning", event))

    def exception(self, event: str, **fields: object) -> None:
        self.calls.append(("exception", event))


def _message(value: str = "v") -> Message[_Payload]:
    return Message(payload=_Payload(value=value), meta=MessageMeta(message_id="m-1"))


def _raise(exc: Exception) -> NoReturn:
    raise exc


def _boundary() -> _error_boundary.ErrorBoundary:
    return _error_boundary.ErrorBoundary(observer=ObservabilityRuntime.noop(), flow="orders")


def test_classify_task_maps_boundary_validation_error_to_business() -> None:
    """A ``BoundaryValidationError`` is a ``DomainError``: bad input is the message's fault,
    logged as a managed boundary error, and repeating the step cannot fix it."""
    assert _error_boundary._classify_task(BoundaryValidationError("bad body")) is ErrorKind.BUSINESS


def test_execute_in_boundary_returns_message_on_success() -> None:
    result = _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        _message(),
        lambda: _message("ok"),
        _boundary(),
    )

    assert isinstance(result, Message)
    payload = cast(_Payload, result.payload)
    assert payload.value == "ok"


def test_execute_in_boundary_maps_domain_error_to_business() -> None:
    result = _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        _message(),
        lambda: _raise(RuleViolation("field", "boom")),
        _boundary(),
    )

    assert isinstance(result, ErrorEnvelope)
    assert result.kind is ErrorKind.BUSINESS
    assert result.reason == "field: boom"
    assert result.payload_type == _Payload.loom_message_type()
    original = result.original_message
    assert original is not None
    payload = cast(_Payload, original.payload)
    assert payload.value == "v"


def test_execute_in_boundary_logs_managed_domain_error(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = _LoggerProbe()
    monkeypatch.setattr(_error_boundary, "logger", logger)

    _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        _message(),
        lambda: _raise(RuleViolation("field", "boom")),
        _boundary(),
    )

    assert ("warning", "managed_boundary_error") in logger.calls


def test_execute_in_boundary_logs_unhandled_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = _LoggerProbe()
    monkeypatch.setattr(_error_boundary, "logger", logger)
    _error_boundary._execute_in_boundary(
        _error_boundary._classify_task,
        _message(),
        lambda: _raise(RuntimeError("boom")),
        _boundary(),
    )

    assert ("exception", "unhandled_boundary_error") in logger.calls


def test_execute_batch_in_boundary_returns_per_message_envelopes_on_failure() -> None:
    originals = [_message("a"), _message("b")]

    result = _error_boundary._execute_batch_in_boundary(
        _error_boundary._classify_routing,
        originals,
        lambda: _raise(RuntimeError("routing-boom")),
        _boundary(),
    )

    envelopes = [item for item in result if isinstance(item, ErrorEnvelope)]

    assert len(result) == 2
    assert len(envelopes) == 2
    assert all(item.original_message is not None for item in envelopes)
    assert all(item.payload_type == _Payload.loom_message_type() for item in envelopes)
    original_payloads = [
        item.original_message.payload for item in envelopes if item.original_message is not None
    ]
    assert original_payloads == [item.payload for item in originals]
    assert all(item.kind is ErrorKind.ROUTING for item in envelopes)


def test_split_node_result_wires_error_branch_and_returns_success_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Context()
    split = SimpleNamespace(trues="ok-stream", falses="error-stream")

    def _branch(step_id: str, stream: object, predicate: object) -> object:
        assert step_id == "node_1_split"
        assert stream == "input-stream"
        assert predicate is _error_boundary._is_message
        return split

    monkeypatch.setattr(_error_boundary, "branch", _branch)

    result = _error_boundary._split_node_result(
        "input-stream",
        "node_1",
        ctx,
        ErrorKind.TASK,
    )

    assert result == "ok-stream"
    assert ctx.calls == [(ErrorKind.TASK, "node_1", "error-stream")]


def test_split_batch_node_result_flattens_error_branch_and_wires_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Context()
    split = SimpleNamespace(trues="ok-stream", falses=[["error-a"], ["error-b"]])

    def _branch(step_id: str, stream: object, predicate: object) -> object:
        assert step_id == "node_2_split"
        assert stream == "batch-stream"
        assert predicate is _error_boundary._is_message_batch
        return split

    def _flat_map(step_id: str, stream: object, fn: object) -> object:
        assert step_id == "node_2_errors"
        assert stream == split.falses
        return "error-stream"

    monkeypatch.setattr(_error_boundary, "branch", _branch)
    monkeypatch.setattr(_error_boundary, "flat_map", _flat_map)

    result = _error_boundary._split_batch_node_result(
        "batch-stream",
        "node_2",
        ctx,
        ErrorKind.ROUTING,
    )

    assert result == "ok-stream"
    assert ctx.calls == [(ErrorKind.ROUTING, "node_2", "error-stream")]
