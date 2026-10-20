"""Bytewax handler family for step-shaped nodes."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from bytewax.operators import flat_map
from bytewax.operators import map as bw_map

from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming.bytewax._error_boundary import (
    ErrorBoundary,
    NodeResult,
    _classify_task,
    _execute_batch_in_boundary,
    _execute_in_boundary,
    _split_batch_node_result,
    _split_node_result,
)
from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
    _ExecutableBatchExpandStep,
    _ExecutableBatchStep,
    _ExecutableExpandStep,
    _ExecutableRecordStep,
    _identity,
    _observe_node,
    _reconcile_fanout,
    _replace_payload,
    _replace_payloads,
    _require_message,
    _resolve_batch_expand_result,
    _resolve_batch_result,
    _resolve_expand_result,
    _resolve_node_name,
    _resolve_record_result,
    _step_id,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.core._message import Message
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._decompose import Explode

Stream = Any


def _apply_record_step(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, _ExecutableRecordStep):
        raise UnsupportedNodeError(f"Unsupported record step {type(raw).__name__}.")
    record_step = raw
    name = _resolve_node_name(record_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    boundary = ErrorBoundary(observer=observer, flow=flow_name)

    def step_fn(msg: Any) -> NodeResult:
        message = _require_message(msg)
        return _execute_in_boundary(
            _classify_task,
            message,
            lambda: _execute_record_step(observer, flow_name, idx, name, record_step, message),
            boundary,
        )

    sid = _step_id(f"record_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    return _split_node_result(mapped, sid, ctx, ErrorKind.TASK)


def _apply_batch_step(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, _ExecutableBatchStep):
        raise UnsupportedNodeError(f"Unsupported batch step {type(raw).__name__}.")
    batch_step = raw
    name = _resolve_node_name(batch_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    boundary = ErrorBoundary(observer=observer, flow=flow_name)

    def step_fn(batch: list[Any]) -> list[NodeResult]:
        messages = [_require_message(item) for item in batch]
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_batch_step(
                observer,
                flow_name,
                idx,
                name,
                batch_step,
                messages,
            ),
            boundary,
        )

    sid = _step_id(f"batch_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    return _split_batch_node_result(mapped, sid, ctx, ErrorKind.TASK)


def _apply_expand_step(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, _ExecutableExpandStep):
        raise UnsupportedNodeError(f"Unsupported expand step {type(raw).__name__}.")
    expand_step = raw
    name = _resolve_node_name(expand_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    boundary = ErrorBoundary(observer=observer, flow=flow_name)

    tracker = ctx.commit_tracker

    def step_fn(msg: Any) -> list[NodeResult]:
        message = _require_message(msg)
        results = _execute_batch_in_boundary(
            _classify_task,
            [message],
            lambda: _execute_expand_step(
                observer,
                flow_name,
                idx,
                name,
                expand_step,
                message,
            ),
            boundary,
        )
        _reconcile_fanout([message], results, tracker)
        return results

    sid = _step_id(f"expand_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    flattened_stream = flat_map(_step_id(f"flatten_expand_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened_stream, sid, ctx, ErrorKind.TASK)


def _apply_batch_expand_step(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, _ExecutableBatchExpandStep):
        raise UnsupportedNodeError(f"Unsupported batch-expand step {type(raw).__name__}.")
    batch_expand_step = raw
    name = _resolve_node_name(batch_expand_step)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    boundary = ErrorBoundary(observer=observer, flow=flow_name)

    tracker = ctx.commit_tracker

    def step_fn(batch: list[Any]) -> list[NodeResult]:
        messages = [_require_message(item) for item in batch]
        results = _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_batch_expand_step(
                observer,
                flow_name,
                idx,
                name,
                batch_expand_step,
                messages,
            ),
            boundary,
        )
        _reconcile_fanout(messages, results, tracker)
        return results

    sid = _step_id(f"batch_expand_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    flattened = flat_map(_step_id(f"flatten_batch_expand_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened, sid, ctx, ErrorKind.TASK)


def _apply_explode(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, Explode):
        raise UnsupportedNodeError(f"Unsupported explode node {type(raw).__name__}.")
    explode_node = raw
    name = _resolve_node_name(explode_node.exploder)
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    boundary = ErrorBoundary(observer=observer, flow=flow_name)

    tracker = ctx.commit_tracker

    def step_fn(msg: Any) -> list[NodeResult]:
        message = _require_message(msg)
        results = _execute_batch_in_boundary(
            _classify_task,
            [message],
            lambda: _execute_explode_step(
                observer,
                flow_name,
                idx,
                name,
                explode_node,
                message,
            ),
            boundary,
        )
        _reconcile_fanout([message], results, tracker)
        return results

    sid = _step_id(f"explode_{idx}_{name}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    flattened = flat_map(_step_id(f"flatten_explode_{idx}", ctx), mapped, _identity)
    return _split_node_result(flattened, sid, ctx, ErrorKind.TASK)


def _execute_record_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    record_step: _ExecutableRecordStep,
    message: Message[StreamPayload],
) -> Message[StreamPayload]:
    with _observe_node(
        observer,
        flow_name,
        idx,
        name,
        trace_id=message.meta.trace_id,
        correlation_id=message.meta.correlation_id,
    ):
        result = _resolve_record_result(record_step.execute(message), name)
        return _replace_payload(message, result)


def _execute_batch_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    batch_step: _ExecutableBatchStep,
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with _observe_node(
        observer,
        flow_name,
        idx,
        name,
        trace_id=messages[0].meta.trace_id if messages else None,
        correlation_id=messages[0].meta.correlation_id if messages else None,
    ):
        result = _resolve_batch_result(batch_step.execute(messages), name)
        if not isinstance(result, list):
            raise TypeError(f"{name} must return a list of payloads.")
        return _replace_payloads(messages, result)


def _execute_expand_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    expand_step: _ExecutableExpandStep,
    message: Message[StreamPayload],
) -> list[Message[StreamPayload]]:
    with _observe_node(
        observer,
        flow_name,
        idx,
        name,
        trace_id=message.meta.trace_id,
        correlation_id=message.meta.correlation_id,
    ):
        result = _resolve_expand_result(expand_step.execute(message), name)
        if not isinstance(result, Iterable):
            raise TypeError(f"{name} must return an iterable of payloads.")
        return [_replace_payload(message, payload) for payload in result]


def _execute_batch_expand_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    batch_expand_step: _ExecutableBatchExpandStep,
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    with _observe_node(
        observer,
        flow_name,
        idx,
        name,
        trace_id=messages[0].meta.trace_id if messages else None,
        correlation_id=messages[0].meta.correlation_id if messages else None,
    ):
        result = _resolve_batch_expand_result(batch_expand_step.execute(messages), name)
        return _outputs_per_message(messages, _result_list(result, name), name)


def _outputs_per_message(
    messages: list[Message[StreamPayload]],
    groups: list[Any],
    name: str,
) -> list[Message[StreamPayload]]:
    """Give each output the metadata of the message whose list it is in.

    ``groups[i]`` holds the outputs of ``messages[i]``; an empty list filters
    that message out.
    """
    if len(groups) != len(messages):
        raise TypeError(
            f"{name} returned {len(groups)} output lists for {len(messages)} messages; "
            "return one list of payloads per message, in batch order."
        )
    outputs: list[Message[StreamPayload]] = []
    for message, payloads in zip(messages, groups, strict=True):
        for payload in _result_list(payloads, name):
            if isinstance(payload, Message):
                raise TypeError(
                    f"{name} must return payloads, not messages: each output "
                    "takes the metadata of the message it came from."
                )
            outputs.append(_replace_payload(message, payload))
    return outputs


def _result_list(value: object, name: str) -> list[Any]:
    """Materialize one list of a batch-expanding result, iterating it once.

    ``str``, ``bytes`` and mappings are iterable but are not lists of results.
    """
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, bytearray, Mapping)):
        raise TypeError(f"{name} must return one list of payloads per message.")
    return list(value)


def _execute_explode_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    name: str,
    explode_node: Explode[Any],
    message: Message[StreamPayload],
) -> list[Message[StreamPayload]]:
    with _observe_node(
        observer,
        flow_name,
        idx,
        name,
        trace_id=message.meta.trace_id,
        correlation_id=message.meta.correlation_id,
    ):
        result = explode_node.exploder.expand(message.payload)
        if not isinstance(result, dict):
            raise TypeError(f"{name} must return a mapping of output types to payload lists.")
        outputs: list[Message[StreamPayload]] = []
        for payloads in result.values():
            if not isinstance(payloads, Iterable):
                raise TypeError(f"{name} must return iterables of payloads.")
            for payload in payloads:
                outputs.append(_replace_payload(message, payload))
        return outputs
