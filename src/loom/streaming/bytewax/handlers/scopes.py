"""Bytewax handler family for scoped nodes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from time import time_ns
from typing import Any

import anyio
from bytewax.operators import branch, flat_map
from bytewax.operators import map as bw_map

from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.streaming.bytewax._batch_spans import BatchSpan, BatchWindow, emit_batch_spans
from loom.streaming.bytewax._error_boundary import (
    ErrorBoundary,
    _build_error_envelope,
    _classify_task,
    _execute_batch_in_boundary,
    _split_batch_node_result,
)
from loom.streaming.bytewax._operators import ResourceLifecycle
from loom.streaming.bytewax.handlers._shared import (
    _batch_dependencies,
    _BuildContextProtocol,
    _ExecutableRecordStep,
    _identity,
    _messages_from_batch,
    _node_log_context,
    _node_meta,
    _node_span_name,
    _replace_payload,
    _replace_payloads,
    _require_message,
    _resolve_async_result,
    _resolve_record_result,
    _step_id,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._exceptions import MissingBridgeError, UnsupportedNodeError
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.core._tracing import message_attributes
from loom.streaming.core._typing import StreamPayload
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._step import RecordStep
from loom.streaming.nodes._with import With, WithAsync

Stream = Any


def _apply_with(stream: Stream, raw: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    if not isinstance(raw, With):
        raise UnsupportedNodeError(f"Unsupported with node {type(raw).__name__}.")
    node = raw
    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    node_type = type(node).__name__
    boundary = ErrorBoundary(observer=observer, flow=flow_name)
    inner_steps, sink_partition = _resolve_inner_process(node, ctx)

    def step(batch: list[Any]) -> list[Any]:
        messages = _messages_from_batch(batch)
        return _execute_batch_in_boundary(
            _classify_task,
            messages,
            lambda: _execute_with_step(
                observer,
                flow_name,
                idx,
                node_type,
                manager,
                worker_resources,
                inner_steps,
                sink_partition,
                messages,
            ),
            boundary,
        )

    mapped = bw_map(_step_id(f"with_{idx}", ctx), stream, step)
    return _split_batch_node_result(
        mapped, _step_id(f"with_{idx}_{node_type}", ctx), ctx, ErrorKind.TASK
    )


def _apply_with_async(
    stream: Stream,
    raw: object,
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    if not isinstance(raw, WithAsync):
        raise UnsupportedNodeError(f"Unsupported with-async node {type(raw).__name__}.")
    node = raw
    if ctx.bridge is None:
        raise MissingBridgeError("WithAsync requires an AsyncBridge but none was created.")
    return _apply_with_async_process(stream, node, idx, ctx)


def _apply_with_async_process(
    stream: Stream,
    node: WithAsync[StreamPayload, StreamPayload],
    idx: int,
    ctx: _BuildContextProtocol,
) -> Stream:
    bridge = ctx.bridge
    if bridge is None:
        raise MissingBridgeError("WithAsync requires an AsyncBridge but none was created.")
    manager = ctx.manager_for(idx, node)
    worker_resources = manager.open_worker()
    observer = ctx.flow_runtime
    flow_name = ctx.plan.name
    node_type = type(node).__name__
    boundary = ErrorBoundary(observer=observer, flow=flow_name)

    inner_steps, sink_partition = _resolve_inner_process(node, ctx)

    def step_fn(msg: Any) -> list[Any]:
        batch = _messages_from_batch(msg) if isinstance(msg, list) else [_require_message(msg)]
        started_ns = time_ns()
        try:
            with (
                _node_log_context(flow_name, idx, node_type),
                _batch_dependencies(manager, worker_resources) as deps,
            ):
                results = bridge.run(
                    _execute_with_async_batch(
                        batch,
                        inner_steps,
                        sink_partition,
                        deps,
                        node.task_timeout_ms,
                        node.max_concurrency,
                        boundary,
                    )
                )
        except Exception as exc:
            _emit_node_batch_spans(
                observer, flow_name, idx, node_type, batch, BatchWindow.since(started_ns), exc
            )
            return [
                _build_error_envelope(ErrorKind.TASK, str(exc), message, boundary)
                for message in batch
            ]
        _emit_node_batch_spans(
            observer, flow_name, idx, node_type, batch, BatchWindow.since(started_ns), None
        )
        return results

    sid = _step_id(f"with_async_process_{idx}", ctx)
    mapped = bw_map(sid, stream, step_fn)
    flat = flat_map(f"{sid}_flat", mapped, _identity)
    split = branch(f"{sid}_split", flat, _is_message_result)
    ctx.wire_node_error(ErrorKind.TASK, sid, split.falses)
    return split.trues


def _resolve_inner_process(
    node: Any,
    ctx: _BuildContextProtocol,
) -> tuple[list[_ExecutableRecordStep], Any]:
    """Extract executable steps and optional sink partition from ``node.process``."""
    inner_steps: list[_ExecutableRecordStep] = []
    sink_partition: Any = None

    for inner_idx, inner_node in enumerate(node.process.nodes):
        if isinstance(inner_node, RecordStep) and isinstance(inner_node, _ExecutableRecordStep):
            inner_steps.append(inner_node)
        elif isinstance(inner_node, IntoTopic):
            inner_path = ctx.current_path + (inner_idx,)
            sink_partition = ctx.inline_sink_partition_for(inner_path)
            break
        else:
            raise UnsupportedNodeError(
                f"{type(node).__name__}(process=...) only supports RecordStep nodes and an "
                f"optional terminal IntoTopic; found {type(inner_node).__name__}."
            )

    return inner_steps, sink_partition


async def _execute_inner_process(
    message: Message[StreamPayload],
    inner_steps: Sequence[_ExecutableRecordStep],
    sink_partition: Any,
    deps: Mapping[str, object],
    timeout_ms: int | None,
) -> Message[StreamPayload]:
    current = message
    for step in inner_steps:
        result = await _resolve_async_result(step.execute(current, **deps), timeout_ms)
        current = _replace_payload(current, result)
    if sink_partition is not None:
        sink_partition.write_batch([current])
    # No offset completes here: an inline partition is built without the commit
    # tracker. The message is returned into the stream and completes where its
    # path ends, after this write.
    return current


async def _execute_with_async_message(
    message: Message[StreamPayload],
    inner_steps: Sequence[_ExecutableRecordStep],
    sink_partition: Any,
    deps: Mapping[str, object],
    timeout_ms: int | None,
    boundary: ErrorBoundary,
) -> Any:
    try:
        current = await _execute_inner_process(
            message,
            inner_steps,
            sink_partition,
            deps,
            timeout_ms,
        )
        return current
    except Exception as exc:
        return _build_error_envelope(_classify_task(exc), str(exc), message, boundary)


async def _execute_with_async_batch(
    messages: Sequence[Message[StreamPayload]],
    inner_steps: Sequence[_ExecutableRecordStep],
    sink_partition: Any,
    deps: Mapping[str, object],
    timeout_ms: int | None,
    max_concurrency: int,
    boundary: ErrorBoundary,
) -> list[Any]:
    if not messages:
        return []

    limiter = anyio.Semaphore(max_concurrency)
    results: list[Any | None] = [None] * len(messages)

    async def _run_one(index: int, message: Message[StreamPayload]) -> None:
        async with limiter:
            results[index] = await _execute_with_async_message(
                message,
                inner_steps,
                sink_partition,
                deps,
                timeout_ms,
                boundary,
            )

    async with anyio.create_task_group() as task_group:
        for index, message in enumerate(messages):
            task_group.start_soon(_run_one, index, message)

    return [result for result in results if result is not None]


def _is_message_result(item: Any) -> bool:
    return isinstance(item, Message)


def _execute_with_step(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    node_type: str,
    manager: ResourceLifecycle,
    worker_resources: Mapping[str, object],
    inner_steps: Sequence[_ExecutableRecordStep],
    sink_partition: Any,
    messages: list[Message[StreamPayload]],
) -> list[Message[StreamPayload]]:
    started_ns = time_ns()
    try:
        with (
            _node_log_context(flow_name, idx, node_type),
            _batch_dependencies(manager, worker_resources) as deps,
        ):
            current_messages = _run_inner_steps(
                messages, inner_steps, sink_partition, deps, node_type
            )
    except Exception as exc:
        _emit_node_batch_spans(
            observer, flow_name, idx, node_type, messages, BatchWindow.since(started_ns), exc
        )
        raise
    _emit_node_batch_spans(
        observer, flow_name, idx, node_type, messages, BatchWindow.since(started_ns), None
    )
    return current_messages


def _run_inner_steps(
    messages: list[Message[StreamPayload]],
    inner_steps: Sequence[_ExecutableRecordStep],
    sink_partition: Any,
    deps: Mapping[str, object],
    node_type: str,
) -> list[Message[StreamPayload]]:
    """Run one scoped node's inner record steps over a whole batch."""
    current_messages = messages
    for step in inner_steps:
        result = [
            _resolve_record_result(step.execute(message, **deps), node_type)
            for message in current_messages
        ]
        current_messages = _replace_payloads(current_messages, result)
    if sink_partition is not None:
        sink_partition.write_batch(current_messages)
    return current_messages


def _emit_node_batch_spans(
    observer: ObservabilityRuntime,
    flow_name: str,
    idx: int,
    node_type: str,
    messages: Sequence[Message[StreamPayload]],
    window: BatchWindow,
    error: BaseException | None,
) -> None:
    """Record a batch-granularity node as N message spans plus one batch span.

    A batch node has N parents, which a trace tree cannot express. Each message
    gets its own ``NODE`` span in its own trace, carrying ``loom.batch_id``, and
    the batch itself gets one span linking back to all of them. Labelling the
    whole batch with ``messages[0]``'s trace instead would tell one message a
    story about N-1 others and leave those N-1 with no node span at all.
    """
    name = _node_span_name(flow_name, idx)
    meta_fields = _node_meta(flow_name, idx, node_type)

    def _open_participation(
        meta: MessageMeta,
        batch_attributes: Mapping[str, object],
        started_ns: int,
    ) -> LoomSpan:
        return observer.open_span(
            Scope.NODE,
            name,
            trace_id=meta.trace_id,
            correlation_id=meta.correlation_id,
            root=True,
            start_time_ns=started_ns,
            attributes={**meta_fields, **message_attributes(meta), **batch_attributes},
        )

    emit_batch_spans(
        observer,
        [message.meta for message in messages],
        batch=BatchSpan(scope=Scope.NODE, name=name, attributes=meta_fields),
        open_participation=_open_participation,
        window=window,
        error=error,
    )
