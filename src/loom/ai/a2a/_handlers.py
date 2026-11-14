"""The A2A methods themselves: ``message/send``, ``message/stream``, the rest.

One handler per method, over :class:`~loom.ai.runtime.AgentRuntime`, reusing
the pure event projection of :mod:`loom.ai.a2a.events`. The rules every agent
transport shares — the span that survives a disconnect and the heartbeat race —
come from :mod:`loom.ai._transport` rather than from the HTTP surface's private
names.

``fasta2a``'s ``TypedDict``s alias to camelCase only when routed through
pydantic, so ``Task`` and the streaming events are built here in their already
serialised form — the choice :mod:`loom.ai.a2a.events` already made.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final
from uuid import uuid4

from starlette.responses import Response, StreamingResponse

from loom.ai._transport import (
    HEARTBEAT_MS,
    always_closed,
    annotate_usage,
    failure_event,
    project_output,
    projecting_final,
    with_heartbeats,
)
from loom.ai.a2a._binding import PublishedAgent
from loom.ai.a2a._rpc import (
    RpcFault,
    error_response,
    internal_error,
    invalid_params_error,
    rpc_response,
    task_not_found_error,
    unsupported_error,
)
from loom.ai.a2a.events import A2AEventProjector
from loom.ai.abc import CONVERSATION_ID_MAX_LENGTH, AgentResult
from loom.ai.config import AiConfig
from loom.ai.errors import AgentRunError
from loom.ai.fastapi.response import ENCODER, AgentJSONResponse
from loom.ai.runtime import AgentRuntime
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.core.tracing import get_trace_id

_logger = logging.getLogger(__name__)

_MEDIA_TYPE_SSE: Final[str] = "text/event-stream"

_DATA_PREFIX: Final[bytes] = b"data: "
_FRAME_SUFFIX: Final[bytes] = b"\n\n"

_OUTPUT_ARTIFACT_ID: Final[str] = "output"

_SEND: Final[str] = "message/send"
_STREAM: Final[str] = "message/stream"

_UNSUPPORTED_METHODS: Final[tuple[str, ...]] = (
    "tasks/get",
    "tasks/list",
    "tasks/cancel",
    "tasks/resubscribe",
    "tasks/pushNotificationConfig/set",
    "tasks/pushNotificationConfig/get",
    "tasks/pushNotificationConfig/list",
    "tasks/pushNotificationConfig/delete",
)


@dataclass(frozen=True, slots=True)
class Call:
    """One decoded JSON-RPC call against one published agent."""

    agent: PublishedAgent
    request_id: int | str | None
    params: Mapping[str, Any] | None
    identity: Identity


Handler = Callable[[Call], Awaitable[Response]]


def _text_of(part: object) -> str | None:
    if not isinstance(part, Mapping) or part.get("kind") != "text":
        return None
    text = part.get("text")
    return text if isinstance(text, str) else None


def _message_of(params: Mapping[str, Any] | None) -> Mapping[str, Any]:
    """Return ``params.message``, or an empty mapping when it is not an object.

    An empty mapping rather than a fault: every reader then refuses on the
    field it reads (``-32602`` on the parts), so a missing message and a
    malformed one answer with one refusal shape.
    """
    message = params.get("message") if params is not None else None
    return message if isinstance(message, Mapping) else {}


def _optional_string(message: Mapping[str, Any], key: str) -> str | None:
    """Return ``message[key]`` when it is a non-empty string, ``None`` when absent or empty.

    Raises:
        RpcFault: ``-32602`` when the value is present and not a string.
    """
    value = message.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise RpcFault(invalid_params_error(f"'params.message.{key}' must be a string"))
    return value


def _extract_prompt(message: Mapping[str, Any], *, max_prompt_bytes: int) -> str:
    """Read the caller prompt out of the message parts.

    Raises:
        RpcFault: ``-32602`` when no text part is present or the concatenated
            text exceeds ``ai.max_prompt_bytes``.
    """
    texts = [text for part in message.get("parts") or () if (text := _text_of(part)) is not None]
    if not texts:
        raise RpcFault(invalid_params_error("'params.message.parts' must carry a text part"))
    prompt = "".join(texts)
    if len(prompt.encode("utf-8")) > max_prompt_bytes:
        raise RpcFault(
            invalid_params_error(
                f"the prompt exceeds the maximum accepted size ({max_prompt_bytes} bytes)"
            )
        )
    return prompt


def _refuse_task_id(message: Mapping[str, Any]) -> None:
    """Refuse a message that continues a task: loom retains none.

    Raises:
        RpcFault: ``-32001`` when ``taskId`` is a non-empty string.
    """
    if _optional_string(message, "taskId") is not None:
        raise RpcFault(task_not_found_error("no task is retained; omit 'params.message.taskId'"))


def _thread_of(message: Mapping[str, Any]) -> str:
    """Return the message's ``contextId``, minting one when absent or empty.

    Raises:
        RpcFault: ``-32602`` when ``contextId`` exceeds
            ``CONVERSATION_ID_MAX_LENGTH``; the value itself is never echoed.
    """
    value = _optional_string(message, "contextId")
    if value is None:
        return uuid4().hex
    if len(value) > CONVERSATION_ID_MAX_LENGTH:
        raise RpcFault(
            invalid_params_error(
                "'params.message.contextId' must be a string of at most "
                f"{CONVERSATION_ID_MAX_LENGTH} characters"
            )
        )
    return value


def _read_message(params: Mapping[str, Any] | None, *, max_prompt_bytes: int) -> tuple[str, str]:
    """Return the prompt and the thread id of one message, refusing what cannot run.

    What cannot run is refused before the prompt is read: a message that
    continues a task, or names a malformed thread, answers on that ground even
    when it carries no text part. Runs before any span opens: a refused
    message admits no run and leaves no trace of one.
    """
    message = _message_of(params)
    _refuse_task_id(message)
    context_id = _thread_of(message)
    return _extract_prompt(message, max_prompt_bytes=max_prompt_bytes), context_id


def _task(task_id: str, context_id: str, status: Mapping[str, object]) -> Mapping[str, object]:
    """Build a task in its already serialised (camelCase) wire form."""
    return {"id": task_id, "contextId": context_id, "kind": "task", "status": dict(status)}


def _completed_task(task_id: str, context_id: str, output: object) -> Mapping[str, object]:
    task = dict(_task(task_id, context_id, {"state": "completed"}))
    task["artifacts"] = [
        {"artifactId": _OUTPUT_ARTIFACT_ID, "parts": [{"kind": "data", "data": output}]}
    ]
    return task


def _sse_frame(request_id: int | str | None, event: Mapping[str, object]) -> bytes:
    """Encode one streamed A2A event as its own JSON-RPC response frame."""
    return _DATA_PREFIX + ENCODER.encode(rpc_response(request_id, event)) + _FRAME_SUFFIX


async def _annotated_run(
    runtime: AgentRuntime,
    span: LoomSpan,
    name: str,
    prompt: str,
    identity: Identity,
    conversation_id: str,
) -> AgentResult:
    """Run one agent, publishing what it spent however it ends.

    Mirrors the HTTP surface: an operator querying ``gen_ai.usage.cost`` over
    agent spans must get the same answer whichever surface the caller used.

    Args:
        runtime: Runtime serving the agent.
        span: Open span of this run.
        name: Agent to run.
        prompt: Caller prompt.
        identity: Verified caller.
        conversation_id: Thread the run continues: the message's ``contextId``.

    Returns:
        The completed run's result.

    Raises:
        AgentRunError: Whatever the run failed with, unchanged.
    """
    try:
        result = await runtime.run(name, prompt, identity=identity, conversation_id=conversation_id)
    except AgentRunError as exc:
        annotate_usage(span, exc.usage)
        raise
    annotate_usage(span, result.usage)
    return result


def _make_send_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Handler:
    """Build the ``message/send`` handler: one run, one terminal task."""

    async def send_message(call: Call) -> Response:
        name = call.agent.name
        prompt, context_id = _read_message(call.params, max_prompt_bytes=config.max_prompt_bytes)
        span = observability_runtime.open_span(
            Scope.AGENT,
            "agent_run",
            trace_id=get_trace_id(),
            route=f"{prefix}/{name}",
            method="POST",
            status_code=200,
            agent=name,
            subject=call.identity.subject,
            mechanism=call.identity.mechanism,
        )
        try:
            with always_closed(span), span.as_current():
                result = await _annotated_run(
                    runtime, span, name, prompt, call.identity, conversation_id=context_id
                )
        except AgentRunError as exc:
            _logger.warning("a2a run of agent %r failed: %s", name, exc)
            return error_response(call.request_id, internal_error(exc.code))
        result = project_output(result, runtime.output_type(name))
        task = _completed_task(uuid4().hex, context_id, result.output)
        return AgentJSONResponse(content=rpc_response(call.request_id, task))

    return send_message


def _run_frames(
    runtime: AgentRuntime, call: Call, prompt: str, *, task_id: str, context_id: str
) -> AsyncIterator[bytes]:
    """Project one run onto A2A frames, heartbeats and terminal failure included.

    The run is entered and left by the task consuming the frames; heartbeats
    relay the run's events inside it.
    """
    projector = A2AEventProjector(
        task_id=task_id, context_id=context_id, max_steps=call.agent.max_steps
    )

    async def _projected(events: AsyncIterator[Any]) -> AsyncIterator[bytes]:
        async for event in events:
            for projected in projector.project(event):
                yield _sse_frame(call.request_id, projected)

    async def _frames() -> AsyncIterator[bytes]:
        yield _sse_frame(call.request_id, _task(task_id, context_id, {"state": "submitted"}))
        try:
            async with runtime.run_stream(
                call.agent.name, prompt, identity=call.identity, conversation_id=context_id
            ) as events:
                projected_events = projecting_final(events, runtime.output_type(call.agent.name))
                async for frame in with_heartbeats(
                    _projected(projected_events), heartbeat_ms=HEARTBEAT_MS
                ):
                    yield frame
        except Exception as exc:
            _logger.warning("a2a stream failed after the first byte", exc_info=exc)
            for projected in projector.project(failure_event(exc)):
                yield _sse_frame(call.request_id, projected)

    return _frames()


def _make_stream_handler(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Handler:
    """Build the ``message/stream`` handler: the run's events, as SSE."""

    async def stream_message(call: Call) -> Response:
        name = call.agent.name
        prompt, context_id = _read_message(call.params, max_prompt_bytes=config.max_prompt_bytes)
        task_id = uuid4().hex

        async def _framed() -> AsyncIterator[bytes]:
            with always_closed(
                observability_runtime.open_span(
                    Scope.AGENT,
                    "agent_run",
                    trace_id=get_trace_id(),
                    route=f"{prefix}/{name}",
                    method="POST",
                    agent=name,
                    subject=call.identity.subject,
                    mechanism=call.identity.mechanism,
                )
            ):
                async for frame in _run_frames(
                    runtime, call, prompt, task_id=task_id, context_id=context_id
                ):
                    yield frame

        return StreamingResponse(_framed(), media_type=_MEDIA_TYPE_SSE)

    return stream_message


def _make_unsupported_handler(method: str) -> Handler:
    """Build the handler answering one advertised-absent method with ``-32004``."""

    async def refuse(call: Call) -> Response:
        return error_response(call.request_id, unsupported_error(method))

    return refuse


def make_handlers(
    runtime: AgentRuntime,
    config: AiConfig,
    *,
    prefix: str,
    observability_runtime: ObservabilityRuntime,
) -> Mapping[str, Handler]:
    """Build the method dispatch table, shared by every published agent."""
    handlers: dict[str, Handler] = {
        method: _make_unsupported_handler(method) for method in _UNSUPPORTED_METHODS
    }
    handlers[_SEND] = _make_send_handler(
        runtime, config, prefix=prefix, observability_runtime=observability_runtime
    )
    handlers[_STREAM] = _make_stream_handler(
        runtime, config, prefix=prefix, observability_runtime=observability_runtime
    )
    return handlers
