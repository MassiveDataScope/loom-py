"""Transport primitives shared by the HTTP and the A2A agent surfaces.

Both surfaces put the same run behind two different wire protocols, so both
need the same request-path rules: the body cap that is applied *while* the body
is read, the caller check evaluated before existence, the span that closes on a
disconnect, and the heartbeat race that keeps a silent stream alive without
letting a timeout tear down the run it is protecting.

This module owns those rules once. It is deliberately protocol-agnostic:
:func:`with_heartbeats` relays already-encoded ``bytes``, so the HTTP surface
can feed it SSE event frames and the A2A surface JSON-RPC envelopes, and
neither has to reimplement the race. Everything above the frame — which frames
exist, what they are named, how many an agent event projects to — stays with
the transport that defines it.

Nothing here renders a response: the two surfaces encode their own bodies, so
this module imports Starlette's ``Request`` and nothing else from either.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Final, TypeVar

import msgspec
from starlette.requests import Request

from loom.ai.abc import AgentEvent, AgentResult, AgentUsage, ErrorEvent, FinalEvent
from loom.ai.config import AgentEndpointConfig
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.identity import Identity, current_identity
from loom.core.model import LoomType
from loom.core.observability.span import LoomSpan

_WireResultT = TypeVar("_WireResultT", AgentResult, FinalEvent)

BODY_OVERHEAD_BYTES: Final[int] = 64 * 1024
"""Headroom over the declared field caps for the JSON envelope around them.

The agent surface's body cap is ``max_prompt_bytes + max_state_bytes +
BODY_OVERHEAD_BYTES``; anything larger is refused before buffering
beyond the cap. Every other surface using this constant has no ``state``
field, so its own cap stays ``max_prompt_bytes + BODY_OVERHEAD_BYTES``.
"""

HEARTBEAT_MS: Final[int] = 15_000
"""Silence after which a streaming surface emits one comment frame.

Well under the 30 s idle timeout of the common reverse proxies, and not
configurable: it is a property of the transport, not of an agent.
"""

HEARTBEAT_FRAME: Final[bytes] = b": ping\n\n"
"""SSE comment frame keeping a silent stream alive; clients ignore it."""

_UNEXPECTED_FAILURE: Final[str] = "the agent run failed unexpectedly"


class TransportError(Exception):
    """A failure that is still a status code because no byte was sent yet.

    Rendering is left to the caller: this module stays free of any response
    type, so it never has to import a transport's encoder and the import graph
    keeps pointing one way — from a surface into the shared rules, never back.

    Args:
        status_code: HTTP status to answer with.
        code: Stable machine-readable code carried in the body.
        message: Human-readable description, safe to return to the caller.

    Attributes:
        status_code: HTTP status to answer with.
        code: Stable machine-readable code carried in the body.
        message: Human-readable description, safe to return to the caller.
    """

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _declared_content_length(request: Request) -> int | None:
    """Return the Content-Length header as an int, or ``None`` when unusable."""
    header = request.headers.get("content-length")
    if header is None:
        return None
    try:
        return int(header)
    except ValueError:
        return None


def _prompt_too_large(max_bytes: int) -> TransportError:
    return TransportError(
        413,
        "PROMPT_TOO_LARGE",
        f"Request body exceeds the maximum accepted size ({max_bytes} bytes)",
    )


async def read_body_capped(request: Request, *, max_bytes: int) -> bytes:
    """Read the request body without ever buffering more than *max_bytes*.

    The Content-Length check is only a fast path for honest clients; the capped
    stream read is the authoritative defense — it covers chunked bodies and
    lying headers too, aborting as soon as the cap is exceeded.

    Args:
        request: Incoming request whose body is read.
        max_bytes: Maximum number of bytes ever buffered.

    Returns:
        The body bytes.

    Raises:
        TransportError: 413 ``PROMPT_TOO_LARGE`` when the cap is hit.

    Example::

        body = await read_body_capped(request, max_bytes=cap)
    """
    declared = _declared_content_length(request)
    if declared is not None and declared > max_bytes:
        raise _prompt_too_large(max_bytes)
    received = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        received += len(chunk)
        if received > max_bytes:
            raise _prompt_too_large(max_bytes)
        chunks.append(chunk)
    return b"".join(chunks)


def require_caller(name: str, endpoint: AgentEndpointConfig | None) -> Identity:
    """Return the verified caller, refusing anonymous ones before existence.

    An unknown agent has no ``allow_anonymous`` opt-out, so an anonymous probe
    is refused with 401 whatever the name — which is what stops an agent
    surface from being used as an agent directory.

    Args:
        name: Agent the caller addressed.
        endpoint: Endpoint configuration of that agent, or ``None`` when the
            agent is unknown to this surface.

    Returns:
        The verified caller, or the anonymous identity when the agent declares
        ``allow_anonymous``.

    Raises:
        TransportError: 401 for an anonymous caller without an opt-out.

    Example::

        identity = require_caller("analyst", exposed.get("analyst"))
    """
    identity = current_identity()
    if identity.is_authenticated or (endpoint is not None and endpoint.allow_anonymous):
        return identity
    raise TransportError(401, "UNAUTHORIZED", f"agent {name!r} requires a verified caller")


_USAGE_PREFIX: Final[str] = "gen_ai.usage."
"""Prefix of the OpenTelemetry GenAI usage attributes both surfaces publish."""


def usage_attributes(usage: AgentUsage) -> dict[str, object]:
    """Return the closing span attributes carrying one run's usage.

    Mapped from loom's own :class:`~loom.ai.abc.AgentUsage`, not from the
    engine's usage type, so a second engine publishes the same attributes and
    no engine type reaches a transport. The names follow the OpenTelemetry
    GenAI semantic conventions where those define one, and stay under the same
    prefix where they do not — cost, requests and tool calls, which the
    conventions leave out and an operator comparing models needs most.

    An unpriced model contributes no cost key at all — a zero would read as a
    free run and win a cost comparison it never entered — but it always
    contributes ``cost_known``, so a dashboard summing the cost can tell a
    complete total from a lower bound instead of silently reporting one as the
    other.

    Args:
        usage: Accounting of the run that is about to close its span.

    Returns:
        The attributes, ready for
        :meth:`~loom.core.observability.span.LoomSpan.annotate`.
    """
    attributes: dict[str, object] = {
        f"{_USAGE_PREFIX}input_tokens": usage.input_tokens,
        f"{_USAGE_PREFIX}output_tokens": usage.output_tokens,
        f"{_USAGE_PREFIX}cache_read.input_tokens": usage.cache_read_tokens,
        f"{_USAGE_PREFIX}cache_creation.input_tokens": usage.cache_write_tokens,
        f"{_USAGE_PREFIX}requests": usage.requests,
        f"{_USAGE_PREFIX}tool_calls": usage.tool_calls,
        f"{_USAGE_PREFIX}cost_known": usage.cost is not None,
    }
    if usage.cost is not None:
        attributes[f"{_USAGE_PREFIX}cost"] = float(usage.cost)
    for name, value in usage.details.items():
        attributes[f"{_USAGE_PREFIX}details.{name}"] = value
    return attributes


def annotate_usage(span: LoomSpan, usage: AgentUsage | None) -> None:
    """Publish *usage* on the closing attributes of *span*, when there is any.

    A run that fails before the engine measured anything — a refusal, a limit
    that killed it from outside the engine — has no usage to publish, and an
    invented zero would be read as a free run.

    Args:
        span: Open span of the run.
        usage: What the run spent, or ``None`` when nothing was measurable.
    """
    if usage is None:
        return
    span.annotate(usage_attributes(usage))


@contextmanager
def always_closed(span: LoomSpan) -> Iterator[None]:
    """Close *span* whatever ends the body, including a client disconnect.

    A client that walks away from a stream ends the generator serving it with
    ``CancelledError`` or ``GeneratorExit`` — neither is an ``Exception`` — so
    without this adapter every abandoned stream would leave a span that emitted
    ``START`` and nothing else. A disconnect closes the span as a normal end:
    the run was not the thing that failed.

    The span is an already-open
    :class:`~loom.core.observability.span.LoomSpan` rather than a context
    manager entered here: the two ends of a streaming run happen in two
    different ``asend`` calls, so a span made current on entry would have its
    context token detached in a context that never attached it.

    Args:
        span: Open span handle to close exactly once.

    Yields:
        ``None``, with the span open.

    Example::

        with always_closed(observability.open_span(Scope.AGENT, "agent_run")):
            ...
    """
    try:
        yield
    except Exception as exc:
        span.fail(exc)
        raise
    except BaseException:
        span.end()
        raise
    else:
        span.end()


def project_output(value: _WireResultT, output_type: LoomType) -> _WireResultT:
    """Rewrite ``value.output`` to builtins before it reaches a wire encoder.

    The one place every surface — ``/run``, the SSE ``final`` frame and both
    A2A paths — projects the run's already-validated answer, so
    ``result_payload``, ``encode_sse_event``, ``_data_part`` and
    ``_completed_task`` keep encoding whatever ``output`` already holds.

    Args:
        value: Result or terminal event whose ``output`` is still the
            engine's validated instance.
        output_type: The run's compiled boundary type, read from
            :meth:`~loom.ai.execution.AgentRuntime.output_type`.

    Returns:
        *value* with ``output`` replaced by its builtins projection.
    """
    return msgspec.structs.replace(value, output=output_type.to_builtins(value.output))


async def with_projected_output(
    events: AsyncIterator[AgentEvent], output_type: LoomType
) -> AsyncIterator[AgentEvent]:
    """Relay *events*, projecting the terminal ``final`` event's ``output``.

    Every event but ``final`` is yielded unchanged; ``final`` is passed
    through :func:`project_output` first, so a consumer downstream — the SSE
    encoder, the A2A projector — never sees the engine's validated instance. Both agent
    surfaces wrap their event stream with this generator before their own projection
    consumes it.

    Args:
        events: Run events, terminal event last.
        output_type: The run's compiled boundary type, read from
            :meth:`~loom.ai.execution.AgentRuntime.output_type`.

    Yields:
        Every event, in order; the ``final`` event's ``output`` projected.
    """
    async for event in events:
        if isinstance(event, FinalEvent):
            event = project_output(event, output_type)
        yield event


def failure_event(exc: BaseException) -> ErrorEvent:
    """Turn a post-first-byte failure into a stream's terminal error event.

    The test is ``isinstance``, not an exact class match: a deployment that
    subclasses :class:`~loom.ai.errors.AgentRunError` to carry extra context
    still owns a stable code, and dropping it would answer a truthful
    ``TOOL_TIMEOUT`` as an invented ``PROVIDER_UNAVAILABLE``.

    Args:
        exc: Failure raised once the status line was already committed.

    Returns:
        The ``error`` event carrying the failure's code, or the catch-all
        ``PROVIDER_UNAVAILABLE`` with a fixed message for anything else.

    Example::

        event = failure_event(AgentRunError(AgentRunErrorCode.RUN_TIMEOUT, "late"))
    """
    if isinstance(exc, AgentRunError):
        return ErrorEvent(
            code=exc.code,
            message=str(exc),
            interaction_id=exc.interaction_id,
            usage=exc.usage,
        )
    return ErrorEvent(code=AgentRunErrorCode.PROVIDER_UNAVAILABLE, message=_UNEXPECTED_FAILURE)


async def _next_frame(frames: AsyncIterator[bytes]) -> bytes | None:
    """Await the next frame, reporting exhaustion as ``None``."""
    try:
        return await anext(frames)
    except StopAsyncIteration:
        return None


async def _drain(pending: asyncio.Future[bytes | None] | None) -> None:
    """Cancel the awaited frame and wait for it, so nothing outlives the stream.

    The wait is shielded and repeated until the frame task is done. A server
    that cancels its handler again at every checkpoint (an ``anyio`` cancel
    scope does, after a disconnect) would otherwise cancel the frame task a
    second time through the bare await — cutting the output hook it shields —
    and then return early, letting the caller close a generator that task is
    still running.
    """
    if pending is None:
        return
    pending.cancel()
    while not pending.done():
        try:
            await asyncio.shield(pending)
        except (Exception, asyncio.CancelledError):
            continue


async def with_heartbeats(
    frames: AsyncIterator[bytes], *, heartbeat_ms: int
) -> AsyncIterator[bytes]:
    """Relay encoded frames, emitting a comment frame during long silences.

    Every silence longer than *heartbeat_ms* produces one
    :data:`HEARTBEAT_FRAME` and the race starts again. The awaited frame is
    shielded from the heartbeat — a timeout must not tear down the run it is
    keeping alive — and is cancelled and drained when the consumer goes away,
    so no work outlives it.

    The race lives in this generator rather than in a background task feeding a
    queue: a background task survives the disconnect cancellation and keeps a
    run burning tokens with nobody listening.

    Args:
        frames: Already-encoded frames to relay, in order; exhaustion ends the
            stream.
        heartbeat_ms: Silence after which a comment frame is emitted.

    Yields:
        The relayed frames, interleaved with heartbeat comment frames.

    Example::

        async for frame in with_heartbeats(encoded, heartbeat_ms=HEARTBEAT_MS):
            ...
    """
    iterator = frames.__aiter__()
    beat = heartbeat_ms / 1000
    pending: asyncio.Future[bytes | None] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(_next_frame(iterator))
            try:
                async with asyncio.timeout(beat):
                    frame = await asyncio.shield(pending)
            except TimeoutError:
                yield HEARTBEAT_FRAME
                continue
            pending = None
            if frame is None:
                return
            yield frame
    finally:
        await _drain(pending)
