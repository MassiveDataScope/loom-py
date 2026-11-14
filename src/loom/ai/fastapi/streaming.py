"""Server-sent-events encoding of the agent event stream.

The wire contract: five event names, fixed payload field names, exactly one terminal frame,
``usage`` only on ``final``, and comment frames during long silences.

Two properties are deliberate and load-bearing:

* Encoding is a dispatch map keyed by the event class, resolved once per event
  with a single mapping lookup. A chain of type tests, or reading a type's name
  at run time, would be reflection on the most frequent event of the whole
  pillar.
* Encoding is all this module owns. The heartbeat race that interleaves comment
  frames is the same one the A2A surface needs over a different frame encoding,
  so it lives in :mod:`loom.ai._transport` and :func:`stream_sse` is the
  composition of the two.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any, Final

from loom.ai._transport import HEARTBEAT_FRAME, failure_event, with_heartbeats
from loom.ai.abc import (
    AgentEvent,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.fastapi.response import ENCODER, result_payload

__all__ = ["HEARTBEAT_FRAME", "encode_sse_event", "stream_sse"]

_logger = logging.getLogger("loom.ai.fastapi.streaming")

_EVENT_PREFIX: Final[bytes] = b"event: "
_DATA_PREFIX: Final[bytes] = b"\ndata: "
_FRAME_SUFFIX: Final[bytes] = b"\n\n"


def _text_delta_payload(event: TextDeltaEvent) -> Mapping[str, object]:
    return {"text": event.text}


def _tool_call_payload(event: ToolCallEvent) -> Mapping[str, object]:
    return {"tool": event.tool, "call_id": event.call_id, "arguments": event.arguments}


def _tool_result_payload(event: ToolResultEvent) -> Mapping[str, object]:
    return {"call_id": event.call_id, "ok": event.ok, "summary": event.summary}


def _error_payload(event: ErrorEvent) -> Mapping[str, object]:
    return {"code": event.code, "message": event.message, "interaction_id": event.interaction_id}


_DISPATCH: Mapping[type[Any], tuple[bytes, Callable[[Any], Mapping[str, object]]]] = {
    TextDeltaEvent: (b"text_delta", _text_delta_payload),
    ToolCallEvent: (b"tool_call", _tool_call_payload),
    ToolResultEvent: (b"tool_result", _tool_result_payload),
    ErrorEvent: (b"error", _error_payload),
    FinalEvent: (b"final", result_payload),
}

_TERMINAL_TYPES: frozenset[type[Any]] = frozenset({ErrorEvent, FinalEvent})


def encode_sse_event(event: AgentEvent) -> bytes:
    """Encode one agent event as a single SSE frame.

    The event's name travels on the ``event:`` line, so the union's ``type``
    tag never appears inside the data payload.

    Args:
        event: Event to encode.

    Returns:
        The frame ``event: <name>\\ndata: <json>\\n\\n``.

    Raises:
        KeyError: When the event is not one of the five contract members —
            widening the union without widening this map is a contract break,
            not a silent pass-through.

    Example::

        frame = encode_sse_event(TextDeltaEvent(text="Demand rose "))
    """
    name, payload = _DISPATCH[event.__class__]
    return _EVENT_PREFIX + name + _DATA_PREFIX + ENCODER.encode(payload(event)) + _FRAME_SUFFIX


async def _encoded_events(events: AsyncIterator[AgentEvent]) -> AsyncIterator[bytes]:
    """Encode an agent event stream as SSE frames, terminal frame last.

    A failure raised once the status line is long gone can only travel in-band,
    so it becomes this stream's single terminal frame.
    """
    try:
        async for event in events:
            yield encode_sse_event(event)
            if event.__class__ in _TERMINAL_TYPES:
                return
    except Exception as exc:
        _logger.warning("agent stream failed after the first byte", exc_info=exc)
        yield encode_sse_event(failure_event(exc))


def stream_sse(events: AsyncIterator[AgentEvent], *, heartbeat_ms: int) -> AsyncIterator[bytes]:
    """Encode an agent event stream as SSE frames, with heartbeats.

    Composition of this module's encoder with the transport-wide heartbeat
    race of :func:`~loom.ai._transport.with_heartbeats`: encoding is what the
    HTTP contract owns, the race is not.

    Args:
        events: Agent events to encode, terminal event last.
        heartbeat_ms: Silence after which a comment frame is emitted.

    Returns:
        The encoded SSE frames, ending at the single terminal frame and
        interleaved with heartbeat comment frames.

    Example::

        async for frame in stream_sse(events, heartbeat_ms=15000):
            ...
    """
    return with_heartbeats(_encoded_events(events), heartbeat_ms=heartbeat_ms)
