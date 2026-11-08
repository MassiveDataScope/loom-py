"""Engine stream events → loom's closed :data:`~loom.ai.abc.AgentEvent` union.

The union has five members and gains a sixth only when two independent
consumers need it (FR-035), so this translation **projects onto what exists
and invents nothing**. pydantic-ai publishes twenty-odd event kinds; four
carry information a loom consumer can act on, and the rest are structural
(part boundaries, availability deltas, realtime-session signals).

Structural kinds are dropped rather than turned into an ``error``: an
``error`` terminates the stream (SC-011), so reporting one for a
``part_end`` would fail a healthy run. Drift is caught instead by a coverage
test that fails when pydantic-ai adds a kind this module has not been taught
— explicit, and at test time rather than in production.

Two translations carry a contract of their own.

``tool_result.summary`` is built by loom from the structured facts a loom
toolset publishes in ``metadata["loom"]``, and only from a closed list of
shapes (FR-030b). A tool result is model-influenced content: were the summary
read from the payload — or from a free-form string a tool supplied — a tool
could dictate what every SSE consumer displays, and a large payload would be
copied into every event. An unknown or absent shape degrades to ``"ok"``.

The ``refused`` shape is the one that also flips ``ok``: a tripped bound or a
contained failure must not read as a normal call in the stream. Its summary is
the fixed word ``"refused"`` — carrying the reason would put tool-authored text
back into the summary, which is exactly what FR-030b forbids.

``tool_call.arguments`` is a decoded mapping, decoded exactly once here. The
engine hands the raw argument string through unparsed when the model emits
malformed JSON — ``ToolCallPart.args_as_dict()`` answers
``{"INVALID_JSON": "<the raw string>"}`` rather than raising — so relaying it
would leak the model's unparsed text to every consumer as if it were
structured arguments. Malformed arguments therefore become ``{}``.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import msgspec
from pydantic_ai.messages import (
    FunctionToolCallEvent,
    FunctionToolResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    RetryPromptPart,
    TextPart,
    TextPartDelta,
    ToolCallPart,
)

from loom.ai.abc import AgentEvent, TextDeltaEvent, ToolCallEvent, ToolResultEvent

MAPPED_EVENT_KINDS: frozenset[str] = frozenset(
    {"part_start", "part_delta", "function_tool_call", "function_tool_result", "agent_run_result"}
)
"""Engine event kinds this module projects onto the loom union."""

IGNORED_EVENT_KINDS: frozenset[str] = frozenset(
    {
        "part_end",
        "final_result",
        "enqueued_messages",
        "tool_availability_delta",
        "capability_load_delta",
        "model_settings_delta",
        "output_tool_call",
        "output_tool_result",
        "deferred_tool_requests",
        "deferred_tool_results",
        "realtime_turn_complete",
        "realtime_input_speech_start",
        "realtime_input_speech_end",
        "realtime_output_speech_start",
        "realtime_output_speech_end",
        "realtime_response_interrupted",
        "realtime_input_transcription_error",
        "realtime_session_reconnect",
        "realtime_session_error",
        # An application's own event and a capability's typed event, emitted into
        # the engine's stream by code loom never runs; the closed loom union has
        # no member for either (FR-035).
        "custom",
        "capability",
    }
)
"""Engine event kinds with no loom counterpart; carried by no consumer."""

_TOOL_OK = "ok"
_TOOL_RETRY = "the model was asked to retry the call"
_TOOL_REFUSED = "refused"
_TOOL_FAILED = "the call failed"
_TOOL_DENIED = "the call was denied"
_TOOL_INTERRUPTED = "the call was interrupted"
_OUTCOME_SUMMARIES: Mapping[str, str] = MappingProxyType(
    {
        "retried": _TOOL_RETRY,
        "failed": _TOOL_FAILED,
        "denied": _TOOL_DENIED,
        "interrupted": _TOOL_INTERRUPTED,
    }
)
"""The summary of each engine outcome that is not a success.

From pydantic-ai 2.66 a retried, failed, denied or interrupted call returns as
a ``ToolReturnPart`` whose ``outcome`` says so; before, a retry was a
``RetryPromptPart`` and the part carried no outcome. Each summary is fixed, so
the event never quotes what the tool or the model wrote (FR-030b)."""

_COUNTED_SHAPES: frozenset[str] = frozenset({"rows"})
"""Shapes whose summary is a count and a unit; every other shape reads ``ok``."""

_REFUSED_SHAPE = "refused"
"""Shape a loom toolset publishes when it answers a refusal instead of data."""


def _text_from_start(event: PartStartEvent) -> AgentEvent | None:
    part = event.part
    if isinstance(part, TextPart) and part.content:
        return TextDeltaEvent(text=part.content)
    return None


def _text_from_delta(event: PartDeltaEvent) -> AgentEvent | None:
    delta = event.delta
    if isinstance(delta, TextPartDelta) and delta.content_delta:
        return TextDeltaEvent(text=delta.content_delta)
    return None


def _arguments(part: ToolCallPart) -> dict[str, Any]:
    """Decode the call arguments once, never relaying an unparsed string."""
    args = part.args
    if isinstance(args, Mapping):
        return dict(args)
    if not isinstance(args, str | bytes):
        return {}
    try:
        decoded = msgspec.json.decode(args)
    except msgspec.DecodeError:
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _tool_call(event: FunctionToolCallEvent) -> AgentEvent:
    part = event.part
    return ToolCallEvent(tool=part.tool_name, call_id=part.tool_call_id, arguments=_arguments(part))


def _facts(part: object) -> Mapping[str, Any]:
    """Read the structured facts a loom toolset published, if any."""
    metadata = getattr(part, "metadata", None)
    if not isinstance(metadata, Mapping):
        return {}
    facts = metadata.get("loom")
    return facts if isinstance(facts, Mapping) else {}


def _summary(part: object) -> str:
    """Build the summary from the facts alone; never from the payload (FR-030b)."""
    facts = _facts(part)
    shape = facts.get("shape")
    count = facts.get("n")
    if shape in _COUNTED_SHAPES and isinstance(count, int):
        return f"{count} {shape}"
    return _TOOL_OK


def _tool_result(event: FunctionToolResultEvent) -> AgentEvent:
    part = event.part
    if isinstance(part, RetryPromptPart):
        return ToolResultEvent(call_id=part.tool_call_id, ok=False, summary=_TOOL_RETRY)
    failure = _OUTCOME_SUMMARIES.get(getattr(part, "outcome", "success"))
    if failure is not None:
        return ToolResultEvent(call_id=part.tool_call_id, ok=False, summary=failure)
    if _facts(part).get("shape") == _REFUSED_SHAPE:
        return ToolResultEvent(call_id=part.tool_call_id, ok=False, summary=_TOOL_REFUSED)
    return ToolResultEvent(call_id=part.tool_call_id, ok=True, summary=_summary(part))


def translate(event: object) -> AgentEvent | None:
    """Project one engine event onto the loom union.

    ``agent_run_result`` is deliberately absent: the terminal ``final`` event
    carries the *validated* answer and the run's usage, so it is built by the
    engine adapter, which owns the decode.

    Args:
        event: Event yielded by the engine's stream.

    Returns:
        The loom event, or ``None`` when the kind carries nothing a loom
        consumer can act on.
    """
    if isinstance(event, PartStartEvent):
        return _text_from_start(event)
    if isinstance(event, PartDeltaEvent):
        return _text_from_delta(event)
    if isinstance(event, FunctionToolCallEvent):
        return _tool_call(event)
    if isinstance(event, FunctionToolResultEvent):
        return _tool_result(event)
    return None
