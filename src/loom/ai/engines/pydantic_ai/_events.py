"""Engine stream events projected onto loom's closed :data:`~loom.ai.abc.AgentEvent` union.

A tool result's summary is built from the closed shapes a loom toolset
publishes in ``metadata["loom"]``, never from the payload; a ``refused``
shape also reads ``ok=False``. Malformed tool-call arguments become ``{}``.
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
from loom.ai.engines.pydantic_ai._returns import RESERVED_METADATA_KEY, ReturnShape

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
        "custom",
        "capability",
    }
)
"""Engine event kinds with no loom counterpart; carried by no consumer."""

_TOOL_RETRY = "the model was asked to retry the call"
_OUTCOME_SUMMARIES: Mapping[str, str] = MappingProxyType(
    {
        "retried": _TOOL_RETRY,
        "failed": "the call failed",
        "denied": "the call was denied",
        "interrupted": "the call was interrupted",
    }
)
"""The fixed summary of each engine outcome that is not a success."""

_COUNTED_SHAPES: frozenset[str] = frozenset({ReturnShape.ROWS})
"""Shapes whose summary is a count and a unit; every other shape reads ``ok``."""


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
    facts = metadata.get(RESERVED_METADATA_KEY)
    return facts if isinstance(facts, Mapping) else {}


def _summary(part: object) -> str:
    """Build the summary from the facts alone; never from the payload."""
    facts = _facts(part)
    shape = facts.get("shape")
    count = facts.get("n")
    if shape in _COUNTED_SHAPES and isinstance(count, int):
        return f"{count} {shape}"
    return ReturnShape.OK


def _tool_result(event: FunctionToolResultEvent) -> AgentEvent:
    part = event.part
    if isinstance(part, RetryPromptPart):
        return ToolResultEvent(call_id=part.tool_call_id, ok=False, summary=_TOOL_RETRY)
    failure = _OUTCOME_SUMMARIES.get(getattr(part, "outcome", "success"))
    if failure is not None:
        return ToolResultEvent(call_id=part.tool_call_id, ok=False, summary=failure)
    if _facts(part).get("shape") == ReturnShape.REFUSED:
        return ToolResultEvent(call_id=part.tool_call_id, ok=False, summary=ReturnShape.REFUSED)
    return ToolResultEvent(call_id=part.tool_call_id, ok=True, summary=_summary(part))


def translate(event: object) -> AgentEvent | None:
    """Project one engine event onto the loom union; ``agent_run_result`` is not projected.

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
