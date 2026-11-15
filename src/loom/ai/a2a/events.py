"""Pure projection of the agent event union onto A2A streaming events.

Maps each ``AgentEvent`` to its A2A event: both HTTP/SSE and A2A streaming
are projections of the same five-member union.

Two properties are load-bearing:

* The projection is pure — no A2A SDK and no web framework is imported — which
  is what makes the redaction guarantee a unit test.
* A tool call publishes an opaque ordinal and nothing else. Redacting
  the capability wiring from the card and then publishing it event by event
  would be a leak standing next to a guarantee, so neither the capability key,
  its arguments, its correlation id nor the outcome summary is projected.

Like :mod:`loom.ai.fastapi.streaming`, projection is a dispatch map keyed by the
event class: one mapping lookup per event, no reflection on the hottest path of
the pillar.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from enum import StrEnum
from typing import Any, Final

from loom.ai.abc import (
    AgentEvent,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)

_TEXT_ARTIFACT_ID: Final[str] = "response"
_OUTPUT_ARTIFACT_ID: Final[str] = "output"

_ARTIFACT_UPDATE: Final[str] = "artifact-update"
_STATUS_UPDATE: Final[str] = "status-update"


class TaskState(StrEnum):
    """The A2A task states a run is published under."""

    SUBMITTED = "submitted"
    WORKING = "working"
    COMPLETED = "completed"
    FAILED = "failed"


def _text_part(text: str) -> Mapping[str, object]:
    return {"kind": "text", "text": text}


def _data_part(data: object) -> Mapping[str, object]:
    return {"kind": "data", "data": data}


def _agent_message(parts: Sequence[Mapping[str, object]]) -> Mapping[str, object]:
    return {"kind": "message", "role": "agent", "parts": list(parts)}


class A2AEventProjector:
    """Projects one run's agent events onto A2A streaming events.

    A projector belongs to a single run: it holds the correlation ids of that
    run and the tool-call counter backing the opaque ordinal, so it is neither
    shared between runs nor safe to reuse across them.

    Args:
        task_id: Id of the A2A task the run is served as.
        context_id: Id of the A2A context the task belongs to.
        max_steps: Iteration ceiling of the run, published as the denominator
            of the ordinal.

    Example::

        projector = A2AEventProjector(task_id=task, context_id=ctx, max_steps=12)
        for event in events:
            frames = projector.project(event)
    """

    __slots__ = ("_context_id", "_max_steps", "_steps", "_task_id")

    def __init__(self, *, task_id: str, context_id: str, max_steps: int) -> None:
        self._task_id = task_id
        self._context_id = context_id
        self._max_steps = max_steps
        self._steps = 0

    def project(self, event: AgentEvent) -> tuple[Mapping[str, object], ...]:
        """Project one agent event onto the A2A events it maps to.

        Args:
            event: Agent event to project.

        Returns:
            The projected A2A events, in emission order; ``final`` is the only
            member producing more than one.

        Raises:
            KeyError: When the event is not one of the five contract members —
                widening the union without widening this map is a contract
                break, not a silent pass-through.
        """
        return _DISPATCH[event.__class__](self, event)

    def _artifact_update(
        self,
        artifact_id: str,
        parts: Sequence[Mapping[str, object]],
        *,
        append: bool,
        last_chunk: bool,
    ) -> Mapping[str, object]:
        return {
            "kind": _ARTIFACT_UPDATE,
            "taskId": self._task_id,
            "contextId": self._context_id,
            "artifact": {"artifactId": artifact_id, "parts": list(parts)},
            "append": append,
            "lastChunk": last_chunk,
        }

    def _status_update(self, status: Mapping[str, object], *, final: bool) -> Mapping[str, object]:
        return {
            "kind": _STATUS_UPDATE,
            "taskId": self._task_id,
            "contextId": self._context_id,
            "status": status,
            "final": final,
        }

    def _next_ordinal(self) -> str:
        self._steps += 1
        return f"step {self._steps}/{self._max_steps}"

    def _project_text_delta(self, event: TextDeltaEvent) -> tuple[Mapping[str, object], ...]:
        update = self._artifact_update(
            _TEXT_ARTIFACT_ID, (_text_part(event.text),), append=True, last_chunk=False
        )
        return (update,)

    def _project_tool_call(self, _: ToolCallEvent) -> tuple[Mapping[str, object], ...]:
        message = _agent_message((_text_part(self._next_ordinal()),))
        return (self._status_update({"state": TaskState.WORKING, "message": message}, final=False),)

    def _project_tool_result(self, _: ToolResultEvent) -> tuple[Mapping[str, object], ...]:
        return (self._status_update({"state": TaskState.WORKING}, final=False),)

    def _project_final(self, event: FinalEvent) -> tuple[Mapping[str, object], ...]:
        artifact = self._artifact_update(
            _OUTPUT_ARTIFACT_ID, (_data_part(event.output),), append=False, last_chunk=True
        )
        return (artifact, self._status_update({"state": TaskState.COMPLETED}, final=True))

    def _project_error(self, event: ErrorEvent) -> tuple[Mapping[str, object], ...]:
        status = {"state": TaskState.FAILED, "metadata": {"code": str(event.code)}}
        return (self._status_update(status, final=True),)


_DISPATCH: Final[
    Mapping[type[Any], Callable[[A2AEventProjector, Any], tuple[Mapping[str, object], ...]]]
] = {
    TextDeltaEvent: A2AEventProjector._project_text_delta,
    ToolCallEvent: A2AEventProjector._project_tool_call,
    ToolResultEvent: A2AEventProjector._project_tool_result,
    FinalEvent: A2AEventProjector._project_final,
    ErrorEvent: A2AEventProjector._project_error,
}
