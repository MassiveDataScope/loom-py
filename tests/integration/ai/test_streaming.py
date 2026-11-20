"""SSE encoding, termination, heartbeats and cancellation (T074, T092).

The wire contract is ``specs/001-ai-agent-layer/contracts/http-sse.md``: five
event names, fixed payload field names, exactly one terminal frame, ``usage``
only on ``final``, comment heartbeats during silences and no work surviving a
cancelled consumer.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import loom.ai.fastapi.streaming as streaming_module
from loom.ai.abc import (
    AgentEvent,
    AgentUsage,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.errors import AgentRunErrorCode
from loom.ai.fastapi.streaming import HEARTBEAT_FRAME, encode_sse_event, stream_sse
from loom.core.identity import Identity
from tests.integration.ai.conftest import DEFAULT_USAGE, ScriptedEngine

_USAGE = AgentUsage(
    input_tokens=1840,
    output_tokens=412,
    requests=3,
    duration_ms=5210,
    cache_read_tokens=1200,
    cache_write_tokens=64,
    tool_calls=2,
    cost=Decimal("0.0413"),
    details={"reasoning_tokens": 96},
)

_EVENTS: dict[str, tuple[AgentEvent, dict[str, Any]]] = {
    "text_delta": (TextDeltaEvent(text="Demand rose "), {"text": "Demand rose "}),
    "tool_call": (
        ToolCallEvent(tool="sales:velocity", call_id="c1", arguments={"segment": "scooter"}),
        {"tool": "sales:velocity", "call_id": "c1", "arguments": {"segment": "scooter"}},
    ),
    "tool_result": (
        ToolResultEvent(call_id="c1", ok=True, summary="142 rows"),
        {"call_id": "c1", "ok": True, "summary": "142 rows"},
    ),
    "error": (
        ErrorEvent(code=AgentRunErrorCode.PROVIDER_RATE_LIMITED, message="rate limited"),
        {"code": "PROVIDER_RATE_LIMITED", "message": "rate limited", "interaction_id": None},
    ),
    "final": (
        FinalEvent(output={"answer": "42"}, usage=_USAGE),
        {
            "output": {"answer": "42"},
            "usage": {
                "input_tokens": 1840,
                "output_tokens": 412,
                "requests": 3,
                "duration_ms": 5210,
                "cache_read_tokens": 1200,
                "cache_write_tokens": 64,
                "tool_calls": 2,
                "cost": "0.0413",
                "details": {"reasoning_tokens": 96},
            },
            "interaction_id": None,
            "hook_result": None,
        },
    ),
}

_NAMES = tuple(_EVENTS)


def _split_frame(frame: bytes) -> tuple[str, dict[str, Any]]:
    """Split one SSE frame into its event name and decoded data payload.

    Raises:
        AssertionError: If the frame is not exactly
            ``event: <name>\\ndata: <json>\\n\\n``.
    """
    text = frame.decode("utf-8")
    assert text.endswith("\n\n"), f"frame must end with a blank line, got {text!r}"
    lines = text[:-2].split("\n")
    assert len(lines) == 2, f"a frame carries exactly one event line and one data line: {text!r}"
    assert lines[0].startswith("event: "), f"missing event line: {text!r}"
    assert lines[1].startswith("data: "), f"missing data line: {text!r}"
    return lines[0][len("event: ") :], json.loads(lines[1][len("data: ") :])


async def _iterate(events: tuple[AgentEvent, ...]) -> AsyncIterator[AgentEvent]:
    """Yield ``events`` without any delay."""
    for event in events:
        yield event


async def _collect(frames: AsyncIterator[bytes]) -> list[bytes]:
    """Drain an SSE byte stream into a list of frames."""
    return [frame async for frame in frames]


class TestFrameFormat:
    """Every frame matches the published wire shape."""

    @pytest.mark.parametrize("name", _NAMES)
    def test_emits_the_events_name_when_encoding(self, name: str) -> None:
        """The SSE ``event:`` line carries the event's contract name."""
        event, _ = _EVENTS[name]

        assert _split_frame(encode_sse_event(event))[0] == name

    @pytest.mark.parametrize("name", _NAMES)
    def test_emits_the_contracts_fields_when_encoding(self, name: str) -> None:
        """The data payload carries exactly the contract's field names."""
        event, payload = _EVENTS[name]

        assert _split_frame(encode_sse_event(event))[1] == payload

    def test_does_not_emit_messages_even_when_the_final_carries_them(self) -> None:
        """The run's new messages never reach the wire: ``final`` keeps its four keys."""
        event, payload = _EVENTS["final"]
        assert isinstance(event, FinalEvent)
        with_messages = FinalEvent(output=event.output, usage=event.usage, messages=b"[]")

        assert _split_frame(encode_sse_event(with_messages))[1] == payload

    @pytest.mark.parametrize("name", _NAMES)
    def test_does_not_leak_the_type_tag_when_encoding(self, name: str) -> None:
        """The ``type`` tag lives on the event line, never inside the payload."""
        event, _ = _EVENTS[name]

        assert "type" not in _split_frame(encode_sse_event(event))[1]

    def test_emits_the_interaction_id_and_hook_result_when_the_final_carries_them(self) -> None:
        """A ``final`` minted by the host carries the id and the hook's result verbatim."""
        event = FinalEvent(
            output={"answer": "42"},
            usage=_USAGE,
            interaction_id="a" * 32,
            hook_result={"triage_id": "a" * 32},
        )

        payload = _split_frame(encode_sse_event(event))[1]

        assert payload["interaction_id"] == "a" * 32
        assert payload["hook_result"] == {"triage_id": "a" * 32}

    def test_emits_the_interaction_id_when_the_error_carries_it(self) -> None:
        """An ``error`` raised after admission carries the id that names the server log line."""
        event = ErrorEvent(
            code=AgentRunErrorCode.HOOK_FAILED, message="hook failed", interaction_id="b" * 32
        )

        assert _split_frame(encode_sse_event(event))[1]["interaction_id"] == "b" * 32


class TestTermination:
    """Exactly one terminal frame per stream, and ``usage`` only on ``final``."""

    async def test_emits_a_single_terminal_frame_when_the_stream_ends_in_final(self) -> None:
        """A successful stream carries exactly one terminal frame (SC-011)."""
        events = (TextDeltaEvent(text="a"), FinalEvent(output={"answer": "42"}, usage=_USAGE))
        frames = await _collect(stream_sse(_iterate(events), heartbeat_ms=1000))
        names = [_split_frame(frame)[0] for frame in frames]

        assert [name for name in names if name in {"final", "error"}] == ["final"]

    async def test_emits_a_single_terminal_frame_when_the_stream_ends_in_error(self) -> None:
        """A failed stream carries exactly one terminal frame (SC-011)."""
        events = (
            TextDeltaEvent(text="a"),
            ErrorEvent(code=AgentRunErrorCode.PROVIDER_UNAVAILABLE, message="down"),
        )
        frames = await _collect(stream_sse(_iterate(events), heartbeat_ms=1000))
        names = [_split_frame(frame)[0] for frame in frames]

        assert [name for name in names if name in {"final", "error"}] == ["error"]

    async def test_only_final_carries_usage_while_walking_the_stream(self) -> None:
        """No frame other than ``final`` carries a ``usage`` key."""
        events = (
            TextDeltaEvent(text="a"),
            ToolCallEvent(tool="sales:velocity", call_id="c1", arguments={}),
            ToolResultEvent(call_id="c1", ok=True, summary="142 rows"),
            FinalEvent(output={"answer": "42"}, usage=_USAGE),
        )
        frames = await _collect(stream_sse(_iterate(events), heartbeat_ms=1000))
        carriers = [name for name, payload in map(_split_frame, frames) if "usage" in payload]

        assert carriers == ["final"]


class TestInBandFailure:
    """A failure after the first byte can only travel in-band (FR-032)."""

    async def _failing_stream(self) -> AsyncIterator[AgentEvent]:
        yield TextDeltaEvent(text="Demand rose ")
        raise RuntimeError("provider exploded")

    async def test_emits_an_in_band_error_when_it_fails_after_the_first_delta(self) -> None:
        """The injected failure arrives as an ``error`` frame, not an exception."""
        frames = await _collect(stream_sse(self._failing_stream(), heartbeat_ms=1000))
        names = [_split_frame(frame)[0] for frame in frames]

        assert names == ["text_delta", "error"]

    async def test_emits_no_final_when_it_fails_after_the_first_delta(self) -> None:
        """The stream ends at the in-band error; nothing follows it."""
        frames = await _collect(stream_sse(self._failing_stream(), heartbeat_ms=1000))
        names = [_split_frame(frame)[0] for frame in frames]

        assert "final" not in names


class TestDispatchWithoutReflection:
    """The encoder dispatches on the event class, never on reflection (T084)."""

    def test_dispatches_by_class_when_encoding(self) -> None:
        """The module-level dispatch map covers exactly the five event types."""
        expected = {TextDeltaEvent, ToolCallEvent, ToolResultEvent, ErrorEvent, FinalEvent}

        assert set(streaming_module._DISPATCH) == expected

    def test_uses_no_isinstance_when_encoding(self) -> None:
        """No ``isinstance`` chain in the module: dispatch is a map lookup."""
        source = Path(str(streaming_module.__file__)).read_text(encoding="utf-8")

        assert "isinstance(" not in source

    def test_uses_no_dunder_name_when_encoding(self) -> None:
        """No ``__name__`` reflection on the most frequent event of all."""
        source = Path(str(streaming_module.__file__)).read_text(encoding="utf-8")

        assert "__name__" not in source


class TestHeartbeats:
    """Comment frames keep a silent stream alive without a background task."""

    async def _silent_then_events(self) -> AsyncIterator[AgentEvent]:
        await asyncio.sleep(0.045)
        yield TextDeltaEvent(text="late")
        yield FinalEvent(output={"answer": "42"}, usage=_USAGE)

    async def test_emits_a_ping_when_the_stream_goes_silent(self) -> None:
        """A silence longer than ``heartbeat_ms`` produces comment frames."""
        frames = await _collect(stream_sse(self._silent_then_events(), heartbeat_ms=10))

        assert frames.count(HEARTBEAT_FRAME) >= 2

    async def test_keeps_the_order_after_heartbeats(self) -> None:
        """The events after a silence still arrive, and in order."""
        frames = await _collect(stream_sse(self._silent_then_events(), heartbeat_ms=10))
        names = [_split_frame(frame)[0] for frame in frames if frame != HEARTBEAT_FRAME]

        assert names == ["text_delta", "final"]


class TestCancellation:
    """Cancelling the consumer cancels the run and leaves nothing behind (T092)."""

    @staticmethod
    def _hanging_engine() -> ScriptedEngine:
        """Engine emitting one delta and then stalling before the terminal event."""
        return ScriptedEngine(
            script=(
                TextDeltaEvent(text="first"),
                FinalEvent(output={"answer": "42"}, usage=DEFAULT_USAGE),
            ),
            delays_ms=(0, 500),
        )

    @staticmethod
    async def _consume(engine: ScriptedEngine, identity: Identity, seen: asyncio.Event) -> None:
        async with engine.run_stream("prompt", identity=identity) as events:
            async for _frame in stream_sse(events, heartbeat_ms=1000):
                seen.set()

    async def _cancel_mid_stream(
        self, engine: ScriptedEngine, identity: Identity
    ) -> set[asyncio.Task[Any]]:
        """Cancel a consumer mid-stream; return the tasks that outlived it."""
        before = asyncio.all_tasks()
        seen = asyncio.Event()
        consumer = asyncio.create_task(self._consume(engine, identity, seen))
        await seen.wait()
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await consumer
        await asyncio.sleep(0)
        return asyncio.all_tasks() - before - {asyncio.current_task()}  # type: ignore[arg-type]

    async def test_the_engine_observes_cancelled_when_the_consumer_is_cancelled(
        self, identity: Identity
    ) -> None:
        """The engine stream sees ``CancelledError``, so the run really stops."""
        engine = self._hanging_engine()

        await self._cancel_mid_stream(engine, identity)

        assert engine.cancelled is True

    async def test_no_task_survives_when_the_consumer_is_cancelled(
        self, identity: Identity
    ) -> None:
        """No task outlives the cancelled consumer (no bare ``create_task``)."""
        engine = self._hanging_engine()

        survivors = await self._cancel_mid_stream(engine, identity)

        assert survivors == set()

    async def test_emits_no_more_events_when_the_consumer_is_cancelled(
        self, identity: Identity
    ) -> None:
        """Nothing is produced after cancellation (FR-033)."""
        engine = self._hanging_engine()

        await self._cancel_mid_stream(engine, identity)
        await asyncio.sleep(0.02)

        assert engine.emitted == [TextDeltaEvent(text="first")]
