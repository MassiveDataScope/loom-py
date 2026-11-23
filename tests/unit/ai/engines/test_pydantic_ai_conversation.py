"""Conversation memory through the pydantic-ai adapter (AC11, AC12).

The engine decodes the history loom hands it, replays it to the model before
the new prompt, forwards loom's ``conversation_id`` so every new message is
stamped with it, and returns only this run's messages. A history that does
not validate fails before the model is called, with nothing spent.
"""

from __future__ import annotations

import logging

import pytest
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

from loom.ai.abc import AgentEngine, AgentEvent, Conversation, ErrorEvent, FinalEvent
from loom.ai.declarative import PolicySpec
from loom.ai.declarative._v1 import MAX_HISTORY_BYTES_MIN
from loom.ai.errors import CONVERSATION_LOAD_FAILED_MESSAGE, AgentRunError, AgentRunErrorCode
from loom.ai.event import AiEvent
from loom.core.identity import Identity
from tests.helpers.loggers import CapturedLog
from tests.helpers.pydantic_ai_engine import (
    build_engine,
    encode,
    flaky_model,
    make_plan,
    recording_model,
)

_IDENTITY = Identity(subject="caller")
_PROMPT = "and now?"
_ANSWER = {"answer": "still fine"}
_CONVERSATION_ID = "c-42"

# An output-tool answer is closed by pydantic-ai with a tool-return request,
# so one turn over this plan is request, response, tool-return request.
_TURN_KINDS = ("request", "response", "request")


def _history() -> bytes:
    prior: list[ModelMessage] = [
        ModelRequest(parts=[UserPromptPart(content="how is the cluster?")]),
        ModelResponse(parts=[TextPart(content="fine")]),
    ]
    return ModelMessagesTypeAdapter.dump_json(prior)


def _conversation(history: bytes | None = None) -> Conversation:
    return Conversation(conversation_id=_CONVERSATION_ID, history=history)


def _engine(seen: list[list[ModelMessage]], *, retries: int = 0, failures: int = 0) -> AgentEngine:
    payload = encode(_ANSWER)
    model = (
        flaky_model(failures, payload, seen=seen) if failures else recording_model(payload, seen)
    )
    return build_engine(make_plan(retries=retries), model)


def _decode(raw: bytes | None) -> list[ModelMessage]:
    assert raw is not None
    return ModelMessagesTypeAdapter.validate_json(raw)


def _prompts(messages: list[ModelMessage]) -> list[str]:
    return [
        str(part.content)
        for message in messages
        if isinstance(message, ModelRequest)
        for part in message.parts
        if isinstance(part, UserPromptPart)
    ]


async def _engine_after_outage() -> AgentEngine:
    """An engine whose last provider outcome was an outage: health is ``unavailable``."""
    engine = _engine([], failures=1)
    with pytest.raises(AgentRunError) as outage:
        await engine.run(_PROMPT, identity=_IDENTITY, conversation=None)
    assert outage.value.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
    assert (await engine.health()).status == "unavailable"
    return engine


async def _collect(engine: AgentEngine, conversation: Conversation | None) -> list[AgentEvent]:
    async with engine.run_stream(_PROMPT, identity=_IDENTITY, conversation=conversation) as events:
        return [event async for event in events]


def _final(events: list[AgentEvent]) -> FinalEvent:
    final = events[-1]
    assert isinstance(final, FinalEvent), events
    return final


def _assert_this_turn_only(raw: bytes | None) -> None:
    """The bytes hold this run's messages, each stamped with loom's id."""
    new = _decode(raw)

    assert tuple(message.kind for message in new) == _TURN_KINDS
    assert _prompts(new) == [_PROMPT]
    assert [message.conversation_id for message in new] == [_CONVERSATION_ID] * len(new)


class TestHistory:
    async def test_the_model_sees_the_history_before_the_prompt_when_run_carries_a_conversation(
        self,
    ) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        await engine.run(_PROMPT, identity=_IDENTITY, conversation=_conversation(_history()))

        assert len(seen) == 1
        assert [message.kind for message in seen[0]] == ["request", "response", "request"]
        assert _prompts(seen[0]) == ["how is the cluster?", _PROMPT]

    async def test_returns_only_this_turns_messages_when_run_carries_a_conversation(
        self,
    ) -> None:
        engine = _engine([])

        result = await engine.run(
            _PROMPT, identity=_IDENTITY, conversation=_conversation(_history())
        )

        _assert_this_turn_only(result.messages)

    async def test_the_final_carries_the_same_messages_when_the_stream_carries_a_conversation(
        self,
    ) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        events = await _collect(engine, _conversation(_history()))

        assert _prompts(seen[0]) == ["how is the cluster?", _PROMPT]
        _assert_this_turn_only(_final(events).messages)

    async def test_stamps_the_id_when_the_conversation_has_no_history(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        result = await engine.run(_PROMPT, identity=_IDENTITY, conversation=_conversation())

        assert _prompts(seen[0]) == [_PROMPT]
        _assert_this_turn_only(result.messages)


class TestSingleShot:
    async def test_serialises_this_turns_messages_when_run_carries_no_conversation(self) -> None:
        """L19: the hook can see the tool traffic of a single-shot run (T901)."""
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        result = await engine.run(_PROMPT, identity=_IDENTITY, conversation=None)

        assert _prompts(seen[0]) == [_PROMPT]
        assert result.messages is not None
        assert _prompts(_decode(result.messages)) == [_PROMPT]

    async def test_serialises_this_turns_messages_when_the_stream_carries_no_conversation(
        self,
    ) -> None:
        """L19: a streamed single-shot run's terminal event carries the same traffic."""
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        events = await _collect(engine, None)

        assert len(seen) == 1
        messages = _final(events).messages
        assert messages is not None
        assert _prompts(_decode(messages)) == [_PROMPT]


class TestOverBound:
    async def test_gives_none_and_logs_the_agent_size_and_bound_when_messages_exceed_it(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """L19: over ``max_history_bytes`` the hook gets ``None`` and the run logs why (T901)."""
        oversized = encode({"answer": "x" * MAX_HISTORY_BYTES_MIN})
        engine = build_engine(
            make_plan(policies=PolicySpec(max_history_bytes=MAX_HISTORY_BYTES_MIN)),
            recording_model(oversized, []),
        )

        with caplog.at_level(logging.WARNING, logger="loom.ai.engines.pydantic_ai._history"):
            result = await engine.run(_PROMPT, identity=_IDENTITY, conversation=None)

        assert result.messages is None
        assert CapturedLog(caplog).messages == [AiEvent.HISTORY_TOO_LARGE]

    async def test_gives_none_when_the_stream_carries_messages_over_the_bound(self) -> None:
        """The streamed terminal event is bounded the same way as the non-streaming run."""
        oversized = encode({"answer": "x" * MAX_HISTORY_BYTES_MIN})
        engine = build_engine(
            make_plan(policies=PolicySpec(max_history_bytes=MAX_HISTORY_BYTES_MIN)),
            recording_model(oversized, []),
        )

        events = await _collect(engine, None)

        assert _final(events).messages is None


class TestMalformedHistory:
    async def test_run_fails_without_calling_the_model_when_the_history_is_not_json(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        conversation = _conversation(b"not json")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(failure.value) == CONVERSATION_LOAD_FAILED_MESSAGE
        assert failure.value.usage is None
        assert seen == []

    async def test_the_stream_emits_an_error_without_calling_the_model_when_the_history_is_not_json(
        self,
    ) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen)

        events = await _collect(engine, _conversation(b"not json"))

        assert events == [
            ErrorEvent(
                code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
                message=CONVERSATION_LOAD_FAILED_MESSAGE,
            )
        ]
        assert seen == []

    async def test_health_keeps_the_providers_outage_when_run_rejects_the_history(
        self,
    ) -> None:
        """A history that does not decode is not a provider outcome.

        After an outage the engine reports ``unavailable``; a rejected history
        must not overwrite that last provider outcome, so health stays put.
        """
        engine = await _engine_after_outage()
        conversation = _conversation(b"not json")

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert (await engine.health()).status == "unavailable"

    async def test_health_keeps_the_providers_outage_when_the_stream_rejects_the_history(
        self,
    ) -> None:
        """The streamed rejection leaves the last provider outcome untouched too."""
        engine = await _engine_after_outage()

        events = await _collect(engine, _conversation(b"not json"))

        assert isinstance(events[-1], ErrorEvent)
        assert events[-1].code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert (await engine.health()).status == "unavailable"

    async def test_run_fails_when_the_history_is_not_a_list_of_messages(self) -> None:
        engine = _engine([])
        conversation = _conversation(b'{"kind": "request"}')

        with pytest.raises(AgentRunError) as failure:
            await engine.run(_PROMPT, identity=_IDENTITY, conversation=conversation)

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED


class TestRetries:
    async def test_resends_the_same_history_when_run_retries(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen, retries=2, failures=2)

        result = await engine.run(
            _PROMPT, identity=_IDENTITY, conversation=_conversation(_history())
        )

        _assert_same_history_every_attempt(seen)
        _assert_this_turn_only(result.messages)

    async def test_resends_the_same_history_when_the_stream_retries(self) -> None:
        seen: list[list[ModelMessage]] = []
        engine = _engine(seen, retries=2, failures=2)

        events = await _collect(engine, _conversation(_history()))

        _assert_same_history_every_attempt(seen)
        _assert_this_turn_only(_final(events).messages)


def _assert_same_history_every_attempt(seen: list[list[ModelMessage]]) -> None:
    assert len(seen) == 3
    prior = ModelMessagesTypeAdapter.validate_json(_history())
    for attempt in seen:
        assert len(attempt) == len(prior) + 1
        assert _prompts(attempt) == ["how is the cluster?", _PROMPT]
        assert [message.kind for message in attempt[:-1]] == [m.kind for m in prior]
