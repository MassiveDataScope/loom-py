"""The shared engine contract suite against the real pydantic-ai adapter.

The suite is the same function the fake runs (``test_contract_fake.py``) and
is called here **unmodified** (FR-048, SC-007): two independent
implementations, one contract. Only the model is scripted — the plan, the
provider, the engine and the decode are the production path.

The serialization check pins constitution VI at the engine boundary: one
loom-side decode of the answer, zero loom-side encodes.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import msgspec
import pytest

from loom.ai.abc import AgentEngine, FinalEvent
from loom.ai.errors import AgentRunErrorCode
from loom.ai.execution import AgentRunError
from loom.core.identity import Identity
from loom.core.model import LoomType
from loom.testing import ContractScenario, agent_engine_contract_suite
from tests.helpers.pydantic_ai_engine import (
    STRICT_SCHEMA,
    answering_model,
    build_engine,
    encode,
    failing_model,
    make_plan,
)

_IDENTITY = Identity(subject="contract-suite")
_DEFAULT_ANSWER: Mapping[str, Any] = {"answer": "contract"}


def _engine_for(scenario: ContractScenario) -> AgentEngine:
    """Build the real engine exhibiting the scenario's behaviour."""
    if scenario.error_code is not None:
        code = scenario.error_code
        return build_engine(
            make_plan(),
            failing_model(lambda: AgentRunError(code, "scripted provider failure")),
        )
    answer = scenario.expected_output
    payload = encode(answer if isinstance(answer, Mapping) else _DEFAULT_ANSWER)
    return build_engine(make_plan(), answering_model(payload))


def test_pydantic_ai_engine_satisfies_the_shared_agent_engine_contract() -> None:
    """The real adapter passes the very suite the fake passes."""
    agent_engine_contract_suite(_engine_for)


class TestOutputValidation:
    async def test_run_rejects_the_response_when_the_model_adds_an_extra_field(
        self,
    ) -> None:
        """A strict decode rejects an unknown field; it is never passed through."""
        engine = build_engine(
            make_plan(schema=STRICT_SCHEMA),
            answering_model(encode({"answer": "ok", "smuggled": "payload"})),
        )

        with pytest.raises(AgentRunError) as failure:
            await engine.run("question", identity=_IDENTITY)

        assert failure.value.code is AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION

    async def test_stream_ends_in_error_when_the_model_adds_an_extra_field(
        self,
    ) -> None:
        """The same violation terminates a stream with one coded error."""
        engine = build_engine(
            make_plan(schema=STRICT_SCHEMA),
            answering_model(encode({"answer": "ok", "smuggled": "payload"})),
        )

        events = []
        async with engine.run_stream("question", identity=_IDENTITY) as stream:
            async for event in stream:
                events.append(event)

        assert [event.code for event in events] == [  # type: ignore[union-attr]
            AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION
        ]

    async def test_run_accepts_the_response_when_it_matches_the_strict_schema(self) -> None:
        """The declared shape decodes into the validated answer."""
        engine = build_engine(
            make_plan(schema=STRICT_SCHEMA), answering_model(encode({"answer": "ok"}))
        )

        result = await engine.run("question", identity=_IDENTITY)

        assert result.output.answer == "ok"  # type: ignore[attr-defined]


class TestSerializationPasses:
    async def test_serialization_decodes_once_and_encodes_never_per_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One loom-side decode of the payload, zero loom-side encodes."""
        plan = make_plan(schema=STRICT_SCHEMA)
        decoder = _CountingDecoder(plan.output.loom_type)
        plan = msgspec.structs.replace(
            plan, output=msgspec.structs.replace(plan.output, loom_type=decoder)
        )
        engine = build_engine(plan, answering_model(encode({"answer": "ok"})))
        encodes = _count_encodes(monkeypatch)

        await engine.run("question", identity=_IDENTITY)

        assert decoder.decodes == 1, "the answer must be decoded exactly once"
        assert encodes == [], f"loom must not encode the answer: {encodes}"

    async def test_serialization_decodes_once_per_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A streamed run decodes the answer once, when it terminates."""
        plan = make_plan(schema=STRICT_SCHEMA)
        decoder = _CountingDecoder(plan.output.loom_type)
        plan = msgspec.structs.replace(
            plan, output=msgspec.structs.replace(plan.output, loom_type=decoder)
        )
        engine = build_engine(plan, answering_model(encode({"answer": "ok"})))
        encodes = _count_encodes(monkeypatch)

        async with engine.run_stream("question", identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        assert isinstance(events[-1], FinalEvent)
        assert decoder.decodes == 1, "the answer must be decoded exactly once"
        assert encodes == [], f"loom must not encode the answer: {encodes}"


class _CountingDecoder:
    """``LoomType`` proxy counting the decode passes loom makes over the payload."""

    def __init__(self, inner: LoomType) -> None:
        self._inner = inner
        self.type = inner.type
        self.library = inner.library
        self.decodes = 0

    def schema(self) -> Mapping[str, Any]:
        return self._inner.schema()

    def decode_json(self, data: bytes | str, /) -> Any:
        self.decodes += 1
        return self._inner.decode_json(data)

    def to_builtins(self, obj: Any, /) -> Any:
        return self._inner.to_builtins(obj)


def _count_encodes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every msgspec encode reachable from the engine boundary."""
    calls: list[str] = []

    def encode_hook(obj: object, **kwargs: object) -> bytes:
        calls.append(type(obj).__name__)
        return b""

    monkeypatch.setattr(msgspec.json, "encode", encode_hook)
    return calls
