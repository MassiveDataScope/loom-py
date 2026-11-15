"""``PydanticAIEngine.run_stream_shaped``: the per-run output override (T304).

Drives the real engine adapter — the model alone is a ``FunctionModel``, no
network. Pins the one property a fake engine could not: that overriding the
shape genuinely bypasses the artefact's own output check
(``decode_output``, compiled against the *declared* schema), rather than a
handle-level double merely choosing not to call it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import msgspec
import pytest
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel

from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import STRICT_SCHEMA, NullDeps, answering_model, make_plan

_IDENTITY = Identity(subject="caller")

# Would violate STRICT_SCHEMA (requires 'answer', forbids extra properties):
# proves the declared-schema check would reject it if it ran.
_OFF_SCHEMA_PAYLOAD = msgspec.json.encode({"other": "value"})

_PROSE = "a plain sentence with no declared shape at all"


def _prose_model() -> Model:
    """A model that answers free text, never a structured tool call."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content=_PROSE)])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield _PROSE

    return FunctionModel(respond, stream_function=stream)


def _engine(model: Model) -> PydanticAIEngine:
    plan = make_plan(schema=STRICT_SCHEMA)
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model)
    engine = provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())
    assert isinstance(engine, PydanticAIEngine)
    return engine


class TestAPerRunShapeSkipsTheDeclaredCheck:
    async def test_the_declared_shape_rejects_a_payload_that_does_not_fit(self) -> None:
        """Control: unshaped, the declared STRICT_SCHEMA check does reject it."""
        engine = _engine(answering_model(_OFF_SCHEMA_PAYLOAD))

        with pytest.raises(AgentRunError) as excinfo:
            await engine.run("hi", identity=_IDENTITY)

        assert excinfo.value.code is AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION

    async def test_a_per_run_shape_lets_through_prose_the_declared_schema_would_reject(
        self,
    ) -> None:
        """The override bypasses the declared check for this call only (run_text's shape).

        Free prose has no JSON shape at all, so it could never satisfy
        ``STRICT_SCHEMA`` — the strongest possible witness that
        ``run_stream_shaped(output_type=str)`` never reaches
        ``decode_output``.
        """
        engine = _engine(_prose_model())

        async with engine.run_stream_shaped("hi", identity=_IDENTITY, output_type=str) as events:
            final = [event async for event in events][-1]

        assert final.output == _PROSE  # type: ignore[union-attr]
