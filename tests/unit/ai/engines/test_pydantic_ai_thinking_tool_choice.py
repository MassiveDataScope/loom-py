"""A ``tool`` output with thinking configured leaves the model free to reason.

Loom sends no ``tool_choice`` of its own: the request's tool choice is the
engine's decision. These tests pin what reaches Bedrock for an Anthropic model
with adaptive thinking, so a loom change that forced the output tool, which
makes the model answer without thinking, fails here. pydantic-ai itself only
stopped forcing it in 2.52, so the tests skip on older releases.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from importlib.metadata import version
from typing import Any

import msgspec
import pytest
from packaging.version import Version
from pydantic_ai.models import Model
from pydantic_ai.models.bedrock import BedrockConverseModel
from pydantic_ai.providers.bedrock import BedrockProvider

from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.ai.engines.pydantic_ai.providers._shared import model_settings
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import NullDeps, make_plan

pytestmark = pytest.mark.skipif(
    Version(version("pydantic-ai-slim")) < Version("2.52"),
    reason="pydantic-ai before 2.52 forces the output tool under adaptive thinking",
)

_TARGET = InferenceTarget(
    provider="bedrock",
    model="us.anthropic.claude-sonnet-4-6",
    region="us-east-1",
    output_mode="tool",
    streaming=False,
    options={"thinking": True},
)
_CONVERSE_RESPONSE: dict[str, Any] = {
    "output": {
        "message": {
            "role": "assistant",
            "content": [
                {"toolUse": {"toolUseId": "t1", "name": "final_result", "input": {"answer": "42"}}}
            ],
        }
    },
    "stopReason": "tool_use",
    "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
    "metrics": {"latencyMs": 1},
}


@dataclass(frozen=True)
class _HttpOk:
    status_code: int = 200


class _RecordedBedrock:
    """A Bedrock runtime client answering ``Converse`` offline and recording each request."""

    def __init__(self) -> None:
        self.provider = BedrockProvider(
            region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
        )
        self.requests: list[dict[str, Any]] = []
        events = self.provider.client.meta.events
        events.register("provide-client-params.bedrock-runtime.Converse", self._record)
        events.register("before-call.bedrock-runtime.Converse", self._answer)

    def _record(self, params: dict[str, Any], **_: Any) -> None:
        self.requests.append(params)

    @staticmethod
    def _answer(**_: Any) -> tuple[_HttpOk, dict[str, Any]]:
        return _HttpOk(), _CONVERSE_RESPONSE

    def model(self, target: InferenceTarget) -> Model:
        return BedrockConverseModel(
            target.model, provider=self.provider, settings=model_settings(target)
        )


@pytest.mark.parametrize("price", [None, ModelPrice(input=Decimal("3"), output=Decimal("15"))])
async def test_adaptive_thinking_keeps_the_tool_choice_automatic(
    price: ModelPrice | None,
) -> None:
    bedrock = _RecordedBedrock()
    plan = msgspec.structs.replace(make_plan(inference=_TARGET), price=price)
    engine = PydanticAIEngineProvider(model_resolver=bedrock.model).create_engine(
        plan, deps=NullDeps(), container=LoomContainer()
    )

    result = await engine.run("go", identity=Identity(subject="bench-runner"))

    assert result.output == {"answer": "42"}
    request = bedrock.requests[0]
    assert request["toolConfig"]["toolChoice"] == {"auto": {}}
    assert request["additionalModelRequestFields"]["thinking"] == {"type": "adaptive"}
