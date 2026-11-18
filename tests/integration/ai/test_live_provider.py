"""One real run against a real provider (US2 · US3), opt-in and self-skipping.

Marked ``live`` and skipped whenever the environment carries no usable
credentials, so the default suite spends no token and needs no vendor account.
Run it deliberately::

    export AWS_PROFILE=…            # or OPENAI_API_KEY, or ANTHROPIC_API_KEY
    uv run pytest tests/integration/ai/test_live_provider.py -v -m live

What it proves is exactly what a scripted model cannot: a real provider
honours the declared output shape, the answer decodes through the plan's
strict decoder, and ``usage`` comes back non-zero.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from typing import Any

import pytest

from loom.ai.abc import AgentResult
from loom.ai.compiler._plan import AgentPlan, CompiledInstruction
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import NullDeps, compiled_output

pytestmark = [pytest.mark.live, pytest.mark.enable_socket]


@pytest.fixture(autouse=True)
def _bare_environment() -> None:
    """Keep the environment the test was given: it is the one that asks for a real provider."""


_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}

_PROMPT = "Reply with the single word: pong."


def _live_target() -> InferenceTarget | None:
    """The first provider the environment actually has credentials for."""
    if os.environ.get("OPENAI_API_KEY"):
        return InferenceTarget(
            provider="openai", model=os.environ.get("LOOM_LIVE_MODEL", "gpt-5.2")
        )
    if os.environ.get("ANTHROPIC_API_KEY"):
        return InferenceTarget(
            provider="anthropic", model=os.environ.get("LOOM_LIVE_MODEL", "claude-sonnet-4-5")
        )
    if os.environ.get("AWS_PROFILE") or os.environ.get("AWS_ACCESS_KEY_ID"):
        return InferenceTarget(
            provider="bedrock",
            model=os.environ.get("LOOM_LIVE_MODEL", "anthropic.claude-sonnet-4-5-20250929-v1:0"),
            region=os.environ.get("AWS_REGION", "eu-west-1"),
        )
    return None


def _plan(target: InferenceTarget) -> AgentPlan:
    return AgentPlan(
        name="live",
        description="live smoke agent",
        instructions=(
            CompiledInstruction(text="Answer with the requested word and nothing else."),
        ),
        spec_version=1,
        inference=target,
        output=compiled_output(_SCHEMA),
        policies=PolicySpec(retries=1),
        metadata={},
    )


@pytest.fixture
def live_result() -> AgentResult:
    """Run the agent once against whichever provider is configured."""
    target = _live_target()
    if target is None:
        pytest.skip("no provider credentials in the environment")
    provider = PydanticAIEngineProvider()
    engine = provider.create_engine(_plan(target), deps=NullDeps(), container=LoomContainer())
    return asyncio.run(engine.run(_PROMPT, identity=Identity(subject="live-suite")))


class TestLiveProvider:
    def test_the_answer_matches_the_declared_shape_when_credentials_are_present(
        self, live_result: AgentResult
    ) -> None:
        """The provider's answer decodes through the plan's strict decoder."""
        assert isinstance(live_result.output.answer, str)  # type: ignore[attr-defined]

    def test_the_usage_is_not_zero_when_credentials_are_present(
        self, live_result: AgentResult
    ) -> None:
        """A real run accounts for real tokens."""
        assert live_result.usage.requests >= 1
        assert live_result.usage.input_tokens > 0
