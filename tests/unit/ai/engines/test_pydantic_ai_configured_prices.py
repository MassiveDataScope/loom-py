"""A configured ``ai.prices`` rate prices every response before genai-prices does."""

from __future__ import annotations

import logging
from decimal import Decimal

import msgspec
import pytest
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.usage import RequestUsage

from loom.ai.abc import AgentEngine, FinalEvent
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.ai.registry import engine_prices_model
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import ScriptedUsageModel, build_engine, encode, make_plan

_IDENTITY = Identity(subject="bench-runner")
_ANSWER = {"answer": "42"}
_PRICE = ModelPrice(input=Decimal("1000"), output=Decimal("2000"), cache_read=Decimal("100"))
_USAGE = RequestUsage(input_tokens=100, output_tokens=10, cache_read_tokens=40)
_EXPECTED = Decimal("0.06") + Decimal("0.02") + Decimal("0.004")
_LIMITS_LOGGER = "loom.ai.engines.pydantic_ai._limits"


def _engine(
    *, policies: PolicySpec, usage: RequestUsage = _USAGE, price: ModelPrice | None = _PRICE
) -> AgentEngine:
    plan = msgspec.structs.replace(
        make_plan(
            policies=policies,
            inference=InferenceTarget(provider="openai", model="unknown-model"),
        ),
        price=price,
    )
    return build_engine(plan, ScriptedUsageModel(encode(_ANSWER), usage))


class TestConfiguredPriceWins:
    async def test_an_unpriced_model_is_priced_with_the_configured_rates(self) -> None:
        engine = _engine(policies=PolicySpec(retries=0))

        result = await engine.run("go", identity=_IDENTITY)

        assert result.usage.cost == _EXPECTED

    async def test_the_configured_rates_override_a_cost_the_model_reported(self) -> None:
        reported = RequestUsage(
            input_tokens=100, output_tokens=10, cache_read_tokens=40, cost=Decimal("9")
        )
        engine = _engine(policies=PolicySpec(retries=0), usage=reported)

        result = await engine.run("go", identity=_IDENTITY)

        assert result.usage.cost == _EXPECTED

    async def test_a_streamed_run_is_priced_the_same_way(self) -> None:
        engine = _engine(policies=PolicySpec(retries=0))

        async with engine.run_stream("go", identity=_IDENTITY) as stream:
            events = [event async for event in stream]

        final = events[-1]
        assert isinstance(final, FinalEvent)
        assert final.usage.cost == _EXPECTED


class TestCapsBecomeEnforceable:
    async def test_refuse_answers_once_the_model_is_priced(self) -> None:
        engine = _engine(
            policies=PolicySpec(retries=0, max_usd=Decimal("1"), on_unpriced_spend="refuse")
        )

        result = await engine.run("go", identity=_IDENTITY)

        assert result.output == _ANSWER
        assert "unpriced_requests" not in result.usage.details

    async def test_serve_enforces_the_cap_once_the_model_is_priced(self) -> None:
        engine = _engine(policies=PolicySpec(retries=0, max_usd=Decimal("0.01")))

        with pytest.raises(AgentRunError) as failure:
            await engine.run("go", identity=_IDENTITY)

        assert failure.value.code == AgentRunErrorCode.USAGE_LIMIT_EXCEEDED


class TestStartUpNotice:
    def test_no_notice_when_the_model_has_a_configured_price(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            _engine(policies=PolicySpec(max_usd=Decimal("1")))

        assert not caplog.records

    def test_the_notice_still_fires_without_a_configured_price(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LIMITS_LOGGER):
            _engine(policies=PolicySpec(max_usd=Decimal("1")), price=None)

        assert caplog.records


class TestPricingOracle:
    def test_a_model_genai_prices_knows_is_priced(self) -> None:
        provider = PydanticAIEngineProvider(model_resolver=_openai_chat)

        assert engine_prices_model(provider, InferenceTarget(provider="openai", model="gpt-4o"))

    def test_a_model_genai_prices_does_not_know_is_not_priced(self) -> None:
        provider = PydanticAIEngineProvider(model_resolver=_openai_chat)
        target = InferenceTarget(provider="openai", model="unknown-model")

        assert not engine_prices_model(provider, target)

    def test_an_engine_without_the_oracle_prices_nothing(self) -> None:
        target = InferenceTarget(provider="openai", model="gpt-4o")

        assert not engine_prices_model(object(), target)


def _openai_chat(target: InferenceTarget) -> Model:
    return OpenAIChatModel(target.model, provider=OpenAIProvider(api_key="test-key"))
