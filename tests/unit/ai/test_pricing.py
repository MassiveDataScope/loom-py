"""Configured model prices (``ai.prices``): decoding, the cost formula and the plan."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import msgspec
import pytest

from loom.ai.compiler import AgentCompiler
from loom.ai.config import AiConfig
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.core.use_case.registry import UseCaseRegistry

_HAIKU = "eu.anthropic.claude-haiku-5-5"
_PRICE = ModelPrice(
    input=Decimal("0.11"),
    output=Decimal("0.55"),
    cache_read=Decimal("0.011"),
    cache_write=Decimal("0.1375"),
)


def _config(prices: dict[str, ModelPrice]) -> AiConfig:
    return AiConfig(
        engine="pydantic-ai",
        models={"classifier": InferenceTarget(provider="openai", model=_HAIKU)},
        prices=prices,
    )


def _spec() -> AgentSpecV1:
    return AgentSpecV1(
        spec_version=1,
        name="seller_reply",
        description="classifies a reply",
        instructions="classify",
        model_role="classifier",
        output=JsonSchemaOutput(schema={"type": "object"}),
    )


def _compile(config: AiConfig) -> ModelPrice | None:
    compiler = AgentCompiler(
        config=config, registry=UseCaseRegistry({}, {}), supported_kinds=frozenset()
    )
    return compiler.compile(_spec()).price


class TestCost:
    def test_uncached_input_and_output_are_billed_per_million(self) -> None:
        cost = _PRICE.cost(input_tokens=1_000_000, output_tokens=1_000_000)

        assert cost == Decimal("0.66")

    def test_cached_tokens_leave_the_input_rate_for_their_own(self) -> None:
        cost = _PRICE.cost(
            input_tokens=1_000_000,
            output_tokens=0,
            cache_read_tokens=500_000,
            cache_write_tokens=200_000,
        )

        assert cost == Decimal("0.033") + Decimal("0.0055") + Decimal("0.0275")

    def test_cache_without_its_own_rate_is_billed_as_input(self) -> None:
        price = ModelPrice(input=Decimal("1"), output=Decimal("2"))

        cost = price.cost(
            input_tokens=1_000_000,
            output_tokens=0,
            cache_read_tokens=400_000,
            cache_write_tokens=100_000,
        )

        assert cost == Decimal("1")


class TestConfigDecoding:
    def test_prices_decode_from_plain_config_values(self) -> None:
        raw = {
            "engine": "pydantic-ai",
            "models": {"classifier": {"provider": "openai", "model": _HAIKU}},
            "prices": {
                _HAIKU: {
                    "input": 0.11,
                    "output": 0.55,
                    "cache_read": 0.011,
                    "source": "AWS Pricing API eu-west-1",
                    "as_of": "2026-10-09",
                }
            },
        }

        config = msgspec.convert(raw, AiConfig, strict=False)

        price = config.prices[_HAIKU]
        assert price.input == Decimal("0.11")
        assert price.cache_write is None
        assert price.as_of == date(2026, 10, 9)

    def test_prices_default_to_none_configured(self) -> None:
        assert _config({}).prices == {}

    def test_a_negative_rate_is_refused(self) -> None:
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert({"input": -1, "output": 1}, ModelPrice)


class TestPlanCarriesThePrice:
    def test_the_plan_carries_the_price_of_its_bound_model(self) -> None:
        assert _compile(_config({_HAIKU: _PRICE})) == _PRICE

    def test_the_plan_carries_no_price_when_its_model_has_none(self) -> None:
        assert _compile(_config({"another-model": _PRICE})) is None
