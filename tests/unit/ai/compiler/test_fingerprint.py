"""``AgentPlan.fingerprint`` names what decides an answer, and nothing else."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import msgspec
import pytest

from loom.ai.compiler import AgentPlan, CompiledInstruction
from loom.ai.declarative import PolicySpec
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from tests.helpers.pydantic_ai_engine import compiled_output, make_plan

_TARGET = InferenceTarget(
    provider="bedrock",
    model="eu.anthropic.claude-haiku-5-5",
    region="eu-west-1",
    output_mode="tool",
    options={"max_tokens": 3000},
)
_BASE = make_plan(policies=PolicySpec(retries=2, max_usd=Decimal("0.01")), inference=_TARGET)


def reject_nothing(payload: Mapping[str, Any]) -> str | None:
    return None


def reject_everything(payload: Mapping[str, Any]) -> str | None:
    return "no"


def _target(**changes: Any) -> AgentPlan:
    return msgspec.structs.replace(_BASE, inference=msgspec.structs.replace(_TARGET, **changes))


class TestStability:
    def test_it_is_a_sha256_hex_digest(self) -> None:
        assert len(_BASE.fingerprint) == 64
        int(_BASE.fingerprint, 16)

    def test_two_plans_built_alike_share_it(self) -> None:
        twin = make_plan(policies=PolicySpec(retries=2, max_usd=Decimal("0.01")), inference=_TARGET)

        assert twin.fingerprint == _BASE.fingerprint

    def test_the_digest_is_pinned_across_processes(self) -> None:
        assert (
            _BASE.fingerprint == "0685609c0c439d412e74e0bcee293b4ad7526e19029a9b98d52a3af298918b31"
        )

    def test_option_order_does_not_change_it(self) -> None:
        first = _target(options={"max_tokens": 3000, "temperature": 0})
        second = _target(options={"temperature": 0, "max_tokens": 3000})

        assert first.fingerprint == second.fingerprint


_SENSITIVE: dict[str, AgentPlan] = {
    "instruction text": msgspec.structs.replace(
        _BASE, instructions=(CompiledInstruction(text="answer the question carefully"),)
    ),
    "instruction template": msgspec.structs.replace(
        _BASE, instructions=(CompiledInstruction(text="answer the question", template="hbs"),)
    ),
    "output schema": msgspec.structs.replace(
        _BASE, output=compiled_output({"type": "object", "properties": {"a": {"type": "string"}}})
    ),
    "output check": msgspec.structs.replace(_BASE, output_check=reject_nothing),
    "spec version": msgspec.structs.replace(_BASE, spec_version=2),
    "retries": msgspec.structs.replace(_BASE, policies=PolicySpec(retries=1)),
    "max_usd": msgspec.structs.replace(
        _BASE, policies=PolicySpec(retries=2, max_usd=Decimal("0.02"))
    ),
    "provider": _target(provider="anthropic"),
    "model": _target(model="eu.anthropic.claude-sonnet-5-5"),
    "output mode": _target(output_mode="native"),
    "options": _target(options={"max_tokens": 4000}),
}


@pytest.mark.parametrize("changed", sorted(_SENSITIVE))
def test_a_change_that_can_change_the_answer_changes_it(changed: str) -> None:
    assert _SENSITIVE[changed].fingerprint != _BASE.fingerprint


def test_two_output_checks_are_told_apart_by_name() -> None:
    first = msgspec.structs.replace(_BASE, output_check=reject_nothing)
    second = msgspec.structs.replace(_BASE, output_check=reject_everything)

    assert first.fingerprint != second.fingerprint


_INSENSITIVE: dict[str, AgentPlan] = {
    "name": msgspec.structs.replace(_BASE, name="renamed"),
    "description": msgspec.structs.replace(_BASE, description="reworded"),
    "metadata": msgspec.structs.replace(_BASE, metadata={"owner": "sales"}),
    "source path": msgspec.structs.replace(_BASE, source_path="agents/x.agent.yaml"),
    "instruction block name": msgspec.structs.replace(
        _BASE, instructions=(CompiledInstruction(text="answer the question", name="guide"),)
    ),
    "region": _target(region="us-east-1"),
    "endpoint": _target(endpoint="https://gateway.invalid"),
    "credentials": _target(credentials_ref="cuimo-dev"),
    "streaming": _target(streaming=False),
    "price": msgspec.structs.replace(
        _BASE, price=ModelPrice(input=Decimal("1"), output=Decimal("2"))
    ),
}


@pytest.mark.parametrize("changed", sorted(_INSENSITIVE))
def test_a_deployment_detail_leaves_it_unchanged(changed: str) -> None:
    assert _INSENSITIVE[changed].fingerprint == _BASE.fingerprint
