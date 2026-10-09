"""``AgentPlan.fingerprint`` names what decides an answer, and nothing else."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import msgspec
import pytest

from loom.ai.compiler import (
    AgentPlan,
    CompiledInstruction,
    CompiledMcpCapability,
    CompiledNativeCapability,
    CompiledSkillsCapability,
)
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


_CRM = CompiledMcpCapability(server="crm", url="https://crm.invalid/mcp")
_SKILLS = CompiledSkillsCapability(library="sales", directory="/srv/skills", names=("pricing",))


def _with(*capabilities: Any) -> AgentPlan:
    return msgspec.structs.replace(_BASE, capabilities=capabilities)


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
            _BASE.fingerprint == "cf42bb6b5ced6ec3ae9ef816afcd492661a744e0e417adc73c66847db68f38f5"
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
    "an mcp server": _with(_CRM),
    "a native tool": _with(CompiledNativeCapability(tool="web_search")),
    "a skill library": _with(_SKILLS),
}


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (_with(_CRM), _with(msgspec.structs.replace(_CRM, server="erp"))),
        (
            _with(CompiledNativeCapability(tool="web_search")),
            _with(CompiledNativeCapability(tool="code_execution")),
        ),
        (_with(_SKILLS), _with(msgspec.structs.replace(_SKILLS, names=("pricing", "returns")))),
        (_with(_SKILLS), _with(msgspec.structs.replace(_SKILLS, library="support"))),
    ],
    ids=["mcp server", "native tool", "skill names", "skill library"],
)
def test_capabilities_are_told_apart_by_kind_and_name(first: AgentPlan, second: AgentPlan) -> None:
    assert first.fingerprint != second.fingerprint


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


def test_where_a_capability_is_served_from_leaves_it_unchanged() -> None:
    moved = _with(msgspec.structs.replace(_CRM, url="https://crm.example/mcp", timeout_ms=5000))
    relocated = _with(msgspec.structs.replace(_SKILLS, directory="/opt/skills"))

    assert moved.fingerprint == _with(_CRM).fingerprint
    assert relocated.fingerprint == _with(_SKILLS).fingerprint


@pytest.mark.parametrize("changed", sorted(_INSENSITIVE))
def test_a_deployment_detail_leaves_it_unchanged(changed: str) -> None:
    assert _INSENSITIVE[changed].fingerprint == _BASE.fingerprint
