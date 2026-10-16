"""``AgentPlan.fingerprint`` names what decides an answer, and nothing else."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import msgspec
import pytest

from loom.ai.compiler import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledInstruction,
    CompiledMcpCapability,
    CompiledNativeCapability,
    CompiledPythonCapability,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.declarative import PolicySpec
from loom.ai.inference import InferenceTarget
from loom.ai.pricing import ModelPrice
from loom.core.sql.config import SqlConnectionConfig
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


def build_toolset() -> object:
    return object()


_CRM = CompiledMcpCapability(server="crm", url="https://crm.invalid/mcp")
_SKILLS = CompiledSkillsCapability(library="sales", directory="/srv/skills", names=("pricing",))
_USECASES = CompiledUsecaseCapability(keys=("orders.list",), use_cases=())
_SQL = CompiledSqlCapability(
    connection="reports",
    config=SqlConnectionConfig(
        backend="clickhouse", url="clickhouse://reports.invalid:8123/reporting", allowed_roles=()
    ),
    max_rows=1000,
    max_result_bytes=1_000_000,
)
_PYTHON = CompiledPythonCapability(factory_ref="app.tools:build_toolset", factory=build_toolset)
_PEER = CompiledA2ACapability(agent="pricing", url="https://pricing.invalid")


def _with(*capabilities: Any) -> AgentPlan:
    return msgspec.structs.replace(_BASE, capabilities=capabilities)


def _policies(**changes: Any) -> AgentPlan:
    return msgspec.structs.replace(
        _BASE, policies=msgspec.structs.replace(_BASE.policies, **changes)
    )


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
            _BASE.fingerprint == "dd47e083e4eb352c9b90d261e5af7408d3b4c163cef71bdda16eef917029ac90"
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
        (_with(_USECASES), _with(msgspec.structs.replace(_USECASES, keys=("orders.cancel",)))),
        (_with(_SQL), _with(msgspec.structs.replace(_SQL, connection="billing"))),
        (_with(_PYTHON), _with(msgspec.structs.replace(_PYTHON, factory_ref="app.tools:other"))),
        (_with(_PEER), _with(msgspec.structs.replace(_PEER, agent="billing"))),
    ],
    ids=[
        "mcp server",
        "native tool",
        "skill names",
        "skill library",
        "use case keys",
        "sql connection",
        "python factory",
        "a2a agent",
    ],
)
def test_capabilities_are_told_apart_by_their_declared_facts(
    first: AgentPlan, second: AgentPlan
) -> None:
    assert first.fingerprint != second.fingerprint


@pytest.mark.parametrize(
    "filtered",
    [
        msgspec.structs.replace(_CRM, include=("search_*",)),
        msgspec.structs.replace(_CRM, exclude=("delete_*",)),
        msgspec.structs.replace(_PEER, include=("quote",)),
        msgspec.structs.replace(_PEER, exclude=("refund",)),
    ],
    ids=["mcp include", "mcp exclude", "a2a include", "a2a exclude"],
)
def test_a_tool_filter_changes_it(filtered: CompiledMcpCapability | CompiledA2ACapability) -> None:
    unfiltered = msgspec.structs.replace(filtered, include=(), exclude=())

    assert _with(filtered).fingerprint != _with(unfiltered).fingerprint


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
    "retries": _policies(retries=1),
    "max_usd": _policies(max_usd=Decimal("0.02")),
    "on_unpriced_spend": _policies(on_unpriced_spend="refuse"),
    "max_requests": _policies(max_requests=7),
    "max_total_tokens": _policies(max_total_tokens=5000),
    "max_input_tokens_per_request": _policies(max_input_tokens_per_request=2000),
    "tool_timeout_ms": _policies(tool_timeout_ms=1000),
    "run_timeout_ms": _policies(run_timeout_ms=9000),
    "max_iterations": _policies(max_iterations=3),
    "max_tool_calls": _policies(max_tool_calls=3),
    "max_history_bytes": _policies(max_history_bytes=1024),
}


def test_where_a_capability_is_served_from_leaves_it_unchanged() -> None:
    moved = _with(msgspec.structs.replace(_CRM, url="https://crm.example/mcp", timeout_ms=5000))
    relocated = _with(msgspec.structs.replace(_SKILLS, directory="/opt/skills"))

    assert moved.fingerprint == _with(_CRM).fingerprint
    assert relocated.fingerprint == _with(_SKILLS).fingerprint


@pytest.mark.parametrize("changed", sorted(_INSENSITIVE))
def test_a_deployment_detail_or_a_policy_leaves_it_unchanged(changed: str) -> None:
    assert _INSENSITIVE[changed].fingerprint == _BASE.fingerprint
