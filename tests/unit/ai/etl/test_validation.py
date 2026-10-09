"""PolarsAgentRunner.validate: the four reasons an agent cannot serve a step."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from loom.ai import AiConfig, InferenceTarget, ModelPrice
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.etl import PolarsAgentRunner
from loom.etl.runtime import AgentIssueKind
from tests.unit.ai.etl._types import Other, Reply

_TYPED = """\
spec_version: 1
name: {name}
description: classifies a seller reply
instructions: classify the reply
model_role: {role}
output: {{kind: type_ref, ref: "tests.unit.ai.etl._types:Reply"}}
policies: {{retries: 1{cap}}}
"""
_UNTYPED = """\
spec_version: 1
name: untyped
description: classifies a seller reply
instructions: classify the reply
model_role: classifier
output: {kind: json_schema, schema: {type: object, properties: {answer: {type: string}}}}
"""


def _openai_chat(target: InferenceTarget) -> Model:
    return OpenAIChatModel(target.model, provider=OpenAIProvider(api_key="test-key"))


def _runner(
    tmp_path: Path,
    *,
    model: str = "unknown-model",
    prices: dict[str, ModelPrice] | None = None,
    role: str = "classifier",
    cap: str = "",
) -> PolarsAgentRunner:
    agents = tmp_path / "agents"
    agents.mkdir(exist_ok=True)
    (agents / "typed.agent.yaml").write_text(_TYPED.format(name="typed", role=role, cap=cap))
    (agents / "untyped.agent.yaml").write_text(_UNTYPED)
    config = AiConfig(
        engine="pydantic-ai",
        specs=("agents/*.agent.yaml",),
        models={"classifier": InferenceTarget(provider="openai", model=model)},
        prices=prices or {},
    )
    return PolarsAgentRunner(
        config,
        root=tmp_path,
        engine_provider=lambda: PydanticAIEngineProvider(model_resolver=_openai_chat),
    )


def _kinds(runner: PolarsAgentRunner, name: str, **kw: Decimal | type) -> list[AgentIssueKind]:
    output = kw.pop("output", Reply)
    budget = kw.pop("max_usd", None)
    assert isinstance(output, type)
    assert budget is None or isinstance(budget, Decimal)
    return [issue.kind for issue in runner.validate(name, output, max_usd=budget)]


class TestServable:
    def test_a_typed_agent_without_budget_can_serve(self, tmp_path: Path) -> None:
        assert _kinds(_runner(tmp_path), "typed") == []


class TestNotFound:
    def test_an_agent_no_artifact_declares_is_not_found(self, tmp_path: Path) -> None:
        issues = _runner(tmp_path).validate("nobody", Reply, max_usd=None)

        assert [issue.kind for issue in issues] == [AgentIssueKind.NOT_FOUND]
        assert "'nobody'" in issues[0].message


class TestCompilationFailed:
    def test_the_compiler_issues_are_carried_over(self, tmp_path: Path) -> None:
        issues = _runner(tmp_path, role="unbound").validate("typed", Reply, max_usd=None)

        assert [issue.kind for issue in issues] == [AgentIssueKind.COMPILATION_FAILED]
        assert "MODEL_ROLE_UNBOUND" in issues[0].message


class TestOutputMismatch:
    def test_another_type_than_the_artifact_declares_is_refused(self, tmp_path: Path) -> None:
        issues = _runner(tmp_path).validate("typed", Other, max_usd=None)

        assert [issue.kind for issue in issues] == [AgentIssueKind.OUTPUT_MISMATCH]
        assert "Other" in issues[0].message

    def test_a_json_schema_output_cannot_serve_a_typed_step(self, tmp_path: Path) -> None:
        assert _kinds(_runner(tmp_path), "untyped") == [AgentIssueKind.OUTPUT_MISMATCH]


class TestUnpricedBudget:
    @pytest.mark.parametrize(
        ("cap", "budget"),
        [("", Decimal("2")), (", max_usd: 0.01", None)],
        ids=["step budget", "run cap"],
    )
    def test_a_budget_on_an_unpriced_model_is_refused(
        self, tmp_path: Path, cap: str, budget: Decimal | None
    ) -> None:
        issues = _runner(tmp_path, cap=cap).validate("typed", Reply, max_usd=budget)

        assert [issue.kind for issue in issues] == [AgentIssueKind.UNPRICED_BUDGET]
        assert "unknown-model" in issues[0].message

    def test_a_configured_price_makes_the_budget_enforceable(self, tmp_path: Path) -> None:
        prices = {"unknown-model": ModelPrice(input=Decimal("1"), output=Decimal("1"))}
        runner = _runner(tmp_path, prices=prices)

        assert _kinds(runner, "typed", max_usd=Decimal("2")) == []

    def test_a_model_the_engine_prices_needs_no_configured_price(self, tmp_path: Path) -> None:
        runner = _runner(tmp_path, model="gpt-4o")

        assert _kinds(runner, "typed", max_usd=Decimal("2")) == []
