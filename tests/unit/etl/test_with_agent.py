"""Tests for WithAgent: declaration, compile-time validation through the port, injection."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
import pytest

from loom.core.config import ConfigContext
from loom.etl import (
    AgentMapper,
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    FromTable,
    IntoTable,
    Sources,
    StepSQL,
    WithAgent,
)
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode
from loom.etl.executor import ETLExecutor
from loom.etl.runner import ETLRunner
from loom.etl.runtime import AgentIssue, AgentIssueKind
from loom.etl.testing import StubSourceReader, StubTargetWriter
from tests.unit.ai.etl._types import Reply as StrictReply


class RunParams(ETLParams):  # type: ignore[misc]
    run_date: date


class Reply(msgspec.Struct, frozen=True):
    answer: str


_PARAMS = RunParams(run_date=date(2026, 10, 9))
_BUDGET = Decimal("2.00")


class LabelStep(ETLStep[RunParams]):
    messages = FromTable("raw.messages")
    labeller = WithAgent("seller_reply", output=Reply, max_usd=_BUDGET)
    target = IntoTable("fact.labels").replace()

    def execute(  # type: ignore[override]
        self, params: RunParams, *, messages: Any, labeller: AgentMapper
    ) -> Any:
        return (labeller.version, labeller.map(messages, keys=["id"], prompt="text"))


@dataclass
class _FakeAgents:
    issues: tuple[AgentIssue, ...] = ()
    validated: list[tuple[str, type[Any], Decimal | None]] = field(default_factory=list)
    mapped: list[dict[str, Any]] = field(default_factory=list)

    def validate(
        self, name: str, output_type: type[Any], *, max_usd: Decimal | None
    ) -> tuple[AgentIssue, ...]:
        self.validated.append((name, output_type, max_usd))
        return self.issues

    def version(self, name: str) -> str:
        return f"{name}-v1"

    def map(
        self,
        name: str,
        frame: Any,
        *,
        keys: Sequence[str],
        prompt: object,
        output_type: type[Any],
        max_usd: Decimal | None,
    ) -> Any:
        self.mapped.append(
            {
                "name": name,
                "frame": frame,
                "keys": tuple(keys),
                "prompt": prompt,
                "output_type": output_type,
                "max_usd": max_usd,
            }
        )
        return "labelled"


def _compile_error(step: type[Any], agents: _FakeAgents | None) -> ETLCompilationError:
    compiler = ETLCompiler(config_context=ConfigContext.from_dict({}), agents=agents)
    with pytest.raises(ETLCompilationError) as exc_info:
        compiler.compile_step(step)
    return exc_info.value


class TestDeclaration:
    def test_carries_name_output_and_budget(self) -> None:
        agent = WithAgent("seller_reply", output=Reply, max_usd=_BUDGET)

        assert (agent.name, agent.output, agent.max_usd) == ("seller_reply", Reply, _BUDGET)

    def test_the_budget_is_optional(self) -> None:
        assert WithAgent("seller_reply", output=Reply).max_usd is None

    def test_repr_names_agent_and_output(self) -> None:
        assert repr(WithAgent("seller_reply", output=Reply)) == "WithAgent('seller_reply', Reply)"

    def test_rejects_an_empty_name(self) -> None:
        with pytest.raises(ValueError, match="WithAgent"):
            WithAgent("", output=Reply)

    @pytest.mark.parametrize("budget", [Decimal("0"), Decimal("-1")])
    def test_rejects_a_budget_that_is_not_positive(self, budget: Decimal) -> None:
        with pytest.raises(ValueError, match="max_usd"):
            WithAgent("seller_reply", output=Reply, max_usd=budget)

    def test_is_not_a_frame_source(self) -> None:
        assert set(LabelStep._agents) == {"labeller"}
        assert set(LabelStep._inline_sources) == {"messages"}


class TestCompileStructure:
    def test_plan_carries_the_binding(self) -> None:
        plan = ETLCompiler().compile_step(LabelStep)

        binding = plan.agent_bindings[0]
        assert (binding.alias, binding.name, binding.output_type, binding.max_usd) == (
            "labeller",
            "seller_reply",
            Reply,
            _BUDGET,
        )

    def test_missing_execute_param_is_rejected(self) -> None:
        class _Step(ETLStep[RunParams]):
            labeller = WithAgent("seller_reply", output=Reply)
            target = IntoTable("fact.labels").replace()

            def execute(self, params: RunParams) -> Any:  # type: ignore[override]
                return None

        error = _compile_error(_Step, _FakeAgents())

        assert error.code is ETLErrorCode.MISSING_CONFIG_PARAMS
        assert "WithAgent" in str(error)

    def test_alias_shared_with_a_source_is_rejected(self) -> None:
        class _Step(ETLStep[RunParams]):
            sources = Sources(labeller=FromTable("raw.messages"))
            labeller = WithAgent("seller_reply", output=Reply)
            target = IntoTable("fact.labels").replace()

            def execute(self, params: RunParams, *, labeller: Any) -> Any:  # type: ignore[override]
                return labeller

        assert _compile_error(_Step, _FakeAgents()).code is ETLErrorCode.CONFIG_ALIAS_CONFLICT

    def test_sql_step_cannot_receive_an_agent(self) -> None:
        class _Sql(StepSQL[RunParams, Any]):
            orders = FromTable("raw.orders")
            labeller = WithAgent("seller_reply", output=Reply)
            target = IntoTable("staging.sql").replace()
            sql = "SELECT * FROM orders"

        assert _compile_error(_Sql, _FakeAgents()).code is ETLErrorCode.UNSUPPORTED_CONFIG_VALUE


class TestCompileThroughThePort:
    def test_the_runner_validates_name_output_and_budget(self) -> None:
        agents = _FakeAgents()

        ETLCompiler(config_context=ConfigContext.from_dict({}), agents=agents).compile_step(
            LabelStep
        )

        assert agents.validated == [("seller_reply", Reply, _BUDGET)]

    @pytest.mark.parametrize(
        ("kind", "code"),
        [
            (AgentIssueKind.NOT_FOUND, ETLErrorCode.AGENT_NOT_FOUND),
            (AgentIssueKind.COMPILATION_FAILED, ETLErrorCode.AGENT_COMPILATION_FAILED),
            (AgentIssueKind.OUTPUT_MISMATCH, ETLErrorCode.AGENT_OUTPUT_MISMATCH),
            (AgentIssueKind.UNPRICED_BUDGET, ETLErrorCode.AGENT_UNPRICED_BUDGET),
            (AgentIssueKind.BUDGET_UNENFORCEABLE, ETLErrorCode.AGENT_BUDGET_UNENFORCEABLE),
        ],
    )
    def test_each_issue_kind_has_its_own_code(
        self, kind: AgentIssueKind, code: ETLErrorCode
    ) -> None:
        agents = _FakeAgents(issues=(AgentIssue(kind=kind, message="the reason"),))

        error = _compile_error(LabelStep, agents)

        assert error.code is code
        assert error.field == "labeller"
        assert "the reason" in str(error)

    def test_a_runner_built_from_config_without_agents_reports_not_found(self) -> None:
        error = _compile_error(LabelStep, None)

        assert error.code is ETLErrorCode.AGENT_NOT_FOUND

    def test_a_compiler_without_config_nor_runner_skips_validation(self) -> None:
        plan = ETLCompiler().compile_step(LabelStep)

        assert plan.step_type is LabelStep


class TestExecutorInjection:
    def test_the_step_receives_a_mapper_bound_to_its_declaration(self) -> None:
        agents = _FakeAgents()
        writer = StubTargetWriter()
        executor = ETLExecutor(StubSourceReader({"messages": "frame"}), writer, agents=agents)

        executor.run_step(ETLCompiler().compile_step(LabelStep), _PARAMS)

        assert writer.written[0][0] == ("seller_reply-v1", "labelled")
        assert agents.mapped == [
            {
                "name": "seller_reply",
                "frame": "frame",
                "keys": ("id",),
                "prompt": "text",
                "output_type": Reply,
                "max_usd": _BUDGET,
            }
        ]

    def test_without_a_runner_fails_clearly(self) -> None:
        executor = ETLExecutor(StubSourceReader({"messages": "frame"}), StubTargetWriter())

        with pytest.raises(RuntimeError, match=r"WithAgent.*ai:"):
            executor.run_step(ETLCompiler().compile_step(LabelStep), _PARAMS)


def test_the_etl_pillar_never_imports_the_ai_pillar() -> None:
    script = (
        "import sys\n"
        "import loom.etl, loom.etl.compiler, loom.etl.executor, loom.etl.runner\n"
        "leaked = sorted(m for m in sys.modules if m == 'loom.ai' or m.startswith('loom.ai.'))\n"
        "print(','.join(leaked))\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == ""


_AGENT_YAML = """\
spec_version: 1
name: seller_reply
description: classifies a seller reply
instructions: classify the reply
model_role: classifier
output: {kind: type_ref, ref: "tests.unit.ai.etl._types:Reply"}
"""


class StrictReplyStep(ETLStep[RunParams]):
    labeller = WithAgent("seller_reply", output=StrictReply)
    target = IntoTable("fact.agent_version").replace()

    def execute(self, params: RunParams, *, labeller: AgentMapper) -> pl.LazyFrame:  # type: ignore[override]
        return pl.LazyFrame({"agent_version": [labeller.version]})


class UnknownAgentStep(ETLStep[RunParams]):
    labeller = WithAgent("nobody", output=StrictReply)
    target = IntoTable("fact.agent_version").replace()

    def execute(self, params: RunParams, *, labeller: AgentMapper) -> pl.LazyFrame:  # type: ignore[override]
        return pl.LazyFrame({"agent_version": [labeller.version]})


class _VersionProcess(ETLProcess[RunParams]):
    steps = [StrictReplyStep]


class _VersionPipeline(ETLPipeline[RunParams]):
    processes = [_VersionProcess]


class _UnknownProcess(ETLProcess[RunParams]):
    steps = [UnknownAgentStep]


class _UnknownPipeline(ETLPipeline[RunParams]):
    processes = [_UnknownProcess]


def _pipeline_yaml(tmp_path: Path, *, with_ai: bool = True) -> str:
    conf = tmp_path / "conf"
    (conf / "agents").mkdir(parents=True)
    (conf / "agents" / "seller_reply.agent.yaml").write_text(_AGENT_YAML)
    ai = (
        "ai:\n"
        "  engine: pydantic-ai\n"
        "  specs: ['agents/*.agent.yaml']\n"
        "  models:\n"
        "    classifier: {provider: openai, model: gpt-4o}\n"
        if with_ai
        else ""
    )
    path = conf / "loom.yaml"
    path.write_text(
        "storage:\n"
        "  missing_table_policy: create\n"
        "  defaults:\n"
        "    table_path:\n"
        f"      uri: {tmp_path / 'lake'}\n" + ai,
        encoding="utf-8",
    )
    return str(path)


@pytest.mark.usefixtures("clear_builtin_resolvers")
class TestRunnerFromYaml:
    def test_specs_resolve_against_the_config_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = _pipeline_yaml(tmp_path)
        monkeypatch.chdir(tmp_path)

        ETLRunner.from_yaml(path).run(_VersionPipeline, _PARAMS)

        written = pl.scan_delta(str(tmp_path / "lake" / "fact" / "agent_version")).collect()
        assert len(written["agent_version"][0]) == 64

    def test_an_unknown_agent_fails_before_any_step_runs(self, tmp_path: Path) -> None:
        runner = ETLRunner.from_yaml(_pipeline_yaml(tmp_path))

        with pytest.raises(ETLCompilationError) as error:
            runner.run(_UnknownPipeline, _PARAMS)

        assert error.value.code is ETLErrorCode.AGENT_NOT_FOUND
        assert "'nobody'" in str(error.value)
        assert not (tmp_path / "lake").exists()

    def test_a_config_without_ai_cannot_serve_an_agent(self, tmp_path: Path) -> None:
        runner = ETLRunner.from_yaml(_pipeline_yaml(tmp_path, with_ai=False))

        with pytest.raises(ETLCompilationError) as error:
            runner.run(_VersionPipeline, _PARAMS)

        assert error.value.code is ETLErrorCode.AGENT_NOT_FOUND
        assert "'ai:'" in str(error.value)
