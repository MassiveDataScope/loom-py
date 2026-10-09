"""``build_agent_runtime`` assembles a runtime from an ``ai:`` section and a root."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic_ai.models.test import TestModel

from loom.ai import AiConfig, InferenceTarget, build_agent_runtime
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.identity import Identity

_ARTIFACT = """\
spec_version: 1
name: {name}
description: answers in one word
instructions: answer in one word
model_role: classifier
output: {{kind: json_schema, schema: {{type: object, properties: {{word: {{type: string}}}}}}}}
"""


def _write(root: Path, name: str) -> None:
    path = root / "agents" / f"{name}.agent.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_ARTIFACT.format(name=name))


def _config(*specs: str) -> AiConfig:
    return AiConfig(
        engine="pydantic-ai",
        specs=specs,
        models={"classifier": InferenceTarget(provider="openai", model="gpt-4o")},
    )


def _provider() -> PydanticAIEngineProvider:
    return PydanticAIEngineProvider(model_resolver=lambda target: TestModel())


class TestAssembly:
    def test_every_artifact_matched_under_the_root_is_served(self, tmp_path: Path) -> None:
        _write(tmp_path, "alpha")
        _write(tmp_path, "beta")

        runtime = build_agent_runtime(
            _config("agents/*.agent.yaml"), root=tmp_path, engine_provider=_provider()
        )

        assert runtime.agent_names() == ("alpha", "beta")

    def test_names_restrict_the_runtime_to_the_agents_asked_for(self, tmp_path: Path) -> None:
        _write(tmp_path, "alpha")
        _write(tmp_path, "beta")

        runtime = build_agent_runtime(
            _config("agents/*.agent.yaml"),
            root=tmp_path,
            names={"beta"},
            engine_provider=_provider(),
        )

        assert runtime.agent_names() == ("beta",)

    def test_explicit_specs_replace_the_configured_ones(self, tmp_path: Path) -> None:
        _write(tmp_path, "alpha")

        runtime = build_agent_runtime(
            _config("nowhere/*.yaml"),
            root=tmp_path,
            specs=("agents/*.agent.yaml",),
            engine_provider=_provider(),
        )

        assert runtime.agent_names() == ("alpha",)

    def test_the_runtime_exposes_its_compiled_plans(self, tmp_path: Path) -> None:
        _write(tmp_path, "alpha")

        runtime = build_agent_runtime(
            _config("agents/*.agent.yaml"), root=tmp_path, engine_provider=_provider()
        )

        assert [plan.name for plan in runtime.plans] == ["alpha"]

    def test_a_glob_leaving_the_root_is_refused(self, tmp_path: Path) -> None:
        config = _config("../agents/*.agent.yaml")
        provider = _provider()

        with pytest.raises(AgentCompilationError) as error:
            build_agent_runtime(config, root=tmp_path, engine_provider=provider)

        assert error.value.issues[0].code == AgentErrorCode.AGENT_SPECS_ESCAPE_ROOT


class TestRun:
    async def test_the_assembled_runtime_runs_an_agent(self, tmp_path: Path) -> None:
        _write(tmp_path, "alpha")
        runtime = build_agent_runtime(
            _config("agents/*.agent.yaml"), root=tmp_path, engine_provider=_provider()
        )

        async with runtime:
            result = await runtime.run("alpha", "hello", identity=Identity(subject="etl"))

        assert result.usage.requests == 1
