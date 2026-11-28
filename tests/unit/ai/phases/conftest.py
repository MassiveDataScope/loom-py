"""Shared builders for the compiler phase tests.

``loom.ai.compiler`` is imported lazily inside fixtures on purpose: a
module-level import in a conftest would abort the whole pytest session while
the compiler does not exist yet, whereas the intended red state is one
ImportError per test module.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.ai.config import AiConfig
from loom.ai.declarative import NATIVE_TOOLS, AgentSpecV1, JsonSchemaOutput
from loom.ai.errors import AgentCompilationIssue
from loom.core.stores.sql.config import SqlConfig
from loom.core.use_case.registry import UseCaseRegistry

SOURCE_PATH = "ai/agents/subject-agent/agent.yaml"
"""Artifact path handed to ``compile``; issues point at it as ``component``."""

SKILL_MANIFEST = "SKILL.md"
"""Manifest turning a directory into one skill package, as in ``pydantic-ai-harness``."""

ALL_KINDS: frozenset[str] = frozenset(
    {"usecase", "sql", "mcp", "skills", "python", "a2a", "native"}
)
"""Every kind a compiler under test accepts; the single source for the suite."""


def admits_every_native_tool(target: object) -> frozenset[str]:
    """Oracle standing in for an engine that admits every provider tool."""
    del target
    return frozenset(NATIVE_TOOLS)


ANSWER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}

_UNSET: Any = object()


@pytest.fixture
def spec_factory() -> Callable[..., AgentSpecV1]:
    """Build a valid base spec; keyword overrides inject the fault under test."""

    def _make(**overrides: Any) -> AgentSpecV1:
        base: dict[str, Any] = {
            "spec_version": 1,
            "name": "subject-agent",
            "description": "Answers questions for the phase tests.",
            "instructions": "Answer using only the prompt. Say so when unsure.",
            "output": JsonSchemaOutput(schema=ANSWER_SCHEMA),
        }
        base.update(overrides)
        return AgentSpecV1(**base)

    return _make


@pytest.fixture
def compiler_factory(
    compiler_env_config: AiConfig,
    compiler_env_registry: UseCaseRegistry,
    compiler_env_sql: SqlConfig,
    fake_myapp_path: object,
) -> Callable[..., Any]:
    """Build an ``AgentCompiler`` over the shared environment.

    ``sql`` distinguishes "not passed" (use the environment's config) from an
    explicit ``None`` (no data layer at all, for ``SQL_CONFIG_MISSING``).
    """

    def _make(
        *,
        config: AiConfig | None = None,
        registry: UseCaseRegistry | None = None,
        supported_kinds: frozenset[str] = ALL_KINDS,
        sql: SqlConfig | None = _UNSET,
    ) -> Any:
        from loom.ai.compiler import AgentCompiler

        return AgentCompiler(
            config=config if config is not None else compiler_env_config,
            registry=registry if registry is not None else compiler_env_registry,
            supported_kinds=supported_kinds,
            sql=compiler_env_sql if sql is _UNSET else sql,
        )

    return _make


@pytest.fixture
def plan_for(compiler_factory: Callable[..., Any]) -> Callable[..., Any]:
    """Compile one spec expecting success; return the plan.

    ``source_path`` is explicit because a ``./`` skill library resolves beside
    the artifact: a test that ships a library must anchor the spec to the
    directory holding it.
    """

    def _plan(
        spec: AgentSpecV1,
        *,
        source_path: str | None = SOURCE_PATH,
        **compiler_kwargs: Any,
    ) -> Any:
        return compiler_factory(**compiler_kwargs).compile(spec, source_path=source_path)

    return _plan


@pytest.fixture
def issues_for(
    compiler_factory: Callable[..., Any],
) -> Callable[..., tuple[AgentCompilationIssue, ...]]:
    """Compile one spec expecting failure; return the accumulated issues."""

    def _issues(
        spec: AgentSpecV1,
        *,
        source_path: str | None = SOURCE_PATH,
        **compiler_kwargs: Any,
    ) -> tuple[AgentCompilationIssue, ...]:
        from loom.ai.compiler import AgentCompilationError

        compiler = compiler_factory(**compiler_kwargs)
        try:
            compiler.compile(spec, source_path=source_path)
        except AgentCompilationError as exc:
            return exc.issues
        pytest.fail("expected AgentCompilationError, but the spec compiled clean")

    return _issues


@pytest.fixture
def single_issue_for(
    issues_for: Callable[..., tuple[AgentCompilationIssue, ...]],
) -> Callable[..., AgentCompilationIssue]:
    """Compile one spec expecting exactly one issue; return it."""

    def _single(spec: AgentSpecV1, **compiler_kwargs: Any) -> AgentCompilationIssue:
        issues = issues_for(spec, **compiler_kwargs)
        assert len(issues) == 1, [issue.code for issue in issues]
        return issues[0]

    return _single


@pytest.fixture
def skill_library(tmp_path: Path) -> Callable[..., Path]:
    """Write a real skill library on disk and return its directory.

    Libraries resolve on the filesystem at compile time, so the phase tests
    need genuine ``SKILL.md`` packages rather than a mocked listing.
    """

    def _make(library: str, *skills: str) -> Path:
        directory = tmp_path / library
        for skill in skills:
            package = directory / skill
            package.mkdir(parents=True, exist_ok=True)
            (package / SKILL_MANIFEST).write_text(
                f"---\nname: {skill}\ndescription: Fixture skill {skill}.\n---\n\nBody.\n",
                encoding="utf-8",
            )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    return _make


@pytest.fixture
def artifact_path(tmp_path: Path) -> str:
    """Path of the artifact a ``./`` skill library is anchored to."""
    return str(tmp_path / "agent.yaml")
