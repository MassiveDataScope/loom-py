"""``load_specs`` never reads an artifact outside the root its globs are anchored to."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.ai.declarative import load_specs
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_ARTIFACT = """\
spec_version: 1
name: {name}
description: contained artifact
instructions: answer
output: {{kind: json_schema, schema: {{type: object}}}}
"""


def _write(path: Path, name: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_ARTIFACT.format(name=name))
    return path


def _codes(error: pytest.ExceptionInfo[AgentCompilationError]) -> list[AgentErrorCode]:
    return [issue.code for issue in error.value.issues]


class TestContainedGlobs:
    def test_a_glob_inside_the_root_loads(self, tmp_path: Path) -> None:
        _write(tmp_path / "root" / "agents" / "a.agent.yaml", "inside")

        decoded = load_specs(["agents/*.agent.yaml"], root=tmp_path / "root")

        assert [item.spec.name for item in decoded] == ["inside"]


class TestEscapingGlobs:
    def test_a_parent_segment_is_refused(self, tmp_path: Path) -> None:
        _write(tmp_path / "outside" / "a.agent.yaml", "outside")
        (tmp_path / "root").mkdir()

        with pytest.raises(AgentCompilationError) as error:
            load_specs(["../outside/*.agent.yaml"], root=tmp_path / "root")

        assert _codes(error) == [AgentErrorCode.AGENT_SPECS_ESCAPE_ROOT]

    def test_an_absolute_pattern_is_refused(self, tmp_path: Path) -> None:
        artifact = _write(tmp_path / "outside" / "a.agent.yaml", "outside")
        (tmp_path / "root").mkdir()

        with pytest.raises(AgentCompilationError) as error:
            load_specs([str(artifact)], root=tmp_path / "root")

        assert _codes(error) == [AgentErrorCode.AGENT_SPECS_ESCAPE_ROOT]

    def test_a_symlink_leaving_the_root_is_refused(self, tmp_path: Path) -> None:
        _write(tmp_path / "outside" / "a.agent.yaml", "outside")
        (tmp_path / "root").mkdir()
        (tmp_path / "root" / "agents").symlink_to(tmp_path / "outside")

        with pytest.raises(AgentCompilationError) as error:
            load_specs(["agents/*.agent.yaml"], root=tmp_path / "root")

        assert _codes(error) == [AgentErrorCode.AGENT_SPECS_ESCAPE_ROOT]

    def test_the_issue_names_the_pattern_not_the_resolved_path(self, tmp_path: Path) -> None:
        (tmp_path / "root").mkdir()

        with pytest.raises(AgentCompilationError) as error:
            load_specs(["../secrets/*.yaml"], root=tmp_path / "root")

        assert "../secrets/*.yaml" in str(error.value)
