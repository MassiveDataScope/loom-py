"""Standalone validation entry point (T069, FR-015).

``python -m loom.ai.validate <globs>`` must exit 0 on the valid corpus and,
for a broken artifact, exit non-zero printing one coded issue per line.

The subprocess environment prepends the fake ``myapp`` package to
``PYTHONPATH`` so the corpus ``module:symbol`` references resolve, exactly as
they would inside a real application.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_AI_TESTS_DIR = Path(__file__).parent.parent
CORPUS_GLOB = str(_AI_TESTS_DIR / "fixtures" / "corpus_v1" / "*" / "agent.yaml")
FAKE_PKGS_DIR = _AI_TESTS_DIR / "fixtures" / "fake_pkgs"

BROKEN_ARTIFACT = """\
spec_version: 1
name: broken-agent
description: Broken on purpose for the standalone validator.
instructions: Answer using only the prompt.
output:
  kind: json_schema
  schema:
    type: 42
policies:
  retries: 99
"""

UNKNOWN_NATIVE_TOOL_ARTIFACT = """\
spec_version: 1
name: unknown-native-tool-agent
description: Declares a provider tool that is not in the v1 vocabulary.
instructions: Answer using only the prompt.
output:
  kind: json_schema
  schema:
    type: object
capabilities:
  - kind: native
    tool: telepathy
"""


def _run_validate(*globs: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(FAKE_PKGS_DIR), env.get("PYTHONPATH", "")]).rstrip(
        os.pathsep
    )
    return subprocess.run(
        [sys.executable, "-m", "loom.ai.validate", *globs],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=120,
    )


class TestValidateCli:
    def test_exits_zero_and_silent_on_stderr_when_corpus_is_valid(self) -> None:
        result = _run_validate(CORPUS_GLOB)
        assert (result.returncode, result.stderr.strip()) == (0, ""), (
            result.returncode,
            result.stdout,
            result.stderr,
        )

    def test_exits_nonzero_with_one_coded_line_per_issue_when_artifact_is_broken(
        self,
        tmp_path: Path,
    ) -> None:
        artifact = tmp_path / "broken.agent.yaml"
        artifact.write_text(BROKEN_ARTIFACT)
        result = _run_validate(str(artifact))
        assert result.returncode != 0
        lines = [line for line in (result.stdout + result.stderr).splitlines() if line.strip()]
        for code in ("OUTPUT_SCHEMA_INVALID", "POLICY_OUT_OF_RANGE"):
            matching = [line for line in lines if code in line]
            assert len(matching) == 1, (code, lines)

    def test_exits_one_with_spec_malformed_when_native_tool_is_unknown(
        self, tmp_path: Path
    ) -> None:
        """An unknown provider tool is a format error the validator catches offline (030/AC-3)."""
        artifact = tmp_path / "unknown-native-tool.agent.yaml"
        artifact.write_text(UNKNOWN_NATIVE_TOOL_ARTIFACT)
        result = _run_validate(str(artifact))
        assert result.returncode == 1
        lines = [line for line in result.stderr.splitlines() if line.strip()]
        assert [line for line in lines if "SPEC_MALFORMED" in line] == lines
        assert len(lines) == 1

    def test_a_pattern_that_matches_nothing_fails(self, tmp_path: Path) -> None:
        """Counting only issues makes an empty match look like a clean corpus.

        That is not hypothetical: this project's own CI guarded the artifact
        corpus with '*.yaml' after the corpus moved to one directory per agent,
        so the step matched nothing, exited 0 and asserted nothing for as long as
        it stood.
        """
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "loom.ai.validate", str(tmp_path / "*.agent.yaml")],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 2
        assert "no artifact matched" in result.stderr
