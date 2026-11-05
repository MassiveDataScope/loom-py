"""Contract tests for the dependency audit that gates every pull request.

The audit only protects anything while it blocks the merge, runs with a read-only
token, installs itself by hash, and every advisory it ignores is written down
with a reason in SECURITY.md.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "ci-pr.yml"
REQUIREMENTS = ROOT / ".github" / "requirements"
SECURITY_PATH = ROOT / "SECURITY.md"
AUDIT = "dependency-audit"


def _workflow() -> dict[str, Any]:
    return cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))


def _job(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], _workflow()["jobs"][name])


def _audit_script() -> str:
    return "\n".join(cast(str, step.get("run", "")) for step in _job(AUDIT)["steps"])


def _ignored_in_workflow() -> set[str]:
    return set(re.findall(r"--ignore-vuln\s+(\S+)", _audit_script()))


def _accepted_in_security_policy() -> set[str]:
    text = SECURITY_PATH.read_text(encoding="utf-8")
    section = text.split("## Accepted advisories", 1)[1].split("\n## ", 1)[0]
    return set(re.findall(r"^\|\s*(GHSA-[\w-]+|CVE-[\d-]+|PYSEC-[\d-]+)\s*\|", section, re.M))


class TestTheGate:
    def test_a_failed_audit_blocks_the_merge(self) -> None:
        validate = _job("validate")
        assert AUDIT in validate["needs"]
        assert f"needs.{AUDIT}.result" in validate["if"]

    def test_the_audit_runs_with_a_read_only_token(self) -> None:
        assert _job(AUDIT)["permissions"] == {"contents": "read"}

    def test_the_audit_fails_on_any_finding_over_the_whole_runtime_closure(self) -> None:
        script = _audit_script()
        assert "--all-extras" in script
        assert "--strict" in script
        assert "--require-hashes" in script


class TestTheAuditorInstall:
    def test_pip_audit_is_installed_only_by_hash(self) -> None:
        steps = cast(list[dict[str, Any]], _job(AUDIT)["steps"])
        install = next(step["run"] for step in steps if step.get("name") == "Install pip-audit")
        assert "--require-hashes -r .github/requirements/pip-audit.txt" in install

    def test_the_hashed_file_matches_its_pin_and_the_primary_python(self) -> None:
        pin = (REQUIREMENTS / "pip-audit.in").read_text(encoding="utf-8").strip()
        compiled = (REQUIREMENTS / "pip-audit.txt").read_text(encoding="utf-8")
        assert re.search(rf"^{re.escape(pin)} \\$", compiled, re.M)
        assert f"--python-version {_workflow()['env']['PRIMARY_PYTHON']} " in compiled


class TestAcceptedAdvisories:
    def test_every_ignored_advisory_is_documented_and_every_documented_one_is_ignored(self) -> None:
        assert _ignored_in_workflow() == _accepted_in_security_policy()
