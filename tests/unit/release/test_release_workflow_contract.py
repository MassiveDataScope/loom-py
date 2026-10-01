"""Contract tests for what loom-py's release delegates and what it must declare.

The mechanism lives in loom-actions; what stays here is the call, so these tests
pin the two things a reader of this repository could get wrong: the inputs the
reusable workflow needs to publish, and the fact that a merge without the label
publishes nothing.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[3]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "release.yml"
REUSABLE = "MassiveDataScope/loom-actions/.github/workflows/release-on-label.yml"


def _workflow() -> dict[str, Any]:
    return cast(dict[str, Any], yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8")))


def _triggers() -> dict[str, Any]:
    workflow = cast(dict[Any, Any], _workflow())
    return cast(dict[str, Any], workflow.get("on", workflow.get(True)))


def _release_job() -> dict[str, Any]:
    return cast(dict[str, Any], cast(dict[str, Any], _workflow()["jobs"])["release"])


class TestTheCall:
    def test_the_release_is_delegated_to_a_pinned_reusable_workflow(self) -> None:
        uses = cast(str, _release_job()["uses"])
        reference, _, revision = uses.partition("@")
        assert reference == REUSABLE
        assert len(revision) == 40, "the reusable workflow must be pinned to a commit"

    def test_building_is_asked_for_with_the_name_it_checks(self) -> None:
        inputs = cast(dict[str, Any], _release_job()["with"])
        assert inputs["build-distribution"] is True
        assert inputs["package-name"] == "loom-kernel"

    def test_the_call_grants_only_what_the_release_needs(self) -> None:
        permissions = cast(dict[str, Any], _release_job()["permissions"])
        assert permissions["contents"] == "write"
        assert "id-token" not in permissions

    def test_a_dispatch_can_resume_a_halted_release(self) -> None:
        assert "merge_sha" in _triggers()["workflow_dispatch"]["inputs"]
        assert "inputs.merge_sha" in cast(str, _release_job()["with"]["merge-sha"])


class TestThePublishStaysHere:
    """PyPI's trusted publishing rejects a token minted inside a reusable
    workflow, so the upload has to run from this file, which the publisher names."""

    def _publish_job(self) -> dict[str, Any]:
        return cast(dict[str, Any], cast(dict[str, Any], _workflow()["jobs"])["publish"])

    def test_the_upload_runs_in_this_repository_own_workflow(self) -> None:
        steps = cast(list[dict[str, Any]], self._publish_job()["steps"])
        assert any("pypi-publish" in str(step.get("uses")) for step in steps)

    def test_it_publishes_what_the_called_workflow_built(self) -> None:
        steps = cast(list[dict[str, Any]], self._publish_job()["steps"])
        download = next(s for s in steps if "download-artifact" in str(s.get("uses")))
        assert download["with"]["name"] == "distributions"

    def test_it_carries_the_oidc_identity_and_no_password(self) -> None:
        job = self._publish_job()
        assert job["permissions"]["id-token"] == "write"
        upload = next(
            s
            for s in cast(list[dict[str, Any]], job["steps"])
            if "pypi-publish" in str(s.get("uses"))
        )
        assert "password" not in str(upload.get("with", {}))

    def test_a_partial_upload_can_be_finished_by_a_rerun(self) -> None:
        """A version on PyPI cannot be replaced, so a re-run must be able to
        upload the files that are missing rather than fail on the ones that landed."""
        steps = cast(list[dict[str, Any]], self._publish_job()["steps"])
        upload = next(s for s in steps if "pypi-publish" in str(s.get("uses")))
        assert cast(dict[str, Any], upload["with"])["skip-existing"] is True

    def test_it_does_not_publish_a_failed_release(self) -> None:
        job = self._publish_job()
        assert "release" in job["needs"]
        assert "needs.release.result == 'success'" in cast(str, job["if"])

    def test_it_publishes_only_when_released_guard_revisions_are_unchanged(self) -> None:
        job = self._publish_job()
        guard = cast(dict[str, Any], cast(dict[str, Any], _workflow()["jobs"])["released-guard"])
        commands = " ".join(str(step.get("run", "")) for step in guard["steps"])
        assert "released-guard" in job["needs"]
        assert "needs.released-guard.result == 'success'" in cast(str, job["if"])
        assert "pip download --no-deps --only-binary=:all:" in commands
        assert "check_released_guard.sh wheel" in commands


class TestNothingElsePublishes:
    def test_a_push_to_master_does_not_release(self) -> None:
        assert "push" not in _triggers()

    def test_the_release_reacts_to_a_closed_pull_request(self) -> None:
        assert _triggers()["pull_request"]["types"] == ["closed"]

    def test_no_index_token_is_read_by_any_workflow(self) -> None:
        for workflow in (WORKFLOW_PATH.parent).glob("*.yml"):
            assert "PYPI_API_TOKEN" not in workflow.read_text(encoding="utf-8"), workflow.name


class TestTheVersionComesFromTheTag:
    def test_the_package_declares_no_static_version(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        assert "version" not in pyproject["project"]
        assert pyproject["project"]["dynamic"] == ["version"]
        assert pyproject["tool"]["hatch"]["version"]["source"] == "vcs"

    def test_a_prerelease_version_carries_no_local_label(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        raw = pyproject["tool"]["hatch"]["version"]["raw-options"]
        assert raw["local_scheme"] == "no-local-version"

    def test_commitizen_reads_the_version_from_the_tags(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        commitizen = pyproject["tool"]["commitizen"]
        assert commitizen["version_provider"] == "scm"
        assert "version_files" not in commitizen


class TestTheCalculatorLeavesNoStaticVersionBehind:
    """The calculator writes project.version, which hatchling refuses next to a
    dynamic one, so every later step that runs uv would fail to build."""

    def _validate_steps(self) -> list[dict[str, Any]]:
        ci_pr = cast(
            dict[str, Any],
            yaml.safe_load((WORKFLOW_PATH.parent / "ci-pr.yml").read_text(encoding="utf-8")),
        )
        jobs = cast(dict[str, Any], ci_pr["jobs"])
        return cast(list[dict[str, Any]], cast(dict[str, Any], jobs["validate"])["steps"])

    def test_the_rewrite_is_undone_before_anything_else_runs(self) -> None:
        steps = self._validate_steps()
        names = [cast(str, step.get("name", "")) for step in steps]
        calculator = next(index for index, step in enumerate(steps) if step.get("id") == "version")
        restore = next(
            index
            for index, step in enumerate(steps)
            if "git restore --source=HEAD" in cast(str, step.get("run", ""))
        )
        assert restore == calculator + 1, (
            "the restore must be the next step after the calculator, "
            f"but the steps between them are {names[calculator + 1 : restore]}"
        )

    def test_only_the_three_derived_files_may_change(self) -> None:
        restore = next(
            step
            for step in self._validate_steps()
            if "git restore --source=HEAD" in cast(str, step.get("run", ""))
        )
        script = cast(str, restore["run"])
        assert "pyproject.toml|uv.lock|CHANGELOG.md" in script
        assert "exit 1" in script


class TestThePrereleaseVersionComesFromTheTag:
    """The calculator reads project.version, which no longer exists: it answered
    0.1.1.dev194 on a repository whose last tag was v1.11.0."""

    def _validate_steps(self) -> list[dict[str, Any]]:
        ci_pr = cast(
            dict[str, Any],
            yaml.safe_load((WORKFLOW_PATH.parent / "ci-pr.yml").read_text(encoding="utf-8")),
        )
        jobs = cast(dict[str, Any], ci_pr["jobs"])
        return cast(list[dict[str, Any]], cast(dict[str, Any], jobs["validate"])["steps"])

    def test_the_version_is_read_from_the_installed_metadata(self) -> None:
        resolver = next(step for step in self._validate_steps() if step.get("id") == "prerelease")
        assert "importlib.metadata" in cast(str, resolver["run"])

    def test_no_step_reports_the_calculator_version(self) -> None:
        for step in self._validate_steps():
            rendered = str(step.get("with", {})) + cast(str, step.get("run", ""))
            assert "steps.version.outputs.version" not in rendered, step.get("name")


class TestTheFloatingTagCannotBecomeTheVersion:
    """`v1` moves onto the commit each release tags. Read as a version it yields
    "2.devN" — a release that never existed, taken from the tag that points at one
    that did. Observed on TestPyPI: 2.dev1, 2.dev3, 2.dev9, 2.dev11, 2.dev12."""

    def test_the_describe_pattern_matches_only_full_versions(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        raw = pyproject["tool"]["hatch"]["version"]["raw-options"]
        command = raw["git_describe_command"]
        assert "--match v[0-9]*.[0-9]*.[0-9]*" in command
