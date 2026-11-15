from __future__ import annotations

import shlex
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

WORKFLOW_DIRECTORY = Path(__file__).parents[3] / ".github" / "workflows"
PINNED_UV = "uv==0.10.2"
_OPTIONS_TAKING_A_VALUE = frozenset(
    {
        "--only-binary",
        "--no-binary",
        "--requirement",
        "-r",
        "--index-url",
        "--extra-index-url",
        # uv's interpreter selector. Without it the tokenizer reads the
        # environment path as a package specifier, sees no '==' and reports a
        # pinned install as unpinned — a false alarm that would push someone
        # to weaken the contract instead of teaching the parser one flag.
        "--python",
        "-p",
    }
)


def _workflow_paths() -> list[Path]:
    return sorted(WORKFLOW_DIRECTORY.glob("*.yml"))


def _logical_lines(script: str) -> Iterator[str]:
    continued = ""
    for line in script.splitlines():
        command = line.strip()
        if command.startswith("#"):
            continue
        if command.endswith("\\"):
            continued += f"{command[:-1]} "
            continue
        yield continued + command
        continued = ""
    if continued:
        yield continued


def _run_commands(workflow_path: Path) -> Iterator[str]:
    workflow = cast(dict[str, Any], yaml.safe_load(workflow_path.read_text(encoding="utf-8")))
    for job in cast(dict[str, Any], workflow["jobs"]).values():
        steps = cast(dict[str, Any], job).get("steps")
        if steps is None:
            continue
        for step in cast(list[dict[str, Any]], steps):
            script = cast(str, cast(dict[str, Any], step).get("run", ""))
            if script:
                yield from _logical_lines(script)


def _tokens(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:
        # Shell fragment (heredoc body, conditional, ...) rather than a command.
        return []


def _installed_specifiers(arguments: Sequence[str]) -> list[str]:
    if "install" not in arguments:
        return []
    specifiers: list[str] = []
    skip_value = False
    for argument in arguments[arguments.index("install") + 1 :]:
        if skip_value:
            skip_value = False
        elif argument in _OPTIONS_TAKING_A_VALUE:
            skip_value = True
        elif not argument.startswith("-"):
            specifiers.append(argument)
    return specifiers


def _pip_install_is_locked_and_verified(command: str) -> bool:
    arguments = _tokens(command)
    if "--require-hashes" in arguments:
        return "-r" in arguments or "--requirement" in arguments
    specifiers = _installed_specifiers(arguments)
    return (
        bool(specifiers)
        and "--only-binary" in arguments
        and all("==" in specifier for specifier in specifiers)
    )


class TestSupplyChainContract:
    @pytest.mark.parametrize("workflow_path", _workflow_paths(), ids=lambda path: path.name)
    def test_pip_installs_are_pinned_and_installed_without_running_setup_scripts(
        self,
        workflow_path: Path,
    ) -> None:
        unverified = [
            command
            for command in _run_commands(workflow_path)
            if "pip install" in command and not _pip_install_is_locked_and_verified(command)
        ]

        assert unverified == []

    @pytest.mark.parametrize("workflow_path", _workflow_paths(), ids=lambda path: path.name)
    def test_uv_run_installs_only_pinned_extra_dependencies(self, workflow_path: Path) -> None:
        unpinned = [
            value
            for command in _run_commands(workflow_path)
            for flag, value in zip(_tokens(command), _tokens(command)[1:], strict=False)
            if flag == "--with" and "==" not in value
        ]

        assert unpinned == []

    def test_every_workflow_installs_the_same_pinned_uv(self) -> None:
        installed_uv = {
            path.name: {
                specifier
                for command in _run_commands(path)
                for specifier in _installed_specifiers(_tokens(command))
                if specifier.startswith("uv==")
            }
            for path in _workflow_paths()
        }
        workflows_installing_uv = {
            name: specifiers for name, specifiers in installed_uv.items() if specifiers
        }

        assert workflows_installing_uv
        assert all(specifiers == {PINNED_UV} for specifiers in workflows_installing_uv.values())

    def test_a_job_that_delegates_pins_what_it_calls(self) -> None:
        """A called workflow runs commands this repository cannot audit, so it is pinned."""
        delegating = {
            (path.name, name): cast(str, job["uses"])
            for path in _workflow_paths()
            for name, job in cast(
                dict[str, Any],
                cast(dict[str, Any], yaml.safe_load(path.read_text(encoding="utf-8")))["jobs"],
            ).items()
            if "uses" in cast(dict[str, Any], job)
        }

        assert delegating, "no job delegates; drop this test with the last caller"
        for (workflow, job), uses in delegating.items():
            _, separator, revision = uses.partition("@")
            assert separator, f"{workflow}:{job} calls {uses} without a revision"
            assert len(revision) == 40, f"{workflow}:{job} pins {revision}, not a commit"
