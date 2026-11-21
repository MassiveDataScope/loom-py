"""Every entry point finds the configuration the same way when no path is given."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from loom.cli.app import app as cli
from loom.cli.check import check_config
from loom.composition import Project
from loom.core.config.errors import ConfigError
from loom.core.environment import (
    CONFIG_DIR_VARIABLE,
    Environment,
    resolve_config_dir,
    select_config_paths,
)


def _config_dir(parent: Path, name: str) -> Path:
    directory = parent / name
    directory.mkdir()
    (directory / "base.yaml").write_text(f"app:\n  name: {name}\n", encoding="utf-8")
    return directory


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.delenv(CONFIG_DIR_VARIABLE, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def local_config(workdir: Path) -> Path:
    return _config_dir(workdir, "config")


@pytest.fixture
def variable_config(workdir: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = _config_dir(workdir, "elsewhere")
    monkeypatch.setenv(CONFIG_DIR_VARIABLE, str(directory))
    return directory


class TestResolveConfigDir:
    def test_the_variable_wins_over_the_local_directory(
        self, local_config: Path, variable_config: Path
    ) -> None:
        assert resolve_config_dir() == variable_config

    def test_the_local_directory_is_used_when_the_variable_is_unset(
        self, local_config: Path
    ) -> None:
        assert resolve_config_dir() == Path("config")

    def test_a_missing_local_directory_names_both_sources(self, workdir: Path) -> None:
        with pytest.raises(ConfigError) as raised:
            resolve_config_dir()

        message = str(raised.value)
        assert "LOOM_CONFIG_DIR" in message
        assert "./config" in message

    def test_a_missing_variable_directory_names_the_value_as_the_path_tried(
        self, workdir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(CONFIG_DIR_VARIABLE, "/not/a/directory")

        with pytest.raises(ConfigError) as raised:
            resolve_config_dir()

        message = str(raised.value)
        assert "'/not/a/directory'" in message
        assert "./config" not in message


class TestSelectConfigPaths:
    def test_without_a_path_the_resolved_directory_loads(self, variable_config: Path) -> None:
        paths = select_config_paths((), None, Environment.PRODUCTION, caller="test")

        assert paths == (str(variable_config / "base.yaml"),)

    def test_an_explicit_directory_wins_over_the_variable_and_the_local_directory(
        self, local_config: Path, variable_config: Path
    ) -> None:
        explicit = _config_dir(local_config.parent, "explicit")

        paths = select_config_paths((), str(explicit), Environment.PRODUCTION, caller="test")

        assert paths == (str(explicit / "base.yaml"),)

    def test_explicit_files_win_over_the_variable_and_the_local_directory(
        self, local_config: Path, variable_config: Path
    ) -> None:
        paths = select_config_paths(("one.yaml",), None, Environment.PRODUCTION, caller="test")

        assert paths == ("one.yaml",)


class TestProjectAndCheckShareTheDefault:
    def test_load_and_check_without_a_path_resolve_the_same_directory(
        self, variable_config: Path
    ) -> None:
        loaded = Project.load()
        checked = check_config([], None, ())

        assert loaded.config_root == checked.config_root == variable_config.resolve()

    def test_the_command_without_a_path_checks_the_default_directory(
        self, variable_config: Path
    ) -> None:
        result = CliRunner().invoke(cli, ["check", "--deploy", "--skip", "app.rest"])

        assert result.exit_code == 0, result.output

    def test_the_command_names_both_sources_when_the_directory_is_missing(
        self, workdir: Path
    ) -> None:
        result = CliRunner().invoke(cli, ["check", "--deploy"])

        assert result.exit_code == 1
        assert "LOOM_CONFIG_DIR" in result.output
