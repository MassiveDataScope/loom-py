"""One environment selector, resolved once at startup.

``LOOM_ENV`` or an explicit argument picks the environment; with neither the
environment is production, so a container started without the variable runs
with the secure defaults. An unknown value is a startup error, never a silent
fallback. The environment also picks the configuration overlay of a config
directory: ``base.yaml`` first, then ``<environment>.yaml`` when present.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from loom.core.config.errors import ConfigError
from loom.core.environment import (
    CONFIG_VARIABLE,
    ENVIRONMENT_VARIABLE,
    Environment,
    config_source_paths,
    confirm_environment,
    environment_config_paths,
    loom_variable,
    override_variables,
    parse_environment,
    resolve_environment,
)


@pytest.fixture
def no_selector(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENVIRONMENT_VARIABLE, raising=False)


class TestResolveEnvironment:
    @pytest.mark.usefixtures("no_selector")
    def test_without_a_selector_the_environment_is_production(self) -> None:
        assert resolve_environment() is Environment.PRODUCTION

    def test_the_variable_selects_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

        assert resolve_environment() is Environment.DEVELOPMENT

    def test_an_explicit_argument_overrides_the_variable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

        assert resolve_environment("staging") is Environment.STAGING

    def test_an_explicit_member_is_returned_as_is(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "production")

        assert resolve_environment(Environment.TEST) is Environment.TEST

    def test_an_unknown_variable_value_is_a_startup_error_naming_the_valid_ones(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "prod")

        with pytest.raises(ConfigError) as caught:
            resolve_environment()

        message = str(caught.value)
        assert "'prod'" in message
        assert ENVIRONMENT_VARIABLE in message
        assert "production, staging, development, test" in message

    @pytest.mark.usefixtures("no_selector")
    def test_an_unknown_argument_is_a_startup_error(self) -> None:
        with pytest.raises(ConfigError, match="'dev'"):
            resolve_environment("dev")

    def test_an_empty_variable_is_not_read_as_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "")

        with pytest.raises(ConfigError, match=ENVIRONMENT_VARIABLE):
            resolve_environment()


class TestParseEnvironment:
    def test_a_value_from_another_source_parses_to_its_member(self) -> None:
        assert parse_environment(" Staging ", source="environments key") is Environment.STAGING

    def test_an_unknown_value_names_its_source(self) -> None:
        with pytest.raises(ConfigError, match="'dev' from environments key"):
            parse_environment("dev", source="environments key")


class TestDeployedEnvironments:
    @pytest.mark.parametrize(
        ("environment", "deployed"),
        [
            (Environment.PRODUCTION, True),
            (Environment.STAGING, True),
            (Environment.DEVELOPMENT, False),
            (Environment.TEST, False),
        ],
    )
    def test_production_and_staging_are_the_deployed_environments(
        self, environment: Environment, deployed: bool
    ) -> None:
        assert environment.is_deployed is deployed


class TestEnvironmentConfigPaths:
    def test_the_overlay_of_the_environment_follows_the_base(self, tmp_path: Path) -> None:
        (tmp_path / "base.yaml").write_text("app: {name: demo}\n")
        (tmp_path / "production.yaml").write_text("app: {name: live}\n")
        (tmp_path / "development.yaml").write_text("app: {name: local}\n")

        paths = environment_config_paths(tmp_path, Environment.PRODUCTION)

        assert paths == (str(tmp_path / "base.yaml"), str(tmp_path / "production.yaml"))

    def test_an_environment_without_an_overlay_loads_the_base_alone(self, tmp_path: Path) -> None:
        (tmp_path / "base.yaml").write_text("app: {name: demo}\n")

        assert environment_config_paths(tmp_path, Environment.STAGING) == (
            str(tmp_path / "base.yaml"),
        )

    def test_a_directory_without_a_base_is_a_config_error(self, tmp_path: Path) -> None:
        (tmp_path / "production.yaml").write_text("app: {name: live}\n")

        with pytest.raises(ConfigError, match="base.yaml"):
            environment_config_paths(tmp_path, Environment.PRODUCTION)


class TestOverrideVariables:
    def test_only_the_loom_double_underscore_variables_are_overrides(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("LOOM__APP__NAME", "x")
        monkeypatch.setenv("LOOM_ENV", "test")
        monkeypatch.setenv("LOOMX__APP__NAME", "y")

        assert override_variables() == {"LOOM__APP__NAME": "x"}


@pytest.mark.parametrize(
    "module", ["loom.core.environment", "loom.core.config.overrides", "loom.core.checks"]
)
def test_each_module_imports_first_in_a_fresh_interpreter(module: str) -> None:
    src = Path(__file__).resolve().parents[3] / "src"
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        check=False,
        env={"PYTHONPATH": str(src)},
    )

    assert result.returncode == 0, result.stderr


class TestConfirmEnvironment:
    """A declared environment confirms the process one; it never selects it."""

    def test_without_a_declaration_the_process_environment_holds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "staging")

        assert confirm_environment(None, source="the run") is Environment.STAGING

    def test_a_matching_declaration_is_confirmed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "staging")

        assert confirm_environment("Staging", source="the run") is Environment.STAGING

    def test_a_declaration_cannot_move_production_to_development(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "production")

        with pytest.raises(ConfigError, match="'development'.*'production'"):
            confirm_environment("development", source="the run")

    @pytest.mark.usefixtures("no_selector")
    def test_the_default_is_confirmed_like_the_variable(self) -> None:
        with pytest.raises(ConfigError, match="'staging'.*'production'"):
            confirm_environment("staging", source="the run")


class TestConfigSourcePaths:
    def test_a_file_is_loaded_alone(self, tmp_path: Path) -> None:
        path = tmp_path / "etl.yaml"
        path.write_text("storage: {}\n")

        assert config_source_paths(str(path), "production") == (str(path),)

    def test_a_cloud_uri_is_loaded_alone(self) -> None:
        assert config_source_paths("s3://bucket/etl.yaml", None) == ("s3://bucket/etl.yaml",)

    def test_a_directory_layers_the_overlay_of_the_environment(self, tmp_path: Path) -> None:
        (tmp_path / "base.yaml").write_text("a: 1\n")
        (tmp_path / "staging.yaml").write_text("a: 2\n")

        assert config_source_paths(str(tmp_path), "staging") == (
            str(tmp_path / "base.yaml"),
            str(tmp_path / "staging.yaml"),
        )

    def test_a_directory_resolves_the_environment_when_none_is_given(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "base.yaml").write_text("a: 1\n")
        (tmp_path / "development.yaml").write_text("a: 2\n")
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

        assert config_source_paths(str(tmp_path), None)[-1] == str(tmp_path / "development.yaml")


class TestLoomVariable:
    def test_a_loom_variable_is_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(CONFIG_VARIABLE, "/srv/config")

        assert loom_variable(CONFIG_VARIABLE) == "/srv/config"

    def test_an_unset_variable_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(CONFIG_VARIABLE, raising=False)

        assert loom_variable(CONFIG_VARIABLE) is None

    def test_only_loom_variables_are_read(self) -> None:
        with pytest.raises(ValueError, match="HOME"):
            loom_variable("HOME")
