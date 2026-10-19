"""One environment selector, resolved once at startup.

``LOOM_ENV`` or an explicit argument picks the environment; with neither the
environment is production, so a container started without the variable runs
with the secure defaults. An unknown value is a startup error, never a silent
fallback. The environment also picks the configuration overlay of a config
directory: ``base.yaml`` first, then ``<environment>.yaml`` when present.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.core.config.errors import ConfigError
from loom.core.environment import (
    ENVIRONMENT_VARIABLE,
    Environment,
    environment_config_paths,
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
