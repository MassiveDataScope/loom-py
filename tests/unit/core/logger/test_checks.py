"""``loom.logger.E001`` and ``E002``: ``observability.log`` as it is written."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from omegaconf import OmegaConf

from loom.composition.checked import CheckedProject
from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.logger.checks import (
    MISSING_USER_HASH_KEY_CHECK,
    NO_USER_IDENTITY_CHECK,
    UNKNOWN_FORMAT_CHECK,
    UNKNOWN_LEVEL_CHECK,
    UNKNOWN_USER_IDENTITY_CHECK,
)

_LOGGER_CHECKS = {
    UNKNOWN_FORMAT_CHECK,
    UNKNOWN_LEVEL_CHECK,
    UNKNOWN_USER_IDENTITY_CHECK,
    MISSING_USER_HASH_KEY_CHECK,
    NO_USER_IDENTITY_CHECK,
}


def _ids(
    log: dict[str, Any] | None, environment: Environment = Environment.DEVELOPMENT
) -> list[str]:
    raw: dict[str, Any] = {} if log is None else {"observability": {"log": log}}
    config = ConfigContext(OmegaConf.create(raw), environment=environment)
    report = run_deploy_checks(config)
    return [result.id for result in report.results if result.id in _LOGGER_CHECKS]


class TestLogFormatCheck:
    @pytest.mark.parametrize("log", [None, {}, {"format": "json"}, {"format": "lambda"}])
    def test_a_registered_or_absent_format_passes(self, log: dict[str, Any] | None) -> None:
        assert _ids(log) == []

    def test_an_unregistered_format_fails(self) -> None:
        assert _ids({"format": "xml"}) == [UNKNOWN_FORMAT_CHECK]

    def test_a_format_from_a_placeholder_is_not_judged(self) -> None:
        assert _ids({"format": "${oc.env:LOG_FORMAT}"}) == []


class TestLogLevelCheck:
    @pytest.mark.parametrize(
        "log", [{"level": "debug"}, {"named_levels": {"botocore": "WARNING"}}, {"level": "${x}"}]
    )
    def test_known_or_unresolved_levels_pass(self, log: dict[str, Any]) -> None:
        assert _ids(log) == []

    @pytest.mark.parametrize(
        "log", [{"level": "LOUD"}, {"named_levels": {"botocore": "LOUD"}}], ids=["root", "named"]
    )
    def test_an_unknown_level_fails(self, log: dict[str, Any]) -> None:
        assert _ids(log) == [UNKNOWN_LEVEL_CHECK]


class TestUserIdentityChecks:
    @pytest.mark.parametrize("mode", ["hash", "id", "none", "${oc.env:MODE}"])
    def test_a_mode_passes(self, mode: str) -> None:
        assert UNKNOWN_USER_IDENTITY_CHECK not in _ids(
            {"user_identity": mode, "user_hash_key": "k"}
        )

    def test_an_unknown_mode_fails(self) -> None:
        assert UNKNOWN_USER_IDENTITY_CHECK in _ids({"user_identity": "plain"})

    def test_the_hash_mode_without_a_key_fails(self) -> None:
        assert MISSING_USER_HASH_KEY_CHECK in _ids({"user_identity": "hash"})

    @pytest.mark.parametrize(
        "log",
        [
            {"user_identity": "hash", "user_hash_key": "${oc.env:KEY}"},
            {"user_identity": "id"},
            {"user_identity": "${oc.env:MODE}"},
        ],
    )
    def test_a_key_or_another_mode_passes(self, log: dict[str, Any]) -> None:
        assert MISSING_USER_HASH_KEY_CHECK not in _ids(log)


class TestNoUserIdentityWarning:
    @pytest.mark.parametrize("log", [{}, {"user_identity": "none"}])
    def test_production_warns_when_no_identity_is_written(self, log: dict[str, Any]) -> None:
        assert _ids(log, Environment.PRODUCTION) == [NO_USER_IDENTITY_CHECK]

    @pytest.mark.parametrize(
        "log", [{"user_identity": "id"}, {"user_identity": "hash", "user_hash_key": "k"}]
    )
    def test_production_with_an_identity_passes(self, log: dict[str, Any]) -> None:
        assert _ids(log, Environment.PRODUCTION) == []

    @pytest.mark.parametrize("environment", [Environment.STAGING, Environment.DEVELOPMENT])
    def test_another_environment_does_not_warn(self, environment: Environment) -> None:
        assert _ids({"user_identity": "none"}, environment) == []


class TestEnvironmentOfLoomCheck:
    @staticmethod
    def _warned(tmp_path: Path, environment: str) -> bool:
        path = tmp_path / "app.yaml"
        path.write_text(
            yaml.safe_dump({"app": {"name": "acme"}, "observability": {"log": {"format": "json"}}})
        )
        checked = CheckedProject.load(str(path), environment=environment)
        return NO_USER_IDENTITY_CHECK in [result.id for result in checked.report.warnings]

    def test_the_environment_flag_decides_when_the_variable_is_unset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(ENVIRONMENT_VARIABLE, raising=False)

        assert self._warned(tmp_path, "production")

    def test_the_environment_flag_decides_over_the_variable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "production")

        assert not self._warned(tmp_path, "development")

    def test_the_variable_decides_when_the_context_names_no_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "staging")

        assert ConfigContext.from_dict({}).environment is Environment.STAGING
