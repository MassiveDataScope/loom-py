"""``loom.logger.E001`` and ``E002``: ``observability.log`` as it is written."""

from __future__ import annotations

from typing import Any

import pytest

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


@pytest.fixture(autouse=True)
def _outside_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENVIRONMENT_VARIABLE, Environment.DEVELOPMENT.value)


def _ids(log: dict[str, Any] | None) -> list[str]:
    raw: dict[str, Any] = {} if log is None else {"observability": {"log": log}}
    report = run_deploy_checks(ConfigContext.from_dict(raw))
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
    @pytest.fixture
    def production(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, Environment.PRODUCTION.value)

    @pytest.mark.usefixtures("production")
    @pytest.mark.parametrize("log", [{}, {"user_identity": "none"}])
    def test_production_warns_when_no_identity_is_written(self, log: dict[str, Any]) -> None:
        assert _ids(log) == [NO_USER_IDENTITY_CHECK]

    @pytest.mark.usefixtures("production")
    @pytest.mark.parametrize(
        "log", [{"user_identity": "id"}, {"user_identity": "hash", "user_hash_key": "k"}]
    )
    def test_production_with_an_identity_passes(self, log: dict[str, Any]) -> None:
        assert _ids(log) == []

    @pytest.mark.parametrize("environment", [Environment.STAGING, Environment.DEVELOPMENT])
    def test_another_environment_does_not_warn(
        self, environment: Environment, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, environment.value)

        assert _ids({"user_identity": "none"}) == []
