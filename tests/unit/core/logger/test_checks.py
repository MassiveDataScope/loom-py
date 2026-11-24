"""``loom.logger.E001`` and ``E002``: ``observability.log`` as it is written."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.logger.checks import UNKNOWN_FORMAT_CHECK, UNKNOWN_LEVEL_CHECK

_LOGGER_CHECKS = {UNKNOWN_FORMAT_CHECK, UNKNOWN_LEVEL_CHECK}


def _findings(log: dict[str, Any] | None) -> dict[str, str]:
    raw: dict[str, Any] = {} if log is None else {"observability": {"log": log}}
    report = run_deploy_checks(ConfigContext.from_dict(raw))
    return {result.id: result.message for result in report.results if result.id in _LOGGER_CHECKS}


class TestLogFormatCheck:
    @pytest.mark.parametrize("log", [None, {}, {"format": "json"}, {"format": "lambda"}])
    def test_a_known_or_absent_format_passes(self, log: dict[str, Any] | None) -> None:
        assert _findings(log) == {}

    def test_an_unknown_format_fails(self) -> None:
        assert list(_findings({"format": "xml"})) == [UNKNOWN_FORMAT_CHECK]

    def test_a_format_from_a_placeholder_is_not_judged(self) -> None:
        assert _findings({"format": "${oc.env:LOG_FORMAT}"}) == {}


class TestLogLevelCheck:
    @pytest.mark.parametrize(
        "log", [{"level": "DEBUG"}, {"named_levels": {"botocore": "WARNING"}}, {"level": "${x}"}]
    )
    def test_known_or_unresolved_levels_pass(self, log: dict[str, Any]) -> None:
        assert _findings(log) == {}

    def test_every_unknown_level_is_named_in_one_finding(self) -> None:
        log = {"level": "info", "named_levels": {"botocore": "LOUD"}}

        assert _findings(log) == {
            UNKNOWN_LEVEL_CHECK: "observability.log.level is 'info'; "
            "observability.log.named_levels.botocore is 'LOUD': not a log level."
        }
