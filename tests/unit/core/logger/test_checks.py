"""``loom.logger.E001`` and ``E002``: ``observability.log`` as it is written."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.checks import run_deploy_checks
from loom.core.config import ConfigContext
from loom.core.logger.checks import UNKNOWN_FORMAT_CHECK, UNKNOWN_LEVEL_CHECK

_LOGGER_CHECKS = {UNKNOWN_FORMAT_CHECK, UNKNOWN_LEVEL_CHECK}


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
