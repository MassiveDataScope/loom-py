from __future__ import annotations

import msgspec
import pytest

from loom.core.environment import Environment
from loom.core.logger.settings import LogFormatName, LogSettings

_LAMBDA = "AWS_LAMBDA_FUNCTION_NAME"


class TestLogSettings:
    def test_the_defaults_are_info_with_no_chosen_format(self) -> None:
        settings = LogSettings()

        assert (settings.format, settings.level, settings.named_levels) == (None, "INFO", {})

    @pytest.mark.parametrize("level", ["debug", "INFO", "Warning"])
    def test_a_level_is_accepted_in_any_case(self, level: str) -> None:
        assert LogSettings(level=level).level == level

    def test_an_unknown_level_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unsupported log level 'LOUD'"):
            LogSettings(level="LOUD")

    def test_an_unknown_named_level_is_refused_when_decoded(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="unsupported log level 'LOUD'"):
            msgspec.convert({"named_levels": {"botocore": "LOUD"}}, LogSettings)

    def test_an_unknown_key_is_refused_when_decoded(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="renderer"):
            msgspec.convert({"renderer": "console"}, LogSettings)


class TestFormatFor:
    @pytest.fixture(autouse=True)
    def _outside_lambda(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(_LAMBDA, raising=False)

    @pytest.mark.parametrize(
        ("environment", "expected"),
        [
            (Environment.PRODUCTION, "json"),
            (Environment.STAGING, "json"),
            (Environment.DEVELOPMENT, "text"),
            (Environment.TEST, "text"),
        ],
    )
    def test_the_environment_picks_json_or_text(
        self, environment: Environment, expected: str
    ) -> None:
        assert LogSettings().format_for(environment) == expected

    def test_inside_lambda_the_format_is_lambda(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(_LAMBDA, "billing-handler")

        assert LogSettings().format_for(Environment.PRODUCTION) == "lambda"

    def test_a_written_format_wins_over_lambda(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(_LAMBDA, "billing-handler")

        assert LogSettings(format=LogFormatName.JSON).format_for(Environment.PRODUCTION) == "json"
