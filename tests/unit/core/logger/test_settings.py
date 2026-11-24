from __future__ import annotations

import msgspec
import pytest

from loom.core.environment import Environment
from loom.core.logger.settings import QUIET_LIBRARIES, LogFormatName, LogLevel, LogSettings
from loom.testing import make

_LAMBDA = "AWS_LAMBDA_FUNCTION_NAME"


class TestLogSettings:
    def test_the_defaults_are_info_with_no_chosen_format(self) -> None:
        settings = LogSettings()

        assert (settings.format, settings.level, settings.named_levels) == (None, "INFO", {})

    def test_a_level_not_written_as_the_standard_library_names_it_is_refused(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="Invalid enum value 'info'"):
            msgspec.convert({"named_levels": {"botocore": "info"}}, LogSettings)

    def test_a_written_logger_level_wins_over_a_quiet_library(self) -> None:
        settings = make(LogSettings, named_levels={"botocore": LogLevel.DEBUG}, subject_key=None)

        assert settings.logger_levels() == {**QUIET_LIBRARIES, "botocore": LogLevel.DEBUG}

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
