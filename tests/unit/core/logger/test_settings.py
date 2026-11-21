from __future__ import annotations

import msgspec
import pytest

from loom.core.environment import Environment
from loom.core.logger.settings import LogSettings, UserIdentityMode
from loom.core.model.redaction import Secret

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


class TestUserIdentitySettings:
    def test_a_security_event_names_no_user_by_default(self) -> None:
        settings = LogSettings()

        assert (settings.user_identity, settings.user_hash_key) == (UserIdentityMode.NONE, None)

    def test_the_hash_mode_requires_a_key(self) -> None:
        with pytest.raises(ValueError, match="requires user_hash_key"):
            LogSettings(user_identity=UserIdentityMode.HASH)

    @pytest.mark.parametrize("mode", [UserIdentityMode.ID, UserIdentityMode.NONE])
    def test_the_other_modes_need_no_key(self, mode: UserIdentityMode) -> None:
        assert LogSettings(user_identity=mode).user_hash_key is None

    def test_a_key_is_a_secret_that_does_not_show(self) -> None:
        settings = LogSettings(user_identity=UserIdentityMode.HASH, user_hash_key=Secret("k1"))

        assert "k1" not in repr(settings)

    def test_an_unknown_mode_is_refused_when_decoded(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="user_identity"):
            msgspec.convert({"user_identity": "plain"}, LogSettings)


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

        assert LogSettings(format="json").format_for(Environment.PRODUCTION) == "json"
