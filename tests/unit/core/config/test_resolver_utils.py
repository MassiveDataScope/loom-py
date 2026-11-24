"""Unit tests for shared resolver utilities (_resolver_utils).

Tests _split_resolver_key, _expand_env_vars, and _navigate_json directly
to catch edge-case regressions that are only indirectly covered by the
SSM and Secrets Manager resolver test suites.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable

import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.event import ConfigEvent, ConfigLogKey
from loom.core.config.resolution import (
    _expand_env_vars,
    _navigate_json,
    _split_resolver_key,
    resolve_cached,
)
from loom.core.connections.aws import aws_error_code
from tests.helpers.loggers import CapturedLog


def _fetched(client: object, path: str, *, label: str) -> str:
    return "value"


def _refused(client: object, path: str, *, label: str) -> str:
    raise ConfigError(f"{label} was refused")


class TestResolveCached:
    @pytest.mark.parametrize(
        ("fetch", "fetched"),
        [
            (_fetched, [{ConfigLogKey.RESOLVER: "ssm", ConfigLogKey.KEY: "/app/token"}]),
            (_refused, []),
        ],
        ids=["fetched", "refused"],
    )
    def test_a_key_is_logged_fetched_only_once_the_fetch_returned(
        self,
        caplog: pytest.LogCaptureFixture,
        fetch: Callable[..., str],
        fetched: list[dict[str, str]],
    ) -> None:
        with caplog.at_level(logging.INFO), contextlib.suppress(ConfigError):
            resolve_cached(
                "/app/token", None, name="ssm", label="ssm", unbound={}, client=object, fetch=fetch
            )

        assert CapturedLog(caplog).named(ConfigEvent.RESOLVER_FETCHED) == fetched


class TestSplitResolverKey:
    @pytest.mark.parametrize(
        ("key", "split"),
        [
            pytest.param("", ("", []), id="empty"),
            pytest.param("/prod/token", ("/prod/token", []), id="plain_path"),
            pytest.param("simple", ("simple", []), id="no_slash_no_dot"),
            pytest.param("/", ("/", []), id="only_slash"),
            pytest.param("/prod/db_config.host", ("/prod/db_config", ["host"]), id="single_dot"),
            pytest.param("param.key", ("param", ["key"]), id="no_slash_with_dot"),
            pytest.param("/prod/db..host", ("/prod/db", ["host"]), id="consecutive_dots"),
            pytest.param("/prod/db.a.host", ("/prod/db", ["a", "host"]), id="nested_dots"),
            pytest.param("/app/db.prod/token", ("/app/db.prod/token", []), id="dot_not_last"),
            pytest.param("/prod/db.", ("/prod/db", []), id="trailing_dot"),
            pytest.param("/app/svc/%ENV%", ("/app/svc/%ENV%", []), id="env_token_last"),
            pytest.param("/app/%ENV%/db.host", ("/app/%ENV%/db", ["host"]), id="env_token_base"),
        ],
    )
    def test_splits_the_dotted_tail_of_the_last_segment(
        self, key: str, split: tuple[str, list[str]]
    ) -> None:
        assert _split_resolver_key(key) == split


class TestExpandEnvVars:
    def test_no_placeholders_returns_unchanged(self) -> None:
        assert _expand_env_vars("/prod/token") == "/prod/token"

    def test_single_placeholder_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENV", "staging")
        assert _expand_env_vars("/app/%ENV%/token") == "/app/staging/token"

    def test_multiple_placeholders_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REGION", "eu-west-1")
        monkeypatch.setenv("ENV", "prod")
        assert _expand_env_vars("/%REGION%/%ENV%/key") == "/eu-west-1/prod/key"

    def test_missing_env_var_raises_config_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING", raising=False)
        with pytest.raises(ConfigError, match="MISSING"):
            _expand_env_vars("/path/%MISSING%/token")

    def test_lowercase_not_expanded(self) -> None:
        assert _expand_env_vars("/path/%env%/token") == "/path/%env%/token"


class TestNavigateJson:
    def test_single_key(self) -> None:
        result = _navigate_json('{"host": "db.internal"}', ["host"], "/path")
        assert result == "db.internal"

    def test_nested_keys(self) -> None:
        result = _navigate_json('{"conn": {"host": "db"}}', ["conn", "host"], "/path")
        assert result == "db"

    @pytest.mark.parametrize(
        ("document", "match", "key"),
        [
            pytest.param(
                '{"host": "db"}', "absent", "absent", id="missing_key_raises_config_error"
            ),
            pytest.param(
                "not-json", "not valid JSON", "key", id="invalid_json_raises_config_error"
            ),
        ],
    )
    def test_unusable_json_raises_config_error(self, document: str, match: str, key: str) -> None:
        with pytest.raises(ConfigError, match=match):
            _navigate_json(document, [key], "/path")

    def test_invalid_json_error_does_not_expose_raw_value(self) -> None:
        """Security: JSONDecodeError.__cause__ must not hold the raw secret."""
        secret_value = "my_raw_secret_value"
        with pytest.raises(ConfigError) as exc_info:
            _navigate_json(secret_value, ["key"], "/path")
        assert exc_info.value.__cause__ is None, (
            "ConfigError must not chain JSONDecodeError — its .doc attribute holds the raw secret"
        )

    def test_non_dict_intermediate_raises_config_error(self) -> None:
        with pytest.raises(ConfigError):
            _navigate_json('{"host": "db"}', ["host", "nested"], "/path")

    def test_invalid_json_error_carries_given_path_verbatim(self) -> None:
        """The path argument is echoed as-is, so callers may pass the unexpanded key."""
        with pytest.raises(ConfigError) as exc_info:
            _navigate_json("not-json", ["key"], "/app/%MY_ENV%/db")
        assert "/app/%MY_ENV%/db" in str(exc_info.value)
        assert "not-json" not in str(exc_info.value)
        assert exc_info.value.__cause__ is None

    def test_missing_key_error_carries_given_path_verbatim_without_cause(self) -> None:
        with pytest.raises(ConfigError) as exc_info:
            _navigate_json('{"host": "db"}', ["absent"], "/app/%MY_ENV%/db")
        assert "/app/%MY_ENV%/db" in str(exc_info.value)
        assert exc_info.value.__cause__ is None


class _BotocoreShapedError(Exception):
    """Stand-in for ``botocore.exceptions.ClientError`` (not a test dep)."""

    def __init__(self, response: object) -> None:
        super().__init__("An error occurred")
        self.response = response


_SHAPED = "_BotocoreShapedError"


class TestAwsErrorCode:
    @pytest.mark.parametrize(
        ("exc", "code"),
        [
            pytest.param(
                _BotocoreShapedError({"Error": {"Code": "AccessDenied"}}), "AccessDenied", id="code"
            ),
            pytest.param(ValueError("boom"), "ValueError", id="no_response"),
            pytest.param(_BotocoreShapedError("not-a-dict"), _SHAPED, id="response_not_a_dict"),
            pytest.param(_BotocoreShapedError({"Error": {"Message": "m"}}), _SHAPED, id="no_code"),
            pytest.param(_BotocoreShapedError({"Error": {"Code": ""}}), _SHAPED, id="empty_code"),
        ],
    )
    def test_names_the_aws_error_code_or_else_the_type(self, exc: Exception, code: str) -> None:
        assert aws_error_code(exc) == code
