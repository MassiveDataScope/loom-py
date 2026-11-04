"""Unit tests for shared resolver utilities (_resolver_utils).

Tests _split_resolver_key, _expand_env_vars, and _navigate_json directly
to catch edge-case regressions that are only indirectly covered by the
SSM and Secrets Manager resolver test suites.
"""

from __future__ import annotations

import pytest

from loom.core.config._resolver_utils import (
    _expand_env_vars,
    _navigate_json,
    _split_resolver_key,
)
from loom.core.config.errors import ConfigError
from loom.core.connections.aws import aws_error_code

# ---------------------------------------------------------------------------
# _split_resolver_key
# ---------------------------------------------------------------------------


class TestSplitResolverKey:
    def test_empty_string_returns_empty_and_no_keys(self) -> None:
        assert _split_resolver_key("") == ("", [])

    def test_plain_path_no_dot_returns_path_and_empty_keys(self) -> None:
        assert _split_resolver_key("/prod/token") == ("/prod/token", [])

    def test_single_dot_notation(self) -> None:
        path, keys = _split_resolver_key("/prod/db_config.host")
        assert path == "/prod/db_config"
        assert keys == ["host"]

    def test_nested_dot_notation(self) -> None:
        path, keys = _split_resolver_key("/prod/db.connection.host")
        assert path == "/prod/db"
        assert keys == ["connection", "host"]

    def test_no_slash_with_dot(self) -> None:
        path, keys = _split_resolver_key("param.key")
        assert path == "param"
        assert keys == ["key"]

    def test_no_slash_no_dot(self) -> None:
        assert _split_resolver_key("simple") == ("simple", [])

    def test_dot_only_in_first_segment_not_last(self) -> None:
        # dot is in segment "db.prod" which is NOT the last segment
        path, keys = _split_resolver_key("/app/db.prod/token")
        assert path == "/app/db.prod/token"
        assert keys == []

    def test_trailing_dot_ignored(self) -> None:
        # trailing dot produces an empty segment that is filtered out
        path, keys = _split_resolver_key("/prod/db.")
        assert path == "/prod/db"
        assert keys == []

    def test_consecutive_dots_filtered(self) -> None:
        path, keys = _split_resolver_key("/prod/db..host")
        assert path == "/prod/db"
        assert keys == ["host"]

    def test_env_token_in_last_segment_is_not_split(self) -> None:
        """Keys are split unexpanded, so a ``%VAR%`` token stays in the base."""
        assert _split_resolver_key("/app/svc/%ENV%") == ("/app/svc/%ENV%", [])

    def test_env_token_in_base_segment_keeps_operator_written_tail(self) -> None:
        assert _split_resolver_key("/app/%ENV%/db.host") == ("/app/%ENV%/db", ["host"])

    def test_path_with_only_slash(self) -> None:
        assert _split_resolver_key("/") == ("/", [])


# ---------------------------------------------------------------------------
# _expand_env_vars
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# _navigate_json
# ---------------------------------------------------------------------------


class TestNavigateJson:
    def test_single_key(self) -> None:
        result = _navigate_json('{"host": "db.internal"}', ["host"], "/path")
        assert result == "db.internal"

    def test_nested_keys(self) -> None:
        result = _navigate_json('{"conn": {"host": "db"}}', ["conn", "host"], "/path")
        assert result == "db"

    def test_missing_key_raises_config_error(self) -> None:
        with pytest.raises(ConfigError, match="absent"):
            _navigate_json('{"host": "db"}', ["absent"], "/path")

    def test_invalid_json_raises_config_error(self) -> None:
        with pytest.raises(ConfigError, match="not valid JSON"):
            _navigate_json("not-json", ["key"], "/path")

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


# ---------------------------------------------------------------------------
# aws_error_code
# ---------------------------------------------------------------------------


class _BotocoreShapedError(Exception):
    """Stand-in for ``botocore.exceptions.ClientError`` (not a test dep)."""

    def __init__(self, response: object) -> None:
        super().__init__("An error occurred")
        self.response = response


class TestAwsErrorCode:
    def test_returns_code_from_botocore_shaped_response(self) -> None:
        exc = _BotocoreShapedError({"Error": {"Code": "AccessDeniedException"}})
        assert aws_error_code(exc) == "AccessDeniedException"

    def test_falls_back_to_type_name_without_response(self) -> None:
        assert aws_error_code(ValueError("boom")) == "ValueError"

    def test_falls_back_to_type_name_when_response_is_not_a_dict(self) -> None:
        assert aws_error_code(_BotocoreShapedError("not-a-dict")) == "_BotocoreShapedError"

    def test_falls_back_to_type_name_when_code_is_absent(self) -> None:
        exc = _BotocoreShapedError({"Error": {"Message": "no code here"}})
        assert aws_error_code(exc) == "_BotocoreShapedError"

    def test_falls_back_to_type_name_when_code_is_empty(self) -> None:
        assert aws_error_code(_BotocoreShapedError({"Error": {"Code": ""}})) == (
            "_BotocoreShapedError"
        )
