"""Unit tests for SecretsManagerResolver.

Secrets live in moto's Secrets Manager; a fake AWS island counts the clients built
and scripts the failures moto cannot produce.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import boto3
import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver
from loom.core.config.secrets import SecretsManagerResolver
from tests.helpers.extras import BOTO3_ISLAND, fake_aws_island

KEY = "/app/%MY_ENV%/db"
EXPANDED = "/app/super-secret/db"


def _put(secretsmanager: Any, name: str, value: str = "mysecret") -> None:
    secretsmanager.create_secret(Name=name, SecretString=value)


def _answer_with_a_secret(client: MagicMock) -> None:
    client.get_secret_value.return_value = {"SecretString": "mysecret"}


def _failing_island(monkeypatch: pytest.MonkeyPatch, failure: Exception) -> None:
    def raise_on_get(client: MagicMock) -> None:
        client.get_secret_value.side_effect = failure

    fake_aws_island(monkeypatch, setup=raise_on_get)


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverIdentity:
    def test_name_returns_secrets(self) -> None:
        assert SecretsManagerResolver().name == "secrets"

    def test_implements_config_resolver_protocol(self) -> None:
        assert isinstance(SecretsManagerResolver(), ConfigResolver)


# ---------------------------------------------------------------------------
# Basic resolve
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverResolve:
    def test_returns_secret_string_value(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/token")
        assert SecretsManagerResolver().resolve("/prod/token") == "mysecret"

    def test_calls_secretsmanager_with_secret_id(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/token", "the-token")
        _put(secretsmanager, "/prod/other", "not-the-token")
        assert SecretsManagerResolver().resolve("/prod/token") == "the-token"

    def test_raises_config_error_on_binary_secret(self, secretsmanager: Any) -> None:
        secretsmanager.create_secret(Name="/prod/token", SecretBinary=b"bytes")
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/prod/token")


# ---------------------------------------------------------------------------
# Environment variable expansion
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverEnvVarExpansion:
    def test_expands_env_var_in_path(
        self, secretsmanager: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        _put(secretsmanager, "/myapp/prod/token")
        assert SecretsManagerResolver().resolve("/myapp/%ENVIRONMENT%/token") == "mysecret"

    def test_raises_on_missing_env_var(
        self, mocked_aws: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/path/%MISSING_VAR%/token")

    def test_percent_syntax_parses_from_omegaconf_yaml(
        self, secretsmanager: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: {VAR} syntax broke OmegaConf ANTLR grammar; %VAR% must not."""
        from omegaconf import OmegaConf

        monkeypatch.setenv("ENV", "prod")
        _put(secretsmanager, "/app/prod/token")
        resolver = SecretsManagerResolver()
        OmegaConf.register_new_resolver("secrets", resolver.resolve, replace=True)
        cfg = OmegaConf.create({"val": "${secrets:/app/%ENV%/token}"})
        assert cfg.val == "mysecret"


# ---------------------------------------------------------------------------
# Dot notation (JSON key navigation)
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverDotNotation:
    def test_plain_path_returns_string(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/token")
        assert SecretsManagerResolver().resolve("/prod/token") == "mysecret"

    def test_single_key_navigation(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/db", json.dumps({"host": "db.internal"}))
        assert SecretsManagerResolver().resolve("/prod/db.host") == "db.internal"

    def test_nested_key_navigation(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/cfg", json.dumps({"conn": {"host": "internal"}}))
        assert SecretsManagerResolver().resolve("/prod/cfg.conn.host") == "internal"

    def test_raises_on_invalid_json(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/db", "not-json-at-all")
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/prod/db.host")

    def test_raises_on_missing_json_key(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/prod/db", json.dumps({"host": "db"}))
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/prod/db.missing_key")

    def test_env_var_expansion_then_dot_navigation(
        self, secretsmanager: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        _put(secretsmanager, "/app/prod/db", json.dumps({"host": "prod-db"}))
        assert SecretsManagerResolver().resolve("/app/%ENV%/db.host") == "prod-db"


# ---------------------------------------------------------------------------
# Client lifecycle
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverClientLifecycle:
    def test_client_created_once_across_multiple_resolves(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = fake_aws_island(monkeypatch, setup=_answer_with_a_secret)
        resolver = SecretsManagerResolver()
        resolver.resolve("/key/one")
        resolver.resolve("/key/two")
        assert [service for service, _ in island.built] == ["secretsmanager"]

    def test_region_forwarded_to_boto3(self, mocked_aws: None) -> None:
        boto3.client("secretsmanager", region_name="eu-west-1").create_secret(
            Name="/some/secret", SecretString="in-ireland"
        )
        assert SecretsManagerResolver("eu-west-1").resolve("/some/secret") == "in-ireland"

    def test_region_none_by_default(self, secretsmanager: Any) -> None:
        _put(secretsmanager, "/some/secret", "in-default-region")
        assert SecretsManagerResolver().resolve("/some/secret") == "in-default-region"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverLogging:
    def test_info_log_emitted_with_path(
        self, secretsmanager: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        _put(secretsmanager, "/prod/db", json.dumps({"host": "db.internal"}))
        with caplog.at_level(logging.INFO, logger="loom.core.config.secrets"):
            SecretsManagerResolver().resolve("/prod/db.host")
        messages = [r.message for r in caplog.records]
        assert any("/prod/db" in msg for msg in messages), (
            f"Expected a log record containing '/prod/db', got: {messages}"
        )

    def test_log_never_contains_secret_value(
        self, secretsmanager: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        _put(secretsmanager, "/prod/token")
        with caplog.at_level(logging.INFO, logger="loom.core.config.secrets"):
            SecretsManagerResolver().resolve("/prod/token")
        messages = [r.message for r in caplog.records]
        assert all("mysecret" not in msg for msg in messages), (
            f"Log must not contain the secret value 'mysecret', got: {messages}"
        )

    def test_log_uses_original_key_not_expanded_path(
        self,
        secretsmanager: Any,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("STAGE", "prod")
        _put(secretsmanager, "/app/prod/db")
        with caplog.at_level(logging.INFO, logger="loom.core.config.secrets"):
            SecretsManagerResolver().resolve("/app/%STAGE%/db")
        messages = [r.message for r in caplog.records]
        assert any("/app/%STAGE%/db" in msg for msg in messages), messages
        assert all("/app/prod/db" not in msg for msg in messages), messages


# ---------------------------------------------------------------------------
# Unexpanded key in logs and errors (AC7)
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverUnexpandedKey:
    """AC7: logs and ConfigError text carry the key as written, never the env value."""

    def test_log_carries_unexpanded_key_and_client_gets_expanded_path(
        self, my_env: None, secretsmanager: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        _put(secretsmanager, EXPANDED)
        with caplog.at_level(logging.INFO, logger="loom.core.config.secrets"):
            assert SecretsManagerResolver().resolve(KEY) == "mysecret"
        messages = [r.message for r in caplog.records]
        assert all("super-secret" not in msg for msg in messages), messages
        assert any(KEY in msg for msg in messages), messages

    def test_fetch_error_carries_unexpanded_key_and_chains_cause(
        self, my_env: None, mocked_aws: None
    ) -> None:
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is not None

    def test_binary_secret_error_carries_unexpanded_key(
        self, my_env: None, secretsmanager: Any
    ) -> None:
        secretsmanager.create_secret(Name=EXPANDED, SecretBinary=b"bytes")
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text

    def test_dotted_env_value_in_last_segment_is_fetched_whole(
        self,
        secretsmanager: Any,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Splitting before expansion keeps a dotted env value out of navigation."""
        monkeypatch.setenv("DOTTED_ENV", "db.prod-secret")
        _put(secretsmanager, "/app/svc/db.prod-secret", json.dumps({"host": "db"}))
        with caplog.at_level(logging.INFO, logger="loom.core.config.secrets"):
            value = SecretsManagerResolver().resolve("/app/svc/%DOTTED_ENV%")
        assert value == json.dumps({"host": "db"})
        messages = [r.message for r in caplog.records]
        assert all("prod-secret" not in msg for msg in messages), messages

    def test_env_token_in_base_segment_still_expands_and_navigates(
        self, my_env: None, secretsmanager: Any
    ) -> None:
        _put(secretsmanager, EXPANDED, json.dumps({"host": "db-1"}))
        assert SecretsManagerResolver().resolve(f"{KEY}.host") == "db-1"

    def test_fetch_error_message_carries_aws_error_code_not_expanded_path(
        self, my_env: None, mocked_aws: None
    ) -> None:
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        text = str(info.value)
        assert "ResourceNotFoundException" in text
        assert EXPANDED not in text
        assert "super-secret" not in text

    def test_fetch_error_message_falls_back_to_type_name(
        self, my_env: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _failing_island(monkeypatch, TimeoutError("boom"))
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        assert "TimeoutError" in str(info.value)

    def test_invalid_json_error_carries_unexpanded_key_without_cause(
        self, my_env: None, secretsmanager: Any
    ) -> None:
        _put(secretsmanager, EXPANDED, "not-json-at-all")
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(f"{KEY}.host")
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text
        assert "not-json-at-all" not in text
        assert info.value.__cause__ is None

    def test_missing_json_key_error_carries_unexpanded_key_without_cause(
        self, my_env: None, secretsmanager: Any
    ) -> None:
        _put(secretsmanager, EXPANDED, json.dumps({"host": "db"}))
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(f"{KEY}.missing_key")
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverErrors:
    def test_raises_config_error_on_api_exception(self, mocked_aws: None) -> None:
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError, match="ResourceNotFoundException"):
            resolver.resolve("/missing/secret")

    def test_raises_config_error_on_empty_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        island = fake_aws_island(monkeypatch)
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("")
        assert island.built == []

    def test_raises_config_error_when_boto3_not_installed(self) -> None:
        resolver = SecretsManagerResolver()
        with (
            patch.dict(sys.modules, {"boto3": None, BOTO3_ISLAND: None}),
            pytest.raises(ConfigError, match="boto3 is required"),
        ):
            resolver.resolve("/some/secret")
