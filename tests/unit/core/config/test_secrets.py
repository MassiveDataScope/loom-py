"""Unit tests for SecretsManagerResolver.

The AWS island serves a mock client; a few tests read moto's Secrets Manager instead.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

import pytest

from loom.connections.aws.secrets import SecretsManagerResolver
from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver
from tests.helpers.aws import OTHER_REGION, moto_client
from tests.helpers.aws_island import FakeAwsIsland
from tests.unit.core.config.doubles import BotocoreClientError


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": "mysecret"}
    return client


@pytest.fixture
def island(mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch) -> FakeAwsIsland:
    return FakeAwsIsland(client=mock_client).install(monkeypatch)


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
    def test_returns_secret_string_value(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        result = SecretsManagerResolver().resolve("/prod/token")
        assert result == "mysecret"

    def test_calls_secretsmanager_with_secret_id(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        SecretsManagerResolver().resolve("/prod/token")
        mock_client.get_secret_value.assert_called_once_with(SecretId="/prod/token")

    def test_raises_config_error_on_binary_secret(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretBinary": b"bytes"}
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/prod/token")


# ---------------------------------------------------------------------------
# Environment variable expansion
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverEnvVarExpansion:
    def test_expands_env_var_in_path(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch, island: FakeAwsIsland
    ) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        SecretsManagerResolver().resolve("/myapp/%ENVIRONMENT%/token")
        mock_client.get_secret_value.assert_called_once_with(SecretId="/myapp/prod/token")

    def test_raises_on_missing_env_var(
        self, monkeypatch: pytest.MonkeyPatch, island: FakeAwsIsland
    ) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/path/%MISSING_VAR%/token")

    def test_percent_syntax_parses_from_omegaconf_yaml(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch, island: FakeAwsIsland
    ) -> None:
        """Regression: {VAR} syntax broke OmegaConf ANTLR grammar; %VAR% must not."""
        from omegaconf import OmegaConf

        monkeypatch.setenv("ENV", "prod")
        resolver = SecretsManagerResolver()
        OmegaConf.register_new_resolver("secrets", resolver.resolve, replace=True)
        cfg = OmegaConf.create({"val": "${secrets:/app/%ENV%/token}"})
        _ = cfg.val
        mock_client.get_secret_value.assert_called_once_with(SecretId="/app/prod/token")


# ---------------------------------------------------------------------------
# Dot notation (JSON key navigation)
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverDotNotation:
    def test_plain_path_returns_string(self, mock_client: MagicMock, island: FakeAwsIsland) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "mysecret"}
        result = SecretsManagerResolver().resolve("/prod/token")
        assert result == "mysecret"

    def test_single_key_navigation(self, mock_client: MagicMock, island: FakeAwsIsland) -> None:
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "db.internal"})
        }
        result = SecretsManagerResolver().resolve("/prod/db.host")
        assert result == "db.internal"
        mock_client.get_secret_value.assert_called_once_with(SecretId="/prod/db")

    def test_nested_key_navigation(self, mock_client: MagicMock, island: FakeAwsIsland) -> None:
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"conn": {"host": "internal"}})
        }
        result = SecretsManagerResolver().resolve("/prod/cfg.conn.host")
        assert result == "internal"

    def test_raises_on_invalid_json(self, mock_client: MagicMock, island: FakeAwsIsland) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "not-json-at-all"}
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/prod/db.host")

    def test_raises_on_missing_json_key(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps({"host": "db"})}
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/prod/db.missing_key")

    def test_env_var_expansion_then_dot_navigation(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch, island: FakeAwsIsland
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "prod-db"})
        }
        result = SecretsManagerResolver().resolve("/app/%ENV%/db.host")
        assert result == "prod-db"
        mock_client.get_secret_value.assert_called_once_with(SecretId="/app/prod/db")


# ---------------------------------------------------------------------------
# Client lifecycle
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverClientLifecycle:
    def test_client_created_once_across_multiple_resolves(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        resolver = SecretsManagerResolver()
        resolver.resolve("/key/one")
        resolver.resolve("/key/two")
        assert len(island.built) == 1

    def test_the_resolver_region_is_the_region_of_its_client(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        SecretsManagerResolver(OTHER_REGION).resolve("/some/secret")
        assert island.regions_built == [("secretsmanager", OTHER_REGION)]

    def test_region_none_by_default(self, mock_client: MagicMock, island: FakeAwsIsland) -> None:
        SecretsManagerResolver().resolve("/some/secret")
        assert island.regions_built == [("secretsmanager", None)]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverLogging:
    def test_info_log_emitted_with_path(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "db.internal"})
        }
        with caplog.at_level(logging.INFO, logger="loom.connections.aws.secrets"):
            SecretsManagerResolver().resolve("/prod/db.host")
        messages = [r.message for r in caplog.records]
        assert any("/prod/db" in msg for msg in messages), (
            f"Expected a log record containing '/prod/db', got: {messages}"
        )

    def test_log_never_contains_secret_value(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "mysecret"}
        with caplog.at_level(logging.INFO, logger="loom.connections.aws.secrets"):
            SecretsManagerResolver().resolve("/prod/token")
        messages = [r.message for r in caplog.records]
        assert all("mysecret" not in msg for msg in messages), (
            f"Log must not contain the secret value 'mysecret', got: {messages}"
        )

    def test_log_uses_original_key_not_expanded_path(
        self,
        mock_client: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        island: FakeAwsIsland,
    ) -> None:
        monkeypatch.setenv("STAGE", "prod")
        with caplog.at_level(logging.INFO, logger="loom.connections.aws.secrets"):
            SecretsManagerResolver().resolve("/app/%STAGE%/db")
        messages = [r.message for r in caplog.records]
        assert any("/app/%STAGE%/db" in msg for msg in messages), messages
        assert all("/app/prod/db" not in msg for msg in messages), messages


# ---------------------------------------------------------------------------
# Unexpanded key in logs and errors (AC7)
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverUnexpandedKey:
    """AC7: logs and ConfigError text carry the key as written, never the env value."""

    KEY = "/app/%MY_ENV%/db"
    EXPANDED = "/app/super-secret/db"

    def test_log_carries_unexpanded_key_and_client_gets_expanded_path(
        self,
        my_env: None,
        mock_client: MagicMock,
        caplog: pytest.LogCaptureFixture,
        island: FakeAwsIsland,
    ) -> None:
        with caplog.at_level(logging.INFO, logger="loom.connections.aws.secrets"):
            SecretsManagerResolver().resolve(self.KEY)
        messages = [r.message for r in caplog.records]
        assert all("super-secret" not in msg for msg in messages), messages
        assert any(self.KEY in msg for msg in messages), messages
        mock_client.get_secret_value.assert_called_once_with(SecretId=self.EXPANDED)

    def test_fetch_error_carries_unexpanded_key_and_chains_cause(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        cause = Exception(f"ResourceNotFoundException: {self.EXPANDED}")
        mock_client.get_secret_value.side_effect = cause
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(self.KEY)
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is cause

    def test_binary_secret_error_carries_unexpanded_key(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretBinary": b"bytes"}
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(self.KEY)
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text

    def test_dotted_env_value_in_last_segment_is_fetched_whole(
        self,
        mock_client: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        island: FakeAwsIsland,
    ) -> None:
        """Splitting before expansion keeps a dotted env value out of navigation."""
        monkeypatch.setenv("DOTTED_ENV", "db.prod-secret")
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps({"host": "db"})}
        with caplog.at_level(logging.INFO, logger="loom.connections.aws.secrets"):
            value = SecretsManagerResolver().resolve("/app/svc/%DOTTED_ENV%")
        assert value == json.dumps({"host": "db"})
        mock_client.get_secret_value.assert_called_once_with(SecretId="/app/svc/db.prod-secret")
        messages = [r.message for r in caplog.records]
        assert all("prod-secret" not in msg for msg in messages), messages

    def test_env_token_in_base_segment_still_expands_and_navigates(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps({"host": "db-1"})}
        value = SecretsManagerResolver().resolve(f"{self.KEY}.host")
        assert value == "db-1"
        mock_client.get_secret_value.assert_called_once_with(SecretId=self.EXPANDED)

    def test_fetch_error_message_carries_aws_error_code_not_expanded_path(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        cause = BotocoreClientError(
            f"An error occurred: {self.EXPANDED}",
            {"Error": {"Code": "AccessDeniedException", "Message": self.EXPANDED}},
        )
        mock_client.get_secret_value.side_effect = cause
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(self.KEY)
        text = str(info.value)
        assert "AccessDeniedException" in text
        assert self.EXPANDED not in text
        assert "super-secret" not in text
        assert info.value.__cause__ is cause

    def test_fetch_error_message_falls_back_to_type_name(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.side_effect = TimeoutError("boom")
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(self.KEY)
        assert "TimeoutError" in str(info.value)

    def test_invalid_json_error_carries_unexpanded_key_without_cause(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "not-json-at-all"}
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(f"{self.KEY}.host")
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text
        assert "not-json-at-all" not in text
        assert info.value.__cause__ is None

    def test_missing_json_key_error_carries_unexpanded_key_without_cause(
        self, my_env: None, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps({"host": "db"})}
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(f"{self.KEY}.missing_key")
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverErrors:
    def test_raises_config_error_on_api_exception(
        self, mock_client: MagicMock, island: FakeAwsIsland
    ) -> None:
        mock_client.get_secret_value.side_effect = Exception("AccessDenied")
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/missing/secret")

    def test_raises_config_error_on_empty_key(self, island: FakeAwsIsland) -> None:
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("")
        assert island.built == []

    def test_raises_config_error_when_boto3_not_installed(self, absent_boto3: None) -> None:
        resolver = SecretsManagerResolver()
        with pytest.raises(ConfigError, match="boto3 is required"):
            resolver.resolve("/some/secret")


class TestSecretsManagerResolverOnMoto:
    """The real request shape: secrets stored in moto, read through a real client."""

    def test_a_string_secret_is_read(self, mocked_aws: None) -> None:
        moto_client("secretsmanager").create_secret(Name="/prod/token", SecretString="mysecret")

        assert SecretsManagerResolver().resolve("/prod/token") == "mysecret"

    def test_a_json_key_of_an_expanded_path_is_navigated(
        self, mocked_aws: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        moto_client("secretsmanager").create_secret(
            Name="/app/prod/db", SecretString=json.dumps({"host": "prod-db"})
        )

        assert SecretsManagerResolver().resolve("/app/%ENV%/db.host") == "prod-db"

    def test_a_binary_secret_is_refused(self, mocked_aws: None) -> None:
        moto_client("secretsmanager").create_secret(Name="/prod/token", SecretBinary=b"bytes")

        with pytest.raises(ConfigError, match="binary"):
            SecretsManagerResolver().resolve("/prod/token")

    def test_the_region_of_the_resolver_selects_the_secret_store(self, mocked_aws: None) -> None:
        moto_client("secretsmanager", OTHER_REGION).create_secret(
            Name="/some/secret", SecretString="in-ireland"
        )

        assert SecretsManagerResolver(OTHER_REGION).resolve("/some/secret") == "in-ireland"

    def test_a_missing_secret_fails_with_the_aws_error_code(self, mocked_aws: None) -> None:
        with pytest.raises(ConfigError, match="ResourceNotFoundException"):
            SecretsManagerResolver().resolve("/missing/secret")
