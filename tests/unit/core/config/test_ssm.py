"""Unit tests for SsmResolver.

boto3 is mocked at the call level (boto3.client / client.get_parameter).
The library is assumed to be installed in dev — no library-presence tests.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver
from loom.core.config.ssm import SsmResolver
from tests.helpers.extras import patched_aws_clients


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_parameter.return_value = {"Parameter": {"Value": "secret123"}}
    return client


class _BotocoreShapedError(Exception):
    """Stand-in for ``botocore.exceptions.ClientError`` (not a test dep)."""

    def __init__(self, message: str, response: dict[str, object]) -> None:
        super().__init__(message)
        self.response = response


class TestSsmResolverIdentity:
    def test_name_returns_ssm(self) -> None:
        assert SsmResolver().name == "ssm"

    def test_implements_config_resolver_protocol(self) -> None:
        assert isinstance(SsmResolver(), ConfigResolver)


class TestSsmResolverResolve:
    def test_returns_parameter_value(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client):
            result = SsmResolver().resolve("/prod/token")
        assert result == "secret123"

    def test_calls_ssm_with_literal_key(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client):
            SsmResolver().resolve("/prod/token")
        mock_client.get_parameter.assert_called_once_with(Name="/prod/token", WithDecryption=True)

    def test_with_decryption_false(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client):
            SsmResolver(with_decryption=False).resolve("/prod/token")
        mock_client.get_parameter.assert_called_once_with(Name="/prod/token", WithDecryption=False)

    def test_no_braces_passes_key_unchanged(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client):
            SsmResolver().resolve("/prod/plain/key")
        mock_client.get_parameter.assert_called_once_with(
            Name="/prod/plain/key", WithDecryption=True
        )


class TestSsmResolverEnvVarExpansion:
    def test_expands_single_env_var(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        with patched_aws_clients(mock_client):
            SsmResolver().resolve("/myapp/%ENVIRONMENT%/token")
        mock_client.get_parameter.assert_called_once_with(
            Name="/myapp/prod/token", WithDecryption=True
        )

    def test_expands_multiple_env_vars(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REGION", "eu-west-1")
        monkeypatch.setenv("ENV", "prod")
        with patched_aws_clients(mock_client):
            SsmResolver().resolve("/%REGION%/%ENV%/key")
        mock_client.get_parameter.assert_called_once_with(
            Name="/eu-west-1/prod/key", WithDecryption=True
        )

    def test_raises_on_missing_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        resolver = SsmResolver()
        with patched_aws_clients(), pytest.raises(ConfigError):
            resolver.resolve("/path/%MISSING_VAR%/token")

    def test_percent_syntax_parses_from_omegaconf_yaml(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: {VAR} syntax broke OmegaConf ANTLR grammar; %VAR% must not."""
        from omegaconf import OmegaConf

        monkeypatch.setenv("ENV", "prod")
        resolver = SsmResolver()
        OmegaConf.register_new_resolver("ssm", resolver.resolve, replace=True)
        with patched_aws_clients(mock_client):
            cfg = OmegaConf.create({"token": "${ssm:/app/%ENV%/token}"})
            _ = cfg.token
        mock_client.get_parameter.assert_called_once_with(
            Name="/app/prod/token", WithDecryption=True
        )


class TestSsmResolverClientLifecycle:
    def test_client_created_once_across_multiple_resolves(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client) as mock_factory:
            resolver = SsmResolver()
            resolver.resolve("/key/one")
            resolver.resolve("/key/two")
        assert mock_factory.call_count == 1

    def test_region_forwarded_to_boto3(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client) as mock_factory:
            SsmResolver("eu-west-1").resolve("/some/key")
        mock_factory.assert_called_once_with("ssm", region_name="eu-west-1")

    def test_region_none_by_default(self, mock_client: MagicMock) -> None:
        with patched_aws_clients(mock_client) as mock_factory:
            SsmResolver().resolve("/some/key")
        mock_factory.assert_called_once_with("ssm", region_name=None)


class TestSsmResolverErrors:
    def test_raises_config_error_on_ssm_exception(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.side_effect = Exception("ParameterNotFound")
        resolver = SsmResolver()
        with patched_aws_clients(mock_client), pytest.raises(ConfigError):
            resolver.resolve("/missing/param")


class TestSsmResolverDotNotation:
    def test_plain_path_returns_string(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "secret123"}}
        with patched_aws_clients(mock_client):
            result = SsmResolver().resolve("/prod/token")
        assert result == "secret123"

    def test_single_key_navigation(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {
            "Parameter": {"Value": '{"host": "mydb.internal", "port": 5432}'}
        }
        with patched_aws_clients(mock_client):
            result = SsmResolver().resolve("/prod/db_config.host")
        assert result == "mydb.internal"
        mock_client.get_parameter.assert_called_once_with(
            Name="/prod/db_config", WithDecryption=True
        )

    def test_nested_key_navigation(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {
            "Parameter": {"Value": '{"connection": {"host": "db.internal"}}'}
        }
        with patched_aws_clients(mock_client):
            result = SsmResolver().resolve("/prod/db.connection.host")
        assert result == "db.internal"

    def test_raises_config_error_on_invalid_json(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "not-json-at-all"}}
        resolver = SsmResolver()
        with patched_aws_clients(mock_client), pytest.raises(ConfigError):
            resolver.resolve("/prod/db.host")

    def test_raises_config_error_on_missing_key(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db"}'}}
        resolver = SsmResolver()
        with patched_aws_clients(mock_client), pytest.raises(ConfigError):
            resolver.resolve("/prod/db.missing_key")

    def test_env_var_expansion_then_dot_navigation(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "prod-db"}'}}
        with patched_aws_clients(mock_client):
            result = SsmResolver().resolve("/myapp/%ENV%/db_config.host")
        assert result == "prod-db"
        mock_client.get_parameter.assert_called_once_with(
            Name="/myapp/prod/db_config", WithDecryption=True
        )


class TestSsmResolverLogging:
    def test_info_log_emitted_with_ssm_path(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db.internal"}'}}
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.ssm"),
            patched_aws_clients(mock_client),
        ):
            SsmResolver().resolve("/prod/db_config.host")
        messages = [r.message for r in caplog.records]
        assert any("/prod/db_config" in msg for msg in messages), (
            f"Expected a log record containing '/prod/db_config', got: {messages}"
        )
        secret_value = "db.internal"
        assert all(secret_value not in msg for msg in messages), (
            f"Log must not contain the secret value '{secret_value}', got: {messages}"
        )

    def test_log_uses_original_key_not_expanded_path(
        self,
        mock_client: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("STAGE", "prod")
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "some-value"}}
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.ssm"),
            patched_aws_clients(mock_client),
        ):
            SsmResolver().resolve("/app/%STAGE%/db")
        messages = [r.message for r in caplog.records]
        assert any("/app/%STAGE%/db" in msg for msg in messages), (
            f"Expected a log record containing '/app/%STAGE%/db', got: {messages}"
        )
        assert all("/app/prod/db" not in msg for msg in messages), (
            f"Log must not contain the expanded path '/app/prod/db', got: {messages}"
        )


class TestSsmResolverUnexpandedKey:
    """AC7: logs and ConfigError text carry the key as written, never the env value."""

    KEY = "/app/%MY_ENV%/db"
    EXPANDED = "/app/super-secret/db"

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_ENV", "super-secret")

    def test_log_carries_unexpanded_key_and_client_gets_expanded_path(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.ssm"),
            patched_aws_clients(mock_client),
        ):
            SsmResolver().resolve(self.KEY)
        messages = [r.message for r in caplog.records]
        assert all("super-secret" not in msg for msg in messages), messages
        assert any(self.KEY in msg for msg in messages), messages
        mock_client.get_parameter.assert_called_once_with(Name=self.EXPANDED, WithDecryption=True)

    def test_fetch_error_carries_unexpanded_key_and_chains_cause(
        self, mock_client: MagicMock
    ) -> None:
        cause = Exception(f"ParameterNotFound: {self.EXPANDED}")
        mock_client.get_parameter.side_effect = cause
        resolver = SsmResolver()
        with patched_aws_clients(mock_client), pytest.raises(ConfigError) as info:
            resolver.resolve(self.KEY)
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is cause

    def test_dotted_env_value_in_last_segment_is_fetched_whole(
        self,
        mock_client: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Splitting before expansion keeps a dotted env value out of navigation."""
        monkeypatch.setenv("DOTTED_ENV", "db.prod-secret")
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db"}'}}
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.ssm"),
            patched_aws_clients(mock_client),
        ):
            value = SsmResolver().resolve("/app/svc/%DOTTED_ENV%")
        assert value == '{"host": "db"}'
        mock_client.get_parameter.assert_called_once_with(
            Name="/app/svc/db.prod-secret", WithDecryption=True
        )
        messages = [r.message for r in caplog.records]
        assert all("prod-secret" not in msg for msg in messages), messages

    def test_env_token_in_base_segment_still_expands_and_navigates(
        self, mock_client: MagicMock
    ) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db-1"}'}}
        with patched_aws_clients(mock_client):
            value = SsmResolver().resolve(f"{self.KEY}.host")
        assert value == "db-1"
        mock_client.get_parameter.assert_called_once_with(Name=self.EXPANDED, WithDecryption=True)

    def test_fetch_error_message_carries_aws_error_code_not_expanded_path(
        self, mock_client: MagicMock
    ) -> None:
        cause = _BotocoreShapedError(
            f"An error occurred: {self.EXPANDED}",
            {"Error": {"Code": "AccessDeniedException", "Message": self.EXPANDED}},
        )
        mock_client.get_parameter.side_effect = cause
        with patched_aws_clients(mock_client):
            resolver = SsmResolver()
            with pytest.raises(ConfigError) as info:
                resolver.resolve(self.KEY)
        text = str(info.value)
        assert "AccessDeniedException" in text
        assert self.EXPANDED not in text
        assert "super-secret" not in text
        assert info.value.__cause__ is cause

    def test_fetch_error_message_falls_back_to_type_name(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.side_effect = TimeoutError("boom")
        with patched_aws_clients(mock_client):
            resolver = SsmResolver()
            with pytest.raises(ConfigError) as info:
                resolver.resolve(self.KEY)
        assert "TimeoutError" in str(info.value)

    def test_invalid_json_error_carries_unexpanded_key_without_cause(
        self, mock_client: MagicMock
    ) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "not-json-at-all"}}
        resolver = SsmResolver()
        with patched_aws_clients(mock_client), pytest.raises(ConfigError) as info:
            resolver.resolve(f"{self.KEY}.host")
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text
        assert "not-json-at-all" not in text
        assert info.value.__cause__ is None

    def test_missing_json_key_error_carries_unexpanded_key_without_cause(
        self, mock_client: MagicMock
    ) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db"}'}}
        resolver = SsmResolver()
        with patched_aws_clients(mock_client), pytest.raises(ConfigError) as info:
            resolver.resolve(f"{self.KEY}.missing_key")
        text = str(info.value)
        assert self.KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is None
