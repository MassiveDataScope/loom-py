"""Unit tests for SsmResolver.

Parameters live in moto's SSM; a fake AWS island counts the clients built and
scripts the failures moto cannot produce.
The library is assumed to be installed in dev — no library-presence tests.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver
from loom.core.config.ssm import SsmResolver
from tests.helpers.extras import FakeAwsIsland, fake_aws_island

KEY = "/app/%MY_ENV%/db"
EXPANDED = "/app/super-secret/db"


def _put(ssm: Any, name: str, value: str = "secret123") -> None:
    ssm.put_parameter(Name=name, Value=value, Type="String")


def _failing_island(monkeypatch: pytest.MonkeyPatch, failure: Exception) -> FakeAwsIsland:
    def raise_on_get(client: MagicMock) -> None:
        client.get_parameter.side_effect = failure

    return fake_aws_island(monkeypatch, setup=raise_on_get)


class TestSsmResolverIdentity:
    def test_name_returns_ssm(self) -> None:
        assert SsmResolver().name == "ssm"

    def test_implements_config_resolver_protocol(self) -> None:
        assert isinstance(SsmResolver(), ConfigResolver)


class TestSsmResolverResolve:
    def test_returns_parameter_value(self, ssm: Any) -> None:
        _put(ssm, "/prod/token")
        assert SsmResolver().resolve("/prod/token") == "secret123"

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("/prod/token", id="calls_ssm_with_literal_key"),
            pytest.param("/prod/plain/key", id="no_braces_passes_key_unchanged"),
        ],
    )
    def test_passes_the_key_unchanged(self, key: str, ssm: Any) -> None:
        _put(ssm, key)
        assert SsmResolver().resolve(key) == "secret123"

    def test_with_decryption_false(self, ssm: Any) -> None:
        ssm.put_parameter(Name="/prod/token", Value="secret123", Type="SecureString")
        assert SsmResolver().resolve("/prod/token") == "secret123"
        assert SsmResolver(with_decryption=False).resolve("/prod/token") != "secret123"


class TestSsmResolverEnvVarExpansion:
    def test_expands_single_env_var(self, ssm: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        _put(ssm, "/myapp/prod/token")
        assert SsmResolver().resolve("/myapp/%ENVIRONMENT%/token") == "secret123"

    def test_expands_multiple_env_vars(self, ssm: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("REGION", "eu-west-1")
        monkeypatch.setenv("ENV", "prod")
        _put(ssm, "/eu-west-1/prod/key")
        assert SsmResolver().resolve("/%REGION%/%ENV%/key") == "secret123"

    def test_raises_on_missing_env_var(
        self, mocked_aws: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        resolver = SsmResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/path/%MISSING_VAR%/token")

    def test_percent_syntax_parses_from_omegaconf_yaml(
        self, ssm: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: {VAR} syntax broke OmegaConf ANTLR grammar; %VAR% must not."""
        from omegaconf import OmegaConf

        monkeypatch.setenv("ENV", "prod")
        _put(ssm, "/app/prod/token")
        resolver = SsmResolver()
        OmegaConf.register_new_resolver("ssm", resolver.resolve, replace=True)
        cfg = OmegaConf.create({"token": "${ssm:/app/%ENV%/token}"})
        assert cfg.token == "secret123"


class TestSsmResolverClientLifecycle:
    def test_client_created_once_across_multiple_resolves(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = fake_aws_island(monkeypatch)
        resolver = SsmResolver()
        resolver.resolve("/key/one")
        resolver.resolve("/key/two")
        assert [service for service, _ in island.built] == ["ssm"]

    def test_region_forwarded_to_boto3(self, mocked_aws: None) -> None:
        boto3.client("ssm", region_name="eu-west-1").put_parameter(
            Name="/some/key", Value="in-ireland", Type="String"
        )
        assert SsmResolver("eu-west-1").resolve("/some/key") == "in-ireland"

    def test_region_none_by_default(self, ssm: Any) -> None:
        _put(ssm, "/some/key", "in-default-region")
        assert SsmResolver().resolve("/some/key") == "in-default-region"


class TestSsmResolverErrors:
    def test_raises_config_error_on_ssm_exception(self, mocked_aws: None) -> None:
        resolver = SsmResolver()
        with pytest.raises(ConfigError, match="ParameterNotFound"):
            resolver.resolve("/missing/param")


class TestSsmResolverDotNotation:
    def test_plain_path_returns_string(self, ssm: Any) -> None:
        _put(ssm, "/prod/token")
        assert SsmResolver().resolve("/prod/token") == "secret123"

    def test_single_key_navigation(self, ssm: Any) -> None:
        _put(ssm, "/prod/db_config", '{"host": "mydb.internal", "port": 5432}')
        assert SsmResolver().resolve("/prod/db_config.host") == "mydb.internal"

    def test_nested_key_navigation(self, ssm: Any) -> None:
        _put(ssm, "/prod/db", '{"connection": {"host": "db.internal"}}')
        assert SsmResolver().resolve("/prod/db.connection.host") == "db.internal"

    @pytest.mark.parametrize(
        ("value", "key"),
        [
            pytest.param(
                "not-json-at-all", "/prod/db.host", id="raises_config_error_on_invalid_json"
            ),
            pytest.param(
                '{"host": "db"}', "/prod/db.missing_key", id="raises_config_error_on_missing_key"
            ),
        ],
    )
    def test_raises_config_error_on_unusable_json(self, value: str, key: str, ssm: Any) -> None:
        _put(ssm, "/prod/db", value)
        resolver = SsmResolver()
        with pytest.raises(ConfigError):
            resolver.resolve(key)

    def test_env_var_expansion_then_dot_navigation(
        self, ssm: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        _put(ssm, "/myapp/prod/db_config", '{"host": "prod-db"}')
        assert SsmResolver().resolve("/myapp/%ENV%/db_config.host") == "prod-db"


class TestSsmResolverLogging:
    def test_info_log_emitted_with_ssm_path(
        self, ssm: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        _put(ssm, "/prod/db_config", '{"host": "db.internal"}')
        with caplog.at_level(logging.INFO, logger="loom.core.config.ssm"):
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
        self, ssm: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("STAGE", "prod")
        _put(ssm, "/app/prod/db", "some-value")
        with caplog.at_level(logging.INFO, logger="loom.core.config.ssm"):
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

    def test_log_carries_unexpanded_key_and_client_gets_expanded_path(
        self, my_env: None, ssm: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        _put(ssm, EXPANDED)
        with caplog.at_level(logging.INFO, logger="loom.core.config.ssm"):
            assert SsmResolver().resolve(KEY) == "secret123"
        messages = [r.message for r in caplog.records]
        assert all("super-secret" not in msg for msg in messages), messages
        assert any(KEY in msg for msg in messages), messages

    def test_fetch_error_carries_unexpanded_key_and_chains_cause(
        self, my_env: None, mocked_aws: None
    ) -> None:
        resolver = SsmResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is not None

    def test_dotted_env_value_in_last_segment_is_fetched_whole(
        self, ssm: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Splitting before expansion keeps a dotted env value out of navigation."""
        monkeypatch.setenv("DOTTED_ENV", "db.prod-secret")
        _put(ssm, "/app/svc/db.prod-secret", '{"host": "db"}')
        with caplog.at_level(logging.INFO, logger="loom.core.config.ssm"):
            value = SsmResolver().resolve("/app/svc/%DOTTED_ENV%")
        assert value == '{"host": "db"}'
        messages = [r.message for r in caplog.records]
        assert all("prod-secret" not in msg for msg in messages), messages

    def test_env_token_in_base_segment_still_expands_and_navigates(
        self, my_env: None, ssm: Any
    ) -> None:
        _put(ssm, EXPANDED, '{"host": "db-1"}')
        assert SsmResolver().resolve(f"{KEY}.host") == "db-1"

    def test_fetch_error_message_carries_aws_error_code_not_expanded_path(
        self, my_env: None, mocked_aws: None
    ) -> None:
        resolver = SsmResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        text = str(info.value)
        assert "ParameterNotFound" in text
        assert EXPANDED not in text
        assert "super-secret" not in text

    def test_fetch_error_message_falls_back_to_type_name(
        self, my_env: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _failing_island(monkeypatch, TimeoutError("boom"))
        resolver = SsmResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(KEY)
        assert "TimeoutError" in str(info.value)

    def test_invalid_json_error_carries_unexpanded_key_without_cause(
        self, my_env: None, ssm: Any
    ) -> None:
        _put(ssm, EXPANDED, "not-json-at-all")
        resolver = SsmResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(f"{KEY}.host")
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text
        assert "not-json-at-all" not in text
        assert info.value.__cause__ is None

    def test_missing_json_key_error_carries_unexpanded_key_without_cause(
        self, my_env: None, ssm: Any
    ) -> None:
        _put(ssm, EXPANDED, '{"host": "db"}')
        resolver = SsmResolver()
        with pytest.raises(ConfigError) as info:
            resolver.resolve(f"{KEY}.missing_key")
        text = str(info.value)
        assert KEY in text
        assert "super-secret" not in text
        assert info.value.__cause__ is None
