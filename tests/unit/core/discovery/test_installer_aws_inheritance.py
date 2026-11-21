"""A section's ``AwsClientConfig`` fields take what they do not set from ``aws:``."""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

from loom.core.config import ConfigContext, ConfigError, ConfigResolver
from loom.core.connections.aws import AwsClientConfig
from loom.core.di.installer import ConfiguredInstaller


class _Sessions(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    table: str
    dynamodb: AwsClientConfig


class _Settings(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    sessions: _Sessions
    audit: AwsClientConfig | None = None
    shards: list[AwsClientConfig] = msgspec.field(default_factory=list)


class _SettingsInstaller(ConfiguredInstaller[_Settings]):
    section = "trachinus"
    settings_type = _Settings

    def install(self, container: object) -> None:
        del container


class _ExplodingResolver:
    name = "vault"

    def resolve(self, key: str) -> object:
        raise AssertionError(f"check resolved {key}")


def _context(tmp_path: Path, yaml: str, *resolvers: ConfigResolver) -> ConfigContext:
    path = tmp_path / "app.yaml"
    path.write_text(yaml)
    return ConfigContext.from_yaml(str(path), resolvers=list(resolvers))


_AWS = "aws:\n  region: eu-west-1\n  url: http://localstack:4566\n  max_attempts: 5\n"
_SESSIONS = "trachinus:\n  sessions:\n    table: sessions\n    dynamodb:\n"


class TestInstallerInheritsAws:
    def test_a_nested_field_takes_the_region_and_url_of_the_aws_section(
        self, tmp_path: Path
    ) -> None:
        config = _context(tmp_path, f"{_AWS}{_SESSIONS}      pool:\n        size: 4\n")

        dynamodb = _SettingsInstaller.from_config(config).settings.sessions.dynamodb

        assert (dynamodb.region, dynamodb.url, dynamodb.max_attempts, dynamodb.pool.size) == (
            "eu-west-1",
            "http://localstack:4566",
            5,
            4,
        )

    def test_a_value_the_field_sets_wins_over_the_aws_section(self, tmp_path: Path) -> None:
        config = _context(tmp_path, f"{_AWS}{_SESSIONS}      region: us-east-2\n")

        dynamodb = _SettingsInstaller.from_config(config).settings.sessions.dynamodb

        assert (dynamodb.region, dynamodb.url) == ("us-east-2", "http://localstack:4566")

    def test_an_optional_field_and_the_items_of_a_list_inherit_too(self, tmp_path: Path) -> None:
        config = _context(
            tmp_path,
            f"{_AWS}{_SESSIONS}      {{}}\n"
            "  audit: {}\n"
            "  shards:\n    - region: ap-south-1\n    - {}\n",
        )

        settings = _SettingsInstaller.from_config(config).settings

        assert settings.audit is not None
        assert settings.audit.region == "eu-west-1"
        assert [shard.region for shard in settings.shards] == ["ap-south-1", "eu-west-1"]

    def test_an_absent_optional_field_stays_absent(self, tmp_path: Path) -> None:
        config = _context(tmp_path, f"{_AWS}{_SESSIONS}      {{}}\n")

        assert _SettingsInstaller.from_config(config).settings.audit is None

    def test_without_an_aws_section_the_field_keeps_its_own_values(self, tmp_path: Path) -> None:
        config = _context(tmp_path, f"{_SESSIONS}      region: us-east-2\n")

        dynamodb = _SettingsInstaller.from_config(config).settings.sessions.dynamodb

        assert (dynamodb.region, dynamodb.url) == ("us-east-2", None)

    def test_the_aws_section_is_checked_for_keys_no_client_takes(self, tmp_path: Path) -> None:
        config = _context(tmp_path, f"aws:\n  regoin: eu-west-1\n{_SESSIONS}      {{}}\n")

        with pytest.raises(ConfigError, match=r"_SettingsInstaller.*takes no key 'regoin'"):
            _SettingsInstaller.from_config(config)


class TestCheckSeesTheInheritedValues:
    def test_check_accepts_the_inherited_values_it_judges(self, tmp_path: Path) -> None:
        config = _context(tmp_path, f"{_AWS}{_SESSIONS}      {{}}\n")

        _SettingsInstaller.check_config(config)

    def test_check_refuses_an_invalid_value_inherited_from_aws(self, tmp_path: Path) -> None:
        config = _context(
            tmp_path, f"aws:\n  region: eu-west-1\n  max_attempts: 0\n{_SESSIONS}      {{}}\n"
        )

        with pytest.raises(ConfigError, match=r"_SettingsInstaller.*max_attempts"):
            _SettingsInstaller.check_config(config)

    def test_check_resolves_no_placeholder_of_the_aws_section(self, tmp_path: Path) -> None:
        config = _context(
            tmp_path,
            f"aws:\n  region: ${{vault:region}}\n{_SESSIONS}      {{}}\n",
            _ExplodingResolver(),
        )

        _SettingsInstaller.check_config(config)
