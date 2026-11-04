"""``${secrets:...}`` and ``${ssm:...}`` resolve from the endpoint and region ``aws:`` names."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.resolver import with_default_resolvers
from tests.integration.aws.conftest import AwsClientFactory, ConfigWriter, unique

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]

_SECTION = "settings"


@dataclass(frozen=True)
class _Stored:
    placeholder: str
    expected: object
    delete: Callable[[], None]


def _secret(aws: AwsClientFactory, value: str, navigate: str, expected: object) -> _Stored:
    client = aws("secretsmanager")
    name = unique("loom-it/secret")
    client.create_secret(Name=name, SecretString=value)
    return _Stored(
        f"${{secrets:{name}{navigate}}}",
        expected,
        lambda: client.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True),
    )


def _parameter(aws: AwsClientFactory, kind: str) -> _Stored:
    client = aws("ssm")
    name = f"/{unique('loom-it')}/db-host"
    client.put_parameter(Name=name, Value="db.internal", Type=kind)
    return _Stored(f"${{ssm:{name}}}", "db.internal", lambda: client.delete_parameter(Name=name))


_STORES: dict[str, Callable[[AwsClientFactory], _Stored]] = {
    "secret string": lambda aws: _secret(aws, "s3cr3t", "", "s3cr3t"),
    "secret JSON key": lambda aws: _secret(aws, json.dumps({"password": "pw"}), ".password", "pw"),
    "ssm String": lambda aws: _parameter(aws, "String"),
    "ssm SecureString": lambda aws: _parameter(aws, "SecureString"),
}


@pytest.fixture(params=list(_STORES))
def stored(request: pytest.FixtureRequest, aws: AwsClientFactory) -> Iterator[_Stored]:
    value = _STORES[request.param](aws)
    yield value
    value.delete()


def _load(write_config: ConfigWriter, placeholder: str) -> ConfigContext:
    path = write_config({_SECTION: {"resolved": placeholder}})
    return ConfigContext.from_yaml(str(path), resolvers=with_default_resolvers())


def test_a_placeholder_resolves_from_the_emulator_aws_names(
    write_config: ConfigWriter, stored: _Stored
) -> None:
    config = _load(write_config, stored.placeholder)

    assert config.section(_SECTION, dict[str, object])["resolved"] == stored.expected


@pytest.mark.parametrize(
    ("placeholder", "code"),
    [
        ("${secrets:loom-it/never-created}", "ResourceNotFoundException"),
        ("${ssm:/loom-it/never-created}", "ParameterNotFound"),
    ],
    ids=["secret", "ssm"],
)
def test_a_missing_value_fails_naming_the_aws_error_code(
    write_config: ConfigWriter, placeholder: str, code: str
) -> None:
    config = _load(write_config, placeholder)

    with pytest.raises(ConfigError, match=code):
        config.section(_SECTION, dict[str, object])
