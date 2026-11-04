"""The top-level ``aws:`` section: every AWS product inherits it, key by key."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.config import ConfigContext, ConfigError, SsmResolver, load_config
from loom.core.config.aws import aws_section
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.repository.dynamodb.backend import DynamoDBBackend
from loom.etl.runner.config_loader import _parse_sections
from loom.prefect.notify._sns import SnsNotifier
from tests.helpers.extras import AwsClientFactory, patched_aws_clients
from tests.helpers.persistence import build_backend

pytestmark = pytest.mark.timeout(10)

_AWS = {"region": "eu-west-3", "url": "http://localhost:4566", "read_timeout": 9}
_TOPIC = "arn:aws:sns:us-east-1:123456789012:alerts"


def _store(config: dict[str, Any]) -> AwsClientFactory:
    ctx = ConfigContext.from_dict(config)
    with patched_aws_clients() as clients:
        build_backend(DynamoDBBackend(), ctx, (), SERVER_PROFILE)
    return clients


def _ssm(tmp_path: Path, yaml: str, resolver: SsmResolver) -> AwsClientFactory:
    path = tmp_path / "config.yaml"
    path.write_text(yaml + "app:\n  token: ${ssm:/app/token}\n", encoding="utf-8")
    client = MagicMock()
    client.get_parameter.return_value = {"Parameter": {"Value": "v"}}
    with patched_aws_clients(client) as clients:
        ConfigContext(load_config(str(path), resolvers=[resolver])).section("app", dict)
    return clients


def test_no_aws_section_decodes_to_nothing() -> None:
    assert aws_section(None) == {}


@pytest.mark.parametrize("key", ["aws_access_key_id", "aws_secret_access_key", "credentials"])
def test_aws_refuses_credentials(key: str) -> None:
    written = {"region": "eu-west-1", key: "x"}
    with pytest.raises(ConfigError, match="'aws'"):
        aws_section(written)


def test_aws_names_the_replacement_of_a_retired_key() -> None:
    written = {"endpoint_url": "http://localhost:4566"}
    with pytest.raises(ConfigError, match=r"aws\.endpoint_url was removed; use aws\.url"):
        aws_section(written)


def test_a_dynamodb_store_inherits_aws() -> None:
    clients = _store({"aws": _AWS, "stores": {"default": {"backend": "dynamodb", "table": "t"}}})

    assert clients.calls == [("dynamodb", "eu-west-3")]
    assert clients.options[0]["endpoint_url"] == "http://localhost:4566"
    assert clients.options[0]["config"].read_timeout == 9


def test_a_dynamodb_store_overrides_aws_key_by_key() -> None:
    store = {"backend": "dynamodb", "table": "t", "region": "eu-west-1", "read_timeout": 3}
    clients = _store({"aws": _AWS, "stores": {"default": store}})

    assert clients.calls == [("dynamodb", "eu-west-1")]
    assert clients.options[0]["endpoint_url"] == "http://localhost:4566"
    assert clients.options[0]["config"].read_timeout == 3


def test_a_dynamodb_store_without_aws_still_needs_its_region() -> None:
    ctx = ConfigContext.from_dict({"stores": {"default": {"backend": "dynamodb", "table": "t"}}})
    backend = DynamoDBBackend()
    with pytest.raises(ConfigError, match="region is required"):
        build_backend(backend, ctx, (), SERVER_PROFILE)


def test_storage_dynamodb_inherits_aws_key_by_key() -> None:
    ctx = ConfigContext.from_dict({"aws": _AWS, "storage": {"dynamodb": {"read_timeout": 3}}})

    dynamodb = _parse_sections(ctx)[0].dynamodb

    assert dynamodb is not None
    assert (dynamodb.region, dynamodb.url, dynamodb.read_timeout) == (
        "eu-west-3",
        "http://localhost:4566",
        3,
    )


def test_aws_alone_enables_no_dynamodb_source() -> None:
    ctx = ConfigContext.from_dict({"aws": _AWS, "storage": {}})

    assert _parse_sections(ctx)[0].dynamodb is None


def test_the_ssm_resolver_reads_aws_as_written(tmp_path: Path) -> None:
    yaml = "aws:\n  region: eu-west-3\n  url: http://localhost:4566\n"

    clients = _ssm(tmp_path, yaml, SsmResolver())

    assert clients.calls == [("ssm", "eu-west-3")]
    assert clients.options[0]["endpoint_url"] == "http://localhost:4566"


def test_the_resolver_region_overrides_aws(tmp_path: Path) -> None:
    clients = _ssm(tmp_path, "aws:\n  region: eu-west-3\n", SsmResolver("eu-west-1"))

    assert clients.calls == [("ssm", "eu-west-1")]


def test_without_aws_the_resolver_lets_boto3_pick_the_region(tmp_path: Path) -> None:
    clients = _ssm(tmp_path, "", SsmResolver())

    assert clients.calls == [("ssm", None)]
    assert "endpoint_url" not in clients.options[0]


@pytest.mark.parametrize("key", ["region", "read_timeout"])
def test_the_resolver_refuses_a_placeholder_in_aws(tmp_path: Path, key: str) -> None:
    yaml = f"aws:\n  {key}: ${{oc.env:AWS_X}}\n"
    resolver = SsmResolver()
    with pytest.raises(ConfigError, match=rf"aws\.{key} is a placeholder"):
        _ssm(tmp_path, yaml, resolver)


def test_the_resolver_fetches_a_key_once_per_configuration(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("a: ${ssm:/k}\nb: ${ssm:/k}\n", encoding="utf-8")
    client = MagicMock()
    client.get_parameter.return_value = {"Parameter": {"Value": "v"}}
    ctx = ConfigContext(load_config(str(path), resolvers=[SsmResolver()]))
    with patched_aws_clients(client):
        ctx.section("a", str)
        ctx.section("b", str)

    client.get_parameter.assert_called_once()


def test_the_sns_topic_region_overrides_aws() -> None:
    with patched_aws_clients() as clients:
        SnsNotifier(topic_arn=_TOPIC, aws=_AWS)

    assert clients.calls == [("sns", "us-east-1")]
    assert clients.options[0]["endpoint_url"] == "http://localhost:4566"
