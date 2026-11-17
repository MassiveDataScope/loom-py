"""The top-level ``aws:`` section: every AWS product inherits it, key by key."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.config import ConfigContext, ConfigError, SsmResolver, load_config
from loom.core.connections.aws import AwsClientConfig
from loom.core.model.redaction import reveal_for_driver
from loom.core.stores.profile import SERVER_PROFILE
from loom.etl.runner.config_loader import _parse_sections
from loom.prefect.notify._sns import SnsNotifier
from loom.stores.dynamodb.backend import DynamoDBBackend
from tests.helpers.extras import FakeAwsIsland, fake_aws_island
from tests.helpers.persistence import build_backend

pytestmark = pytest.mark.timeout(10)

_AWS = {"region": "eu-west-3", "url": "http://localhost:4566", "read_timeout": 9}
_TOPIC = "arn:aws:sns:us-east-1:123456789012:alerts"


def _answer_with_a_parameter(client: MagicMock) -> None:
    client.get_parameter.return_value = {"Parameter": {"Value": "v"}}


class _Built:
    """What loom asked the AWS island for: the service, region and client options of each build."""

    def __init__(self, island: FakeAwsIsland) -> None:
        self.calls = [(service, config.region) for service, config in island.built]
        self.options = [reveal_for_driver(config.client_options()) for _, config in island.built]


def _store(config: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> _Built:
    island = fake_aws_island(monkeypatch)
    build_backend(DynamoDBBackend(), ConfigContext.from_dict(config), (), SERVER_PROFILE)
    return _Built(island)


def _ssm(
    tmp_path: Path, yaml: str, resolver: SsmResolver, monkeypatch: pytest.MonkeyPatch
) -> _Built:
    island = fake_aws_island(monkeypatch, setup=_answer_with_a_parameter)
    path = tmp_path / "config.yaml"
    path.write_text(yaml + "app:\n  token: ${ssm:/app/token}\n", encoding="utf-8")
    ConfigContext(load_config(str(path), resolvers=[resolver])).section("app", dict)
    return _Built(island)


def _inherit(aws: dict[str, Any] | None) -> dict[str, Any]:
    ctx = ConfigContext.from_dict({} if aws is None else {"aws": aws})
    return AwsClientConfig.inherit(ctx, {"read_timeout": 3}, "storage.dynamodb")


class TestAwsSection:
    def test_no_aws_section_adds_nothing(self) -> None:
        assert _inherit(None) == {"read_timeout": 3}

    @pytest.mark.parametrize(
        "key", ["aws_access_key_id", "aws_secret_access_key", "credentials", "table", "queues"]
    )
    def test_aws_refuses_credentials_and_product_keys(self, key: str) -> None:
        with pytest.raises(ConfigError, match=f"'aws' takes no key '{key}'"):
            _inherit({"region": "eu-west-1", key: "x"})

    def test_aws_names_the_replacement_of_a_retired_key(self) -> None:
        written = {"endpoint_url": "http://localhost:4566"}
        with pytest.raises(ConfigError, match=r"aws\.endpoint_url was removed; use aws\.url"):
            _inherit(written)


class TestDynamoDbAwsInheritance:
    def test_a_dynamodb_store_inherits_aws(self, monkeypatch: pytest.MonkeyPatch) -> None:
        clients = _store(
            {"aws": _AWS, "stores": {"default": {"backend": "dynamodb", "table": "t"}}},
            monkeypatch,
        )

        assert clients.calls == [("dynamodb", "eu-west-3")]
        assert clients.options[0]["endpoint_url"] == "http://localhost:4566"
        assert clients.options[0]["config"]["read_timeout"] == 9

    def test_a_dynamodb_store_overrides_aws_key_by_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = {"backend": "dynamodb", "table": "t", "region": "eu-west-1", "read_timeout": 3}
        clients = _store({"aws": _AWS, "stores": {"default": store}}, monkeypatch)

        assert clients.calls == [("dynamodb", "eu-west-1")]
        assert clients.options[0]["endpoint_url"] == "http://localhost:4566"
        assert clients.options[0]["config"]["read_timeout"] == 3

    def test_a_dynamodb_store_without_aws_still_needs_its_region(self) -> None:
        ctx = ConfigContext.from_dict(
            {"stores": {"default": {"backend": "dynamodb", "table": "t"}}}
        )
        backend = DynamoDBBackend()
        with pytest.raises(ConfigError, match="region is required"):
            build_backend(backend, ctx, (), SERVER_PROFILE)

    def test_storage_dynamodb_inherits_aws_key_by_key(self) -> None:
        ctx = ConfigContext.from_dict({"aws": _AWS, "storage": {"dynamodb": {"read_timeout": 3}}})

        dynamodb = _parse_sections(ctx)[0].dynamodb

        assert dynamodb is not None
        assert (dynamodb.region, dynamodb.url, dynamodb.read_timeout) == (
            "eu-west-3",
            "http://localhost:4566",
            3,
        )

    def test_aws_alone_enables_no_dynamodb_source(self) -> None:
        ctx = ConfigContext.from_dict({"aws": _AWS, "storage": {}})

        assert _parse_sections(ctx)[0].dynamodb is None


class TestResolversReadAws:
    def test_the_ssm_resolver_reads_aws_as_written(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        yaml = "aws:\n  region: eu-west-3\n  url: http://localhost:4566\n"

        clients = _ssm(tmp_path, yaml, SsmResolver(), monkeypatch)

        assert clients.calls == [("ssm", "eu-west-3")]
        assert clients.options[0]["endpoint_url"] == "http://localhost:4566"

    def test_the_resolver_region_overrides_aws(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clients = _ssm(
            tmp_path, "aws:\n  region: eu-west-3\n", SsmResolver("eu-west-1"), monkeypatch
        )

        assert clients.calls == [("ssm", "eu-west-1")]

    def test_without_aws_the_resolver_lets_boto3_pick_the_region(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clients = _ssm(tmp_path, "", SsmResolver(), monkeypatch)

        assert clients.calls == [("ssm", None)]
        assert "endpoint_url" not in clients.options[0]

    @pytest.mark.parametrize("key", ["region", "read_timeout"])
    def test_the_resolver_refuses_a_placeholder_in_aws(
        self, tmp_path: Path, key: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        yaml = f"aws:\n  {key}: ${{oc.env:AWS_X}}\n"
        resolver = SsmResolver()
        with pytest.raises(ConfigError, match=rf"aws\.{key} is a placeholder"):
            _ssm(tmp_path, yaml, resolver, monkeypatch)

    def test_the_resolver_fetches_a_key_once_per_configuration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = fake_aws_island(monkeypatch, setup=_answer_with_a_parameter)
        path = tmp_path / "config.yaml"
        path.write_text("a: ${ssm:/k}\nb: ${ssm:/k}\n", encoding="utf-8")
        ctx = ConfigContext(load_config(str(path), resolvers=[SsmResolver()]))
        ctx.section("a", str)
        ctx.section("b", str)

        island.created[0].get_parameter.assert_called_once()

    def test_the_sns_topic_region_overrides_aws(self, monkeypatch: pytest.MonkeyPatch) -> None:
        island = fake_aws_island(monkeypatch)
        SnsNotifier(topic_arn=_TOPIC, aws=_AWS)
        clients = _Built(island)

        assert clients.calls == [("sns", "us-east-1")]
        assert clients.options[0]["endpoint_url"] == "http://localhost:4566"

    def test_one_resolver_builds_a_client_per_configuration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        resolver = SsmResolver()
        first = tmp_path / "first.yaml"
        second = tmp_path / "second.yaml"
        first.write_text("aws:\n  region: eu-west-1\na: ${ssm:/k}\n", encoding="utf-8")
        second.write_text(
            "aws:\n  region: us-east-1\n  url: http://localhost:4566\na: ${ssm:/k}\n",
            encoding="utf-8",
        )
        island = fake_aws_island(monkeypatch, setup=_answer_with_a_parameter)
        for path in (first, second):
            ConfigContext(load_config(str(path), resolvers=[resolver])).section("a", str)
        clients = _Built(island)

        assert clients.calls == [("ssm", "eu-west-1"), ("ssm", "us-east-1")]
        assert "endpoint_url" not in clients.options[0]
        assert clients.options[1]["endpoint_url"] == "http://localhost:4566"
