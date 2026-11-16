"""``AwsClientConfig.from_config`` and ``build_client``, the one way to an AWS client."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.connections.aws import AwsClientConfig
from loom.core.plugins.optional import MissingExtraError
from tests.helpers.extras import patched_aws_clients, without_boto3

_AWS = {"region": "eu-west-3", "url": "http://localhost:4566", "read_timeout": 9}


class TestAwsClientConfigFromConfig:
    @pytest.mark.parametrize(
        ("document", "own", "over", "region", "read_timeout"),
        [
            ({}, None, {}, None, 5),
            ({"aws": _AWS}, None, {}, "eu-west-3", 9),
            ({"aws": _AWS}, {"region": "eu-west-1"}, {}, "eu-west-1", 9),
            ({"aws": _AWS}, {"read_timeout": 3}, {"region": "us-east-1"}, "us-east-1", 3),
        ],
    )
    def test_the_product_section_wins_key_by_key_and_over_wins_last(
        self,
        document: dict[str, Any],
        own: dict[str, Any] | None,
        over: dict[str, Any],
        region: str | None,
        read_timeout: int,
    ) -> None:
        config = AwsClientConfig.from_config(
            ConfigContext.from_dict(document), "storage.dynamodb", own=own, **over
        )

        assert (config.region, config.read_timeout) == (region, read_timeout)

    @pytest.mark.parametrize(
        ("aws", "message"),
        [
            ({"table": "x"}, "'aws' takes no key 'table'"),
            ({"aws_secret_access_key": "x"}, "'aws' takes no key 'aws_secret_access_key'"),
            ({"endpoint_url": "http://x"}, r"aws\.endpoint_url was removed; use aws\.url"),
        ],
    )
    def test_the_aws_section_refuses_foreign_and_retired_keys(
        self, aws: dict[str, Any], message: str
    ) -> None:
        with pytest.raises(ConfigError, match=message):
            AwsClientConfig.from_config(ConfigContext.from_dict({"aws": aws}), "sqs")

    def test_the_decoded_type_is_the_class_it_is_called_on(self) -> None:
        class Product(AwsClientConfig, frozen=True, kw_only=True, forbid_unknown_fields=True):
            table: str = "t"

        config = Product.from_config(ConfigContext.from_dict({}), "p", own={"table": "orders"})

        assert (type(config), config.table) == (Product, "orders")


class TestAwsClientConfigBuildClient:
    @pytest.mark.parametrize("service", ["dynamodb", "sqs", "s3"])
    def test_the_client_is_built_for_the_boto3_service_in_the_region_of_the_settings(
        self, service: str
    ) -> None:
        with patched_aws_clients() as clients:
            AwsClientConfig(region="eu-west-1").build_client(service)

        clients.assert_called_once_with(service, region_name="eu-west-1")

    def test_a_missing_boto3_names_the_aws_extra(self, monkeypatch: pytest.MonkeyPatch) -> None:
        without_boto3(monkeypatch)

        with pytest.raises(MissingExtraError, match=r"needs the 'aws' extra"):
            AwsClientConfig().build_client("s3")

    def test_options_the_settings_decide_are_refused(self) -> None:
        with pytest.raises(ValueError, match="read_timeout"):
            AwsClientConfig().build_client("dynamodb", read_timeout=60)
