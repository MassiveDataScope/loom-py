"""``AwsClientConfig.from_config`` and ``client_key``, what decides which AWS client is built."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.connections.aws import AwsClientConfig, AwsPool
from loom.core.model.redaction import Dsn

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


class TestAwsClientConfigClientKey:
    def test_a_subclass_with_the_same_client_settings_has_the_same_key(self) -> None:
        class Product(AwsClientConfig, frozen=True, kw_only=True):
            table: str = "t"

        assert Product(table="a").client_key() == Product(table="b").client_key()
        assert Product().client_key() == AwsClientConfig().client_key()

    @pytest.mark.parametrize(
        "other",
        [
            AwsClientConfig(region="eu-west-1"),
            AwsClientConfig(url=Dsn("http://localhost:4566")),
            AwsClientConfig(profile="dev"),
            AwsClientConfig(pool=AwsPool(size=2)),
            AwsClientConfig(connect_timeout=9),
            AwsClientConfig(read_timeout=9),
            AwsClientConfig(max_attempts=9),
            AwsClientConfig(retry_mode="adaptive"),
        ],
    )
    def test_every_setting_that_changes_the_client_changes_the_key(
        self, other: AwsClientConfig
    ) -> None:
        assert other.client_key() != AwsClientConfig().client_key()
