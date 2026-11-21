"""The environment every test starts from: no ambient AWS setup, no proxy, no stray models."""

from __future__ import annotations

import os

from tests.helpers.aws import FAKE_KEY, build_moto_client


class TestBareEnvironment:
    def test_only_the_minimum_of_aws_variables_is_left(self) -> None:
        left = {name: value for name, value in os.environ.items() if name.startswith("AWS_")}

        assert set(left) == {
            "AWS_CONFIG_FILE",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_EC2_METADATA_DISABLED",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        }
        assert left["AWS_ACCESS_KEY_ID"] == FAKE_KEY

    def test_no_proxy_is_named_and_every_host_bypasses_it(self) -> None:
        proxies = {name for name in os.environ if name.lower().endswith("_proxy")}

        assert proxies == {"NO_PROXY", "no_proxy"}
        assert {os.environ[name] for name in proxies} == {"*"}

    def test_a_client_reads_only_the_models_botocore_ships(self) -> None:
        client = build_moto_client("dynamodb")

        assert client.meta.service_model.metadata["serviceFullName"] == "Amazon DynamoDB"
