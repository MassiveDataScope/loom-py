"""The real AWS island and the fake serving moto clients meet the same expectations."""

from __future__ import annotations

import importlib
from typing import Any

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from loom.core.connections.aws import AwsClientConfig
from loom.core.connections.aws.loader import AwsIsland
from tests.helpers.aws import build_moto_client
from tests.helpers.aws_island import FakeAwsIsland

_SETTINGS = AwsClientConfig(region="eu-west-1", read_timeout=7, max_attempts=2)


class _MotoAwsIsland(FakeAwsIsland):
    """The fake island handing out a moto client built from the settings of each build."""

    def sdk_errors(self) -> tuple[type[Exception], ...]:
        return (BotoCoreError, ClientError)

    def _new_client(self, service: str, config: Any) -> Any:
        return build_moto_client(service, config.region, config)


class AwsIslandConformance:
    """The expectations of an AWS island, checked through its public port."""

    def test_a_session_keeps_the_profile_it_was_opened_for(
        self, aws_island: AwsIsland, dev_profile: str
    ) -> None:
        assert aws_island.open_session(dev_profile).profile_name == dev_profile

    def test_a_client_carries_the_region_and_bounds_of_its_settings(
        self, aws_island: AwsIsland, dev_profile: str
    ) -> None:
        session = aws_island.open_session(dev_profile)

        client = aws_island.build_aws_client(session, "sqs", _SETTINGS)

        assert client.meta.region_name == "eu-west-1"
        assert client.meta.config.read_timeout == 7
        assert client.meta.config.retries["total_max_attempts"] == 2

    def test_the_sdk_errors_catch_what_a_client_raises(self, aws_island: AwsIsland) -> None:
        client = aws_island.build_aws_client(aws_island.open_session(None), "sqs", _SETTINGS)

        with pytest.raises(ClientError) as raised:
            client.get_queue_url(QueueName="missing")

        assert isinstance(raised.value, aws_island.sdk_errors())


class TestRealAwsIsland(AwsIslandConformance):
    @pytest.fixture
    def aws_island(self, mocked_aws: None) -> AwsIsland:
        island: AwsIsland = importlib.import_module("loom.core.connections.aws.client")  # type: ignore[assignment]
        return island


class TestFakeAwsIsland(AwsIslandConformance):
    @pytest.fixture
    def aws_island(self, mocked_aws: None, monkeypatch: pytest.MonkeyPatch) -> AwsIsland:
        return _MotoAwsIsland().install(monkeypatch)
