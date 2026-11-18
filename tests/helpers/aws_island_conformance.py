"""What every AWS island promises: the battery the real island and its double both pass.

A subclass provides the ``aws_island`` fixture, an island that serves moto's clients.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from loom.core.connections.aws import AwsClientConfig
from loom.core.connections.aws.loader import AwsIsland
from tests.helpers.aws import FAKE_KEY, REGION

_SETTINGS = AwsClientConfig(region="eu-west-1", read_timeout=7, max_attempts=2)


class AwsIslandConformance:
    """The expectations of an AWS island, checked through its public port."""

    @pytest.fixture(autouse=True)
    def _dev_profile(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        profiles = tmp_path / "config"
        profiles.write_text(
            f"[profile dev]\nregion = {REGION}\naws_access_key_id = {FAKE_KEY}\n"
            f"aws_secret_access_key = {FAKE_KEY}\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("AWS_CONFIG_FILE", str(profiles))

    def test_a_session_keeps_the_profile_it_was_opened_for(self, aws_island: AwsIsland) -> None:
        assert aws_island.open_session("dev").profile_name == "dev"

    def test_a_client_carries_the_region_and_bounds_of_its_settings(
        self, aws_island: AwsIsland
    ) -> None:
        client = aws_island.build_aws_client(aws_island.open_session("dev"), "sqs", _SETTINGS)

        assert client.meta.region_name == "eu-west-1"
        assert client.meta.config.read_timeout == 7
        assert client.meta.config.retries["total_max_attempts"] == 2

    def test_the_sdk_errors_catch_what_a_client_raises(self, aws_island: AwsIsland) -> None:
        client = aws_island.build_aws_client(aws_island.open_session(None), "sqs", _SETTINGS)

        with pytest.raises(ClientError) as raised:
            client.get_queue_url(QueueName="missing")

        assert isinstance(raised.value, aws_island.sdk_errors())
