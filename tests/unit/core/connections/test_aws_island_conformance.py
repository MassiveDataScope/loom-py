"""The real AWS island and the fake serving moto clients meet the same expectations."""

from __future__ import annotations

import importlib
from typing import Any

import boto3
import pytest
from botocore.config import Config

from loom.core.connections.aws import AwsClientConfig
from loom.core.connections.aws.loader import AwsIsland
from loom.core.model.redaction import reveal_for_driver
from tests.helpers.aws import FAKE_KEY
from tests.helpers.aws_island import fake_aws_island
from tests.helpers.aws_island_conformance import AwsIslandConformance


def _build_moto_client(settings: AwsClientConfig) -> Any:
    options = reveal_for_driver(settings.client_options())
    return boto3.Session().client(
        "sqs",
        region_name=settings.region,
        config=Config(**options["config"]),
        aws_access_key_id=FAKE_KEY,
        aws_secret_access_key=FAKE_KEY,
    )


class TestRealAwsIsland(AwsIslandConformance):
    @pytest.fixture
    def aws_island(self, mocked_aws: None) -> AwsIsland:
        island: AwsIsland = importlib.import_module("loom.core.connections.aws.client")  # type: ignore[assignment]
        return island


class TestFakeAwsIsland(AwsIslandConformance):
    @pytest.fixture
    def aws_island(self, mocked_aws: None, monkeypatch: pytest.MonkeyPatch) -> AwsIsland:
        return fake_aws_island(monkeypatch, serving=_build_moto_client)
