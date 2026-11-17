"""``DynamoDBBackend``: the readiness probe over the configured table."""

from __future__ import annotations

import logging

import pytest

import loom.stores.dynamodb.backend as backend_module
from loom.core.config import ConfigContext
from loom.core.connections.aws import AwsClients
from loom.core.stores import StoreWiring
from loom.core.stores.profile import SERVER_PROFILE
from loom.stores.dynamodb.backend import DynamoDBBackend
from tests.helpers.extras import FakeAwsIsland, fake_aws_island
from tests.helpers.persistence import build_backend

from .conftest import FakeClient


def _wiring(monkeypatch: pytest.MonkeyPatch, fake_client: FakeClient, table: str) -> StoreWiring:
    monkeypatch.setattr(AwsClients, "client", lambda _self, _service, _config=None: fake_client)
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "stores": {
                "default": {"backend": "dynamodb", "region": "eu-west-1", "table": table},
            },
        }
    )
    return build_backend(DynamoDBBackend(), ctx, (), SERVER_PROFILE)


class TestDynamoReadiness:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "expected"),
        [("ACTIVE", True), ("UPDATING", True), ("CREATING", False), ("DELETING", False)],
    )
    async def test_readiness_reflects_the_table_status(
        self, monkeypatch: pytest.MonkeyPatch, fake_client: FakeClient, status: str, expected: bool
    ) -> None:
        fake_client.table_status["products"] = status
        wiring = _wiring(monkeypatch, fake_client, "products")

        assert wiring.readiness is not None
        assert await wiring.readiness() is expected

    @pytest.mark.asyncio
    async def test_readiness_is_false_when_the_table_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fake_client: FakeClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        wiring = _wiring(monkeypatch, fake_client, "missing")

        assert wiring.readiness is not None
        with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
            assert await wiring.readiness() is False

        assert "readiness" in caplog.text
        assert "missing" in caplog.text


class TestDynamoClientsPerStore:
    @pytest.fixture
    def island(self, monkeypatch: pytest.MonkeyPatch) -> FakeAwsIsland:
        return fake_aws_island(monkeypatch)

    @staticmethod
    def _build_stores(aws: AwsClients, stores: dict[str, dict[str, str]]) -> None:
        ctx = ConfigContext.from_dict(
            {
                "app": {"name": "demo"},
                "stores": {
                    name: {"backend": "dynamodb", "table": "t", **settings}
                    for name, settings in stores.items()
                },
            }
        )
        for name in stores:
            build_backend(DynamoDBBackend(), ctx, (), SERVER_PROFILE, store=name, aws=aws)

    def test_stores_with_different_settings_get_different_clients(
        self, island: FakeAwsIsland
    ) -> None:
        aws = AwsClients()

        self._build_stores(
            aws,
            {"orders": {"region": "eu-west-1"}, "audit": {"region": "us-east-1"}},
        )

        assert [config.region for _service, config in island.built] == ["eu-west-1", "us-east-1"]
        assert len(island.built) == 2

    def test_stores_with_the_same_settings_share_one_client(self, island: FakeAwsIsland) -> None:
        aws = AwsClients()

        self._build_stores(
            aws,
            {"orders": {"region": "eu-west-1"}, "audit": {"region": "eu-west-1", "table": "u"}},
        )

        assert len(island.built) == 1
