"""``DynamoDBBackend``: the readiness probe over the configured table."""

from __future__ import annotations

import logging
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

import loom.stores.dynamodb.backend as backend_module
from loom.core.config import ConfigContext
from loom.core.connections.aws import AwsClients
from loom.core.stores import StoreWiring
from loom.core.stores.profile import SERVER_PROFILE
from loom.stores.dynamodb.backend import DynamoDBBackend
from tests.helpers.aws import REGION
from tests.helpers.aws_island import FakeAwsIsland
from tests.helpers.dynamodb import MotoTable
from tests.helpers.persistence import build_backend


def _wiring(table: str) -> StoreWiring:
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "stores": {
                "default": {"backend": "dynamodb", "region": REGION, "table": table},
            },
        }
    )
    return build_backend(DynamoDBBackend(), ctx, (), SERVER_PROFILE)


def _describing(status: str) -> Callable[[MagicMock], None]:
    def setup(client: MagicMock) -> None:
        client.describe_table.return_value = {"Table": {"TableStatus": status}}

    return setup


class TestDynamoReadiness:
    @pytest.mark.asyncio
    async def test_readiness_is_true_for_an_existing_table(self, products_table: MotoTable) -> None:
        wiring = _wiring("products")

        assert wiring.readiness is not None
        assert await wiring.readiness() is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "expected"),
        [("ACTIVE", True), ("UPDATING", True), ("CREATING", False), ("DELETING", False)],
    )
    async def test_readiness_reflects_the_table_status(
        self, monkeypatch: pytest.MonkeyPatch, status: str, expected: bool
    ) -> None:
        FakeAwsIsland(setup=_describing(status)).install(monkeypatch)
        wiring = _wiring("products")

        assert wiring.readiness is not None
        assert await wiring.readiness() is expected

    @pytest.mark.asyncio
    async def test_readiness_is_false_when_the_table_is_missing(
        self, caplog: pytest.LogCaptureFixture, products_table: MotoTable
    ) -> None:
        wiring = _wiring("missing")

        assert wiring.readiness is not None
        with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
            assert await wiring.readiness() is False

        assert "readiness" in caplog.text
        assert "missing" in caplog.text


class TestDynamoClientsPerStore:
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
            {"orders": {"region": "eu-west-1"}, "audit": {"region": REGION}},
        )

        assert [config.region for _service, config in island.built] == ["eu-west-1", REGION]
        assert len(island.built) == 2

    def test_stores_with_the_same_settings_share_one_client(self, island: FakeAwsIsland) -> None:
        aws = AwsClients()

        self._build_stores(
            aws,
            {"orders": {"region": "eu-west-1"}, "audit": {"region": "eu-west-1", "table": "u"}},
        )

        assert len(island.built) == 1
