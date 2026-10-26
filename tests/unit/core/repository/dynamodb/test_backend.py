"""``DynamoDBBackend``: the readiness probe over the configured table."""

from __future__ import annotations

import logging

import pytest

import loom.core.repository.dynamodb.backend as backend_module
from loom.core.config import ConfigContext
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository.dynamodb.backend import DynamoDBBackend

from .conftest import FakeClient


def _wiring(
    monkeypatch: pytest.MonkeyPatch, fake_client: FakeClient, table: str
) -> PersistenceWiring:
    monkeypatch.setattr(backend_module, "build_aws_client", lambda _service, _cfg: fake_client)
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "persistence": {
                "backend": "dynamodb",
                "dynamodb": {"region": "eu-west-1", "table": table},
            },
        }
    )
    return DynamoDBBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [("ACTIVE", True), ("UPDATING", True), ("CREATING", False), ("DELETING", False)],
)
async def test_readiness_reflects_the_table_status(
    monkeypatch: pytest.MonkeyPatch, fake_client: FakeClient, status: str, expected: bool
) -> None:
    fake_client.table_status["products"] = status
    wiring = _wiring(monkeypatch, fake_client, "products")

    assert wiring.readiness is not None
    assert await wiring.readiness() is expected


@pytest.mark.asyncio
async def test_readiness_is_false_when_the_table_is_missing(
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
