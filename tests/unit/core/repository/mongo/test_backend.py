"""``MongoBackend``: config section, wiring shape, model preparation and readiness."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

import loom.core.repository.mongo.backend as backend_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence import PersistenceWiring, resolve_backend
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository.mongo.backend import MongoBackend
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoUnitOfWorkFactory

from ._fake import FakeMongoClient
from .conftest import Article

_SRC = Path(__file__).resolve().parents[5] / "src"

_MISSING_EXTRA_SCRIPT = """
import sys
sys.modules["pymongo"] = None
sys.modules["bson"] = None

from loom.core.config import ConfigError
from loom.core.persistence import resolve_backend

try:
    resolve_backend("mongo")
except ConfigError as exc:
    assert "loom-kernel[mongo]" in str(exc), str(exc)
else:
    raise SystemExit("resolve_backend('mongo') did not fail without pymongo")
"""


class Counter(BaseModel):
    __tablename__ = "counters"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    value: int = ColumnField()


class Ticket(BaseModel):
    """Key loom would have to mint, but whose annotation cannot hold a generated id."""

    __tablename__ = "tickets"

    id: int = ColumnField(primary_key=True)
    subject: str = ColumnField(length=32)


def _ctx(section: dict[str, Any] | None, cursors: dict[str, Any] | None = None) -> ConfigContext:
    persistence: dict[str, Any] = {
        "backend": "mongo",
        "cursors": {"signing": "ephemeral"} if cursors is None else cursors,
    }
    if section is not None:
        persistence["mongo"] = section
    return ConfigContext.from_dict({"app": {"name": "demo"}, "persistence": persistence})


def _wiring(
    monkeypatch: pytest.MonkeyPatch,
    fake_client: FakeMongoClient,
    section: dict[str, Any],
    models: Sequence[type[BaseModel]] = (),
) -> PersistenceWiring:
    monkeypatch.setattr(backend_module, "_build_mongo_client", lambda _cfg: fake_client)
    return MongoBackend().build(_ctx(section), models, BackendRuntime(SERVER_PROFILE))


_SECTION: dict[str, Any] = {"uri": "mongodb://localhost:27017", "database": "demo"}


def test_backend_is_registered_under_its_name() -> None:
    assert isinstance(resolve_backend("mongo"), MongoBackend)
    assert MongoBackend.name == "mongo"


def test_missing_section_is_a_config_error() -> None:
    ctx = _ctx(None)
    backend = MongoBackend()

    with pytest.raises(ConfigError, match="persistence.mongo"):
        backend.build(ctx, (), BackendRuntime(SERVER_PROFILE))


def test_a_missing_cursor_key_fails_the_build_naming_the_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(backend_module, "_build_mongo_client", lambda _cfg: FakeMongoClient())
    ctx = _ctx(_SECTION, cursors={})

    with pytest.raises(ConfigError, match=r"persistence\.cursors\.signing: ephemeral"):
        MongoBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))


def test_wiring_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION, (Article,))

    assert isinstance(wiring.uow_factory, MongoUnitOfWorkFactory)
    assert wiring.default_repository_type is RepositoryMongo
    assert wiring.readiness is not None


def test_client_seam_forwards_only_the_options_set(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, dict[str, Any]]] = []

    def _record(uri: str, **options: Any) -> FakeMongoClient:
        seen.append((uri, options))
        return FakeMongoClient()

    monkeypatch.setattr(backend_module, "AsyncMongoClient", _record)

    MongoBackend().build(_ctx(_SECTION), (), BackendRuntime(SERVER_PROFILE))
    MongoBackend().build(
        _ctx({**_SECTION, "max_pool_size": 8, "server_selection_timeout_ms": 500}),
        (),
        BackendRuntime(SERVER_PROFILE),
    )

    assert seen == [
        ("mongodb://localhost:27017", {"tz_aware": True}),
        (
            "mongodb://localhost:27017",
            {"tz_aware": True, "maxPoolSize": 8, "serverSelectionTimeoutMS": 500},
        ),
    ]


def test_unknown_section_key_is_a_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    section = {**_SECTION, "transactoins": True}
    client = FakeMongoClient()

    with pytest.raises(ConfigError, match="transactoins"):
        _wiring(monkeypatch, client, section)


@pytest.mark.parametrize("transactions", [False, True])
def test_build_logs_the_unit_of_work_mode(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, transactions: bool
) -> None:
    with caplog.at_level(logging.INFO, logger=backend_module.__name__):
        _wiring(monkeypatch, FakeMongoClient(), {**_SECTION, "transactions": transactions})

    assert f"mongo unit of work: transactions={transactions}" in caplog.text


def test_prepare_models_rejects_autoincrement_naming_the_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION, (Article, Counter))

    with pytest.raises(ConfigError, match="Counter.id"):
        wiring.prepare_models((Article, Counter))


def test_prepare_models_rejects_a_key_that_cannot_hold_a_generated_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION, (Article, Ticket))

    with pytest.raises(ConfigError, match=r"Ticket\.id .*int"):
        wiring.prepare_models((Article, Ticket))


def test_prepare_models_accepts_models_with_a_client_or_generated_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION, (Article,))

    wiring.prepare_models((Article,))


@pytest.mark.asyncio
async def test_readiness_is_true_when_ping_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION)

    assert wiring.readiness is not None
    assert await wiring.readiness() is True


@pytest.mark.asyncio
async def test_readiness_is_false_and_logged_when_ping_fails(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    client = FakeMongoClient()
    client.ping_error = ConnectionError("no primary")
    wiring = _wiring(monkeypatch, client, _SECTION)

    assert wiring.readiness is not None
    with caplog.at_level(logging.WARNING, logger=backend_module.__name__):
        assert await wiring.readiness() is False

    assert "readiness" in caplog.text
    assert "ConnectionError" in caplog.text


@pytest.mark.asyncio
async def test_lifespan_closes_the_client_on_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeMongoClient()
    wiring = _wiring(monkeypatch, client, _SECTION)

    async with wiring.lifespan_init():
        assert client.closed is False

    assert client.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("section", "expected_sessions"),
    [
        pytest.param(_SECTION, 0, id="default-no-op"),
        pytest.param({**_SECTION, "transactions": True}, 1, id="transactional"),
    ],
)
async def test_transactions_flag_selects_the_unit_of_work(
    monkeypatch: pytest.MonkeyPatch, section: dict[str, Any], expected_sessions: int
) -> None:
    client = FakeMongoClient()
    wiring = _wiring(monkeypatch, client, section)

    assert wiring.uow_factory is not None
    async with wiring.uow_factory.create():
        pass

    assert len(client.sessions) == expected_sessions


def test_missing_extra_names_it() -> None:
    """US2 s6: without pymongo, selecting the backend names ``loom-kernel[mongo]``."""
    result = subprocess.run(
        [sys.executable, "-c", _MISSING_EXTRA_SCRIPT],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert result.returncode == 0, result.stderr
