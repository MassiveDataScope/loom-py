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

import loom.core.connections.mongo.client as mongo_island
import loom.core.repository.mongo.backend as backend_module
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence import PersistenceWiring, resolve_backend
from loom.core.persistence.profile import SERVER_PROFILE, WORKER_PROFILE, RuntimeProfile
from loom.core.persistence.stores import StoreKey
from loom.core.repository.abc import Creatable
from loom.core.repository.mongo.backend import MongoBackend
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, invocation
from tests.helpers.persistence import build_backend

from ._fake import FakeMongoClient
from .conftest import Article
from .test_repository import ArticleCreate

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
    persistence = {"cursors": {"signing": "ephemeral"} if cursors is None else cursors}
    config: dict[str, Any] = {"app": {"name": "demo"}, "persistence": persistence}
    if section is not None:
        config["stores"] = {"default": {"backend": "mongo", **section}}
    return ConfigContext.from_dict(config)


def _wiring(
    monkeypatch: pytest.MonkeyPatch,
    fake_client: FakeMongoClient,
    section: dict[str, Any],
    models: Sequence[type[BaseModel]] = (),
) -> PersistenceWiring:
    monkeypatch.setattr(
        backend_module, "build_async_mongo_client", lambda _cfg, **_extra: fake_client
    )
    return build_backend(MongoBackend(), _ctx(section), models, SERVER_PROFILE)


_SECTION: dict[str, Any] = {"url": "mongodb://localhost:27017", "database": "demo"}


def test_backend_is_registered_under_its_name() -> None:
    assert isinstance(resolve_backend("mongo"), MongoBackend)
    assert MongoBackend.name == "mongo"


def test_missing_section_is_a_config_error() -> None:
    ctx = _ctx(None)
    backend = MongoBackend()

    with pytest.raises(ConfigError, match="stores.default"):
        build_backend(backend, ctx, (), SERVER_PROFILE)


def test_a_missing_cursor_key_fails_the_build_naming_the_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        backend_module, "build_async_mongo_client", lambda _cfg, **_extra: FakeMongoClient()
    )
    ctx = _ctx(_SECTION, cursors={})

    with pytest.raises(ConfigError, match=r"persistence\.cursors\.signing: ephemeral"):
        build_backend(MongoBackend(), ctx, (), SERVER_PROFILE)


def test_wiring_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION, (Article,))

    assert isinstance(wiring.uow_factory, MongoUnitOfWorkFactory)
    assert MongoBackend.repository_type is RepositoryMongo
    assert wiring.readiness is not None


@pytest.fixture
def client_options(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []

    def _record(**options: Any) -> FakeMongoClient:
        seen.append(options)
        return FakeMongoClient()

    monkeypatch.setattr(mongo_island, "AsyncMongoClient", _record)
    return seen


@pytest.mark.parametrize(
    ("profile", "max_size", "wait_ms"),
    [
        (SERVER_PROFILE, 10, 10000),
        (WORKER_PROFILE, 2, 30000),
    ],
)
def test_client_uses_profile_pool(
    client_options: list[dict[str, Any]],
    profile: RuntimeProfile,
    max_size: int,
    wait_ms: int,
) -> None:
    build_backend(MongoBackend(), _ctx(_SECTION), (), profile)

    assert client_options[0]["maxPoolSize"] == max_size
    assert client_options[0]["waitQueueTimeoutMS"] == wait_ms
    assert client_options[0]["minPoolSize"] == 0
    assert client_options[0]["maxIdleTimeMS"] == 1800000


def test_explicit_pool_values_win_key_by_key(client_options: list[dict[str, Any]]) -> None:
    build_backend(
        MongoBackend(),
        _ctx({**_SECTION, "pool": {"size": 8, "timeout": 2, "recycle": 60}}),
        (),
        SERVER_PROFILE,
    )

    assert client_options[0]["maxPoolSize"] == 13
    assert client_options[0]["waitQueueTimeoutMS"] == 2000
    assert client_options[0]["maxIdleTimeMS"] == 60000
    assert client_options[0]["minPoolSize"] == 0


def test_store_without_pool_section_gets_profile(client_options: list[dict[str, Any]]) -> None:
    build_backend(MongoBackend(), _ctx(_SECTION), (), WORKER_PROFILE)

    assert client_options[0]["maxPoolSize"] == 2


def test_explicit_overflow_wins_over_profile(client_options: list[dict[str, Any]]) -> None:
    build_backend(
        MongoBackend(),
        _ctx({**_SECTION, "pool": {"max_overflow": 1}}),
        (),
        SERVER_PROFILE,
    )

    assert client_options[0]["maxPoolSize"] == 6


def test_client_forwards_explicit_server_selection_timeout(
    client_options: list[dict[str, Any]],
) -> None:
    build_backend(
        MongoBackend(),
        _ctx({**_SECTION, "server_selection_timeout": 2}),
        (),
        SERVER_PROFILE,
    )

    assert client_options[0]["serverSelectionTimeoutMS"] == 2000


def test_client_forwards_uri_and_aware_datetime_option(
    client_options: list[dict[str, Any]],
) -> None:
    build_backend(MongoBackend(), _ctx(_SECTION), (), SERVER_PROFILE)

    assert client_options[0]["host"] == "mongodb://localhost:27017"
    assert client_options[0]["tz_aware"] is True


def test_pre_ping_is_accepted_without_a_driver_option(client_options: list[dict[str, Any]]) -> None:
    build_backend(
        MongoBackend(),
        _ctx({**_SECTION, "pool": {"pre_ping": False}}),
        (),
        SERVER_PROFILE,
    )

    assert "pre_ping" not in client_options[0]


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


async def test_readiness_is_true_when_ping_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    wiring = _wiring(monkeypatch, FakeMongoClient(), _SECTION)

    assert wiring.readiness is not None
    assert await wiring.readiness() is True


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


async def test_lifespan_closes_the_client_on_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeMongoClient()
    wiring = _wiring(monkeypatch, client, _SECTION)

    async with wiring.lifespan_init():
        assert client.closed is False

    assert client.closed is True


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


_AUDIT = StoreKey("audit")


def _audit_store(
    monkeypatch: pytest.MonkeyPatch, fake_client: FakeMongoClient
) -> PersistenceWiring:
    """Wire a transactional Mongo store under a key other than ``default``."""
    monkeypatch.setattr(
        backend_module, "build_async_mongo_client", lambda _cfg, **_extra: fake_client
    )
    section = {"backend": "mongo", **_SECTION, "transactions": True}
    ctx = ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "persistence": {"cursors": {"signing": "ephemeral"}},
            "stores": {"default": section, "audit": section},
        }
    )
    return build_backend(MongoBackend(), ctx, (Article,), SERVER_PROFILE, store=_AUDIT)


async def test_a_store_s_repositories_write_in_its_own_unit_of_work_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeMongoClient()
    wiring = _audit_store(monkeypatch, client)
    container = LoomContainer()
    wiring.repo_registration_module(container)
    assert wiring.uow_factory is not None
    coordinator = OrderedCommits({_AUDIT: wiring.uow_factory})

    async with invocation(coordinator) as scope, scope.frame(Frame(_AUDIT)):
        await container.resolve(Creatable[Article]).create(ArticleCreate(slug="a", title="A"))
        active = container.resolve(MongoSessions).active_session

    assert client.collection("articles").last_session is client.sessions[0]
    assert active is client.sessions[0]


async def test_a_store_s_unit_of_work_entered_by_hand_serves_its_repositories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeMongoClient()
    wiring = _audit_store(monkeypatch, client)
    container = LoomContainer()
    wiring.repo_registration_module(container)
    assert wiring.uow_factory is not None

    async with wiring.uow_factory.create():
        await container.resolve(Creatable[Article]).create(ArticleCreate(slug="a", title="A"))

    assert client.collection("articles").last_session is client.sessions[0]


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
