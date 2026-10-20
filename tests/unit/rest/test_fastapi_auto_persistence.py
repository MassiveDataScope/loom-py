"""Persistence resolution in the auto-bootstrap REST app.

Backends are reached by name through the ``loom.persistence.backends`` entry
point group; these tests exercise the three loom registers, an injected one,
and the bootstrap steps around them.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

import loom.core.plugins.entrypoints as entrypoints_module
import loom.core.repository.mongo.backend as mongo_backend_module
import loom.core.repository.sqlalchemy.backend as sqlalchemy_backend_module
from loom.core.config import ConfigContext
from loom.core.config.errors import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence import NoneBackend, PersistenceWiring
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.repository.mongo.uow import MongoUnitOfWorkFactory
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.fastapi import auto
from loom.rest.fastapi.auto import (
    _AppConfig,
    _discover_components,
    create_app,
)
from loom.rest.fastapi.auto import (
    _build_bootstrap as _build_bootstrap_in,
)
from loom.rest.model import RestInterface, RestRoute
from tests.unit.core.repository.mongo._fake import FakeMongoClient
from tests.unit.rest._fixture_app import UUID4_ID_FIELD, write_project

# The fixture apps opt out of cursor signing, which a deployed environment refuses.
_build_bootstrap = functools.partial(_build_bootstrap_in, environment=Environment.TEST)


def _interfaces(*classes: type) -> tuple[type[RestInterface[Any]], ...]:
    """Return interface classes typed for ``DiscoveryResult`` (``RestInterface`` is invariant)."""
    return tuple(cast(type[RestInterface[Any]], klass) for klass in classes)


_DYNAMODB_SECTION = {
    "region": "eu-west-1",
    "table": "products",
    "endpoint_url": "http://localhost:8000",
}
_DYNAMODB_PERSISTENCE = {"backend": "dynamodb", "dynamodb": _DYNAMODB_SECTION}
_MONGO_PERSISTENCE = {
    "backend": "mongo",
    "mongo": {"uri": "mongodb://localhost:27017", "database": "demo"},
}


class PersistenceNoneRecord(BaseModel):
    __tablename__ = "persistence_none_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


def _ctx(persistence: Mapping[str, object] | None = None, **sections: object) -> ConfigContext:
    cursors = {"cursors": {"signing": "ephemeral"}}
    return ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "database": {"url": "sqlite+aiosqlite:///"},
            "persistence": {**cursors, **(persistence or {})},
            **sections,
        }
    )


def _no_models() -> DiscoveryResult:
    return DiscoveryResult(models=(), use_cases=(), interfaces=())


def _agents_only() -> DiscoveryResult:
    return DiscoveryResult(models=(), use_cases=(), interfaces=(), agent_specs=("agents/*.yaml",))


@pytest.fixture
def aws_test_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dummy credentials satisfy boto3's default chain against a local endpoint."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


@pytest.fixture
def fake_mongo_client(monkeypatch: pytest.MonkeyPatch) -> FakeMongoClient:
    """Serve the Mongo backend an in-memory client so no connection is attempted."""
    client = FakeMongoClient()
    monkeypatch.setattr(mongo_backend_module, "_build_mongo_client", lambda _cfg: client)
    return client


@pytest.fixture
def agents_only_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: _agents_only())


class TestBackendSelection:
    @pytest.mark.parametrize(
        "persistence",
        [pytest.param(None, id="default"), pytest.param({"backend": "sqlalchemy"}, id="explicit")],
    )
    def test_defaults_to_sqlalchemy(
        self, agents_only_discovery: None, persistence: dict[str, object] | None
    ) -> None:
        ctx = _ctx() if persistence is None else _ctx(persistence=persistence)
        _runtime, wiring, _discovered, _python_interfaces = _build_bootstrap(
            _AppConfig(name="demo"), ctx
        )

        assert isinstance(wiring.uow_factory, SQLAlchemyUnitOfWorkFactory)

    def test_selects_dynamodb_backend(
        self, agents_only_discovery: None, aws_test_credentials: None
    ) -> None:
        _runtime, wiring, _discovered, _python_interfaces = _build_bootstrap(
            _AppConfig(name="demo"), _ctx(persistence=_DYNAMODB_PERSISTENCE)
        )

        assert isinstance(wiring.uow_factory, DynamoUnitOfWorkFactory)

    def test_selects_mongo_backend(
        self, agents_only_discovery: None, fake_mongo_client: FakeMongoClient
    ) -> None:
        _runtime, wiring, _discovered, _python_interfaces = _build_bootstrap(
            _AppConfig(name="demo"), _ctx(persistence=_MONGO_PERSISTENCE)
        )

        assert isinstance(wiring.uow_factory, MongoUnitOfWorkFactory)

    def test_none_has_no_unit_of_work(self, agents_only_discovery: None) -> None:
        _runtime, wiring, _discovered, _python_interfaces = _build_bootstrap(
            _AppConfig(name="demo"), _ctx(persistence={"backend": "none"})
        )

        assert wiring.uow_factory is None

    def test_unknown_backend_lists_registered_names(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, persistence={"backend": "mongodb"})

        with pytest.raises(ConfigError, match="Unknown persistence backend 'mongodb'") as exc_info:
            create_app(config_path)

        message = str(exc_info.value)
        assert "sqlalchemy" in message
        assert "dynamodb" in message
        assert "none" in message


class TestDynamoDBBoot:
    def test_requires_its_config_section(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, persistence={"backend": "dynamodb"})

        with pytest.raises(ConfigError, match="persistence.dynamodb"):
            create_app(config_path)


class TestMongoBoot:
    def test_boots_without_a_database_section(
        self, tmp_path: Path, fake_mongo_client: FakeMongoClient
    ) -> None:
        config_path = write_project(
            tmp_path, persistence=_MONGO_PERSISTENCE, database=None, id_field=UUID4_ID_FIELD
        )

        assert create_app(config_path) is not None

    def test_requires_its_config_section(self, tmp_path: Path) -> None:
        config_path = write_project(tmp_path, persistence={"backend": "mongo"})

        with pytest.raises(ConfigError, match="persistence.mongo"):
            create_app(config_path)

    def test_rejects_an_autoincrement_key_naming_the_model(
        self, tmp_path: Path, fake_mongo_client: FakeMongoClient
    ) -> None:
        config_path = write_project(tmp_path, persistence=_MONGO_PERSISTENCE, database=None)

        with pytest.raises(ConfigError, match="ConfigRecord.id"):
            create_app(config_path)


class TestNoneBoot:
    @pytest.mark.parametrize(
        "database",
        [
            pytest.param(None, id="no-database-section"),
            pytest.param(
                {"url": "postgresql+asyncpg://nobody@unreachable/nothing"},
                id="unusable-database-section",
            ),
        ],
    )
    def test_boots_regardless_of_the_database_section(
        self, tmp_path: Path, database: dict[str, object] | None
    ) -> None:
        config_path = write_project(tmp_path, persistence={"backend": "none"}, database=database)

        assert create_app(config_path) is not None


class _RecordingBackend:
    """Backend that records what the host hands it, delegating to ``none``."""

    name: ClassVar[str] = "recording"
    events: ClassVar[list[tuple[str, tuple[type[BaseModel], ...]]]] = []

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        self.events.append(("build", tuple(models)))
        wiring = NoneBackend().build(ctx, models)
        return PersistenceWiring(
            uow_factory=wiring.uow_factory,
            repo_registration_module=wiring.repo_registration_module,
            lifespan_init=wiring.lifespan_init,
            default_repository_type=wiring.default_repository_type,
            prepare_models=self._prepare_models,
        )

    def _prepare_models(self, models: Sequence[type[BaseModel]]) -> None:
        self.events.append(("prepare_models", tuple(models)))


class _RecordingEntryPoint:
    name = _RecordingBackend.name
    group = "loom.persistence.backends"

    def load(self) -> type[_RecordingBackend]:
        return _RecordingBackend


class _RecordingEntryPoints:
    def select(self, *, group: str) -> tuple[_RecordingEntryPoint, ...]:
        return (_RecordingEntryPoint(),) if group == _RecordingEntryPoint.group else ()


def test_injected_backend_boots_and_prepares_models_after_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A backend registered by another package boots through ``create_app`` unchanged."""
    monkeypatch.setattr(entrypoints_module, "entry_points", lambda: _RecordingEntryPoints())
    monkeypatch.setattr(_RecordingBackend, "events", [])
    config_path = write_project(tmp_path, persistence={"backend": "recording"}, database=None)

    assert create_app(config_path) is not None

    steps = [step for step, _models in _RecordingBackend.events]
    assert steps == ["build", "prepare_models"]
    built, prepared = (models for _step, models in _RecordingBackend.events)
    assert built == prepared
    assert [model.__name__ for model in built] == ["ConfigRecord"]


def test_discover_components_accepts_agents_only_result(agents_only_discovery: None) -> None:
    discovered = _discover_components(_AppConfig(name="demo"))

    assert discovered.agent_specs == ("agents/*.yaml",)


def test_discover_components_rejects_empty_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auto, "_build_discovery_result", lambda _cfg: _no_models())

    app_cfg = _AppConfig(name="demo")

    with pytest.raises(RuntimeError, match="Nothing discovered") as exc_info:
        _discover_components(app_cfg)

    message = str(exc_info.value)
    assert "discovery.mode: manifest" in message
    assert "AGENTS" in message


def test_build_bootstrap_sqlalchemy_without_models_warns_and_starts(
    agents_only_discovery: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A project whose relational schema is still empty boots, with a warning."""
    app_cfg = _AppConfig(name="demo")
    ctx = _ctx()

    with caplog.at_level(logging.WARNING, logger=sqlalchemy_backend_module.__name__):
        runtime, wiring, discovered, _python_interfaces = _build_bootstrap(app_cfg, ctx)

    assert discovered.models == ()
    assert runtime is not None
    assert wiring is not None
    assert "no BaseModel classes discovered" in caplog.text
    assert "persistence.backend: none" in caplog.text


@pytest.mark.parametrize(
    "persistence",
    [
        pytest.param(None, id="sqlalchemy"),
        pytest.param({"backend": "none"}, id="none"),
        pytest.param(_DYNAMODB_PERSISTENCE, id="dynamodb"),
    ],
)
def test_build_bootstrap_rejects_autocrud_without_model_on_any_backend(
    persistence: dict[str, object] | None,
    monkeypatch: pytest.MonkeyPatch,
    aws_test_credentials: None,
) -> None:
    """The coherence of an interface with its model does not depend on the backend."""

    class OrphanOnAnyBackend(RestInterface[PersistenceNoneRecord]):
        prefix = "/orphans"
        auto = True

    monkeypatch.setattr(
        auto,
        "_build_discovery_result",
        lambda _cfg: DiscoveryResult(
            models=(), use_cases=(), interfaces=_interfaces(OrphanOnAnyBackend), agent_specs=()
        ),
    )
    ctx = _ctx() if persistence is None else _ctx(persistence=persistence)
    app_config = _AppConfig(name="demo")

    with pytest.raises(RuntimeError, match="OrphanOnAnyBackend") as exc_info:
        _build_bootstrap(app_config, ctx)

    assert "PersistenceNoneRecord" in str(exc_info.value)
    assert "app.discovery" in str(exc_info.value)


def test_build_bootstrap_accepts_auto_true_with_hand_declared_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An interface that declares its own routes generates no CRUD, so it is left alone."""

    class _ListRecords(UseCase[PersistenceNoneRecord, None]):
        async def execute(self) -> None:  # pragma: no cover - never invoked
            return None

    class HandWrittenInterface(RestInterface[PersistenceNoneRecord]):
        prefix = "/hand-written"
        auto = True
        routes = (RestRoute(use_case=_ListRecords, method="GET", path=""),)

    monkeypatch.setattr(
        auto,
        "_build_discovery_result",
        lambda _cfg: DiscoveryResult(
            models=(), use_cases=(), interfaces=_interfaces(HandWrittenInterface), agent_specs=()
        ),
    )

    runtime, _wiring, discovered, _python_interfaces = _build_bootstrap(
        _AppConfig(name="demo"), _ctx()
    )

    assert runtime is not None
    assert discovered.models == ()
