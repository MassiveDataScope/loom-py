"""Each store an application composes gets its own backend, repositories and unit of work."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import AppConfig, Application, CompositionSource, compose_application
from loom.core.bootstrap.bootstrap import BootstrapError
from loom.core.command import Command
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope
from loom.core.discovery.base import DiscoveryResult
from loom.core.errors import UndeclaredStoreWrite
from loom.core.job.job import Job
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import StoreKey
from loom.core.repository import DefaultRepositoryBuilder, RepositoryBuildContext
from loom.core.repository.abc import RepoFor
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import transactional
from loom.core.use_case import Input, UseCase
from loom.core.use_case.errors import UncomposedWriteStore
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.rest.model import RestInterface
from tests.helpers.composition import Serving, composition_source

pytestmark = pytest.mark.timeout(30)

_LEDGER = StoreKey("ledger")


class _Account(BaseModel):
    __tablename__ = "per_store_accounts"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=20)


class _Entry(BaseModel):
    __tablename__ = "per_store_entries"
    __store__ = "ledger"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=20)


class _Named(Command, frozen=True):
    name: str


class _RecordEntry(UseCase[_Entry, int]):
    """A use case whose primary store is ``ledger``, not ``default``."""

    def __init__(self, entries: RepoFor[_Entry]) -> None:
        self.entries = entries

    async def execute(self, cmd: _Named = Input()) -> int:
        created = await self.entries.create(cmd)
        return int(created.id)


class _Service:
    """A service whose ``@transactional`` method joins the store of its session manager."""

    def __init__(self, session_manager: SessionManager) -> None:
        self.session_manager = session_manager

    @transactional
    async def joined(self) -> bool:
        return self.session_manager.active_session is not None


class _LedgerService(_Service):
    pass


class _DefaultService(_Service):
    pass


def _services(container: LoomContainer) -> None:
    container.register(
        _LedgerService,
        lambda: _LedgerService(container.resolve((SessionManager, _LEDGER))),
        scope=Scope.APPLICATION,
    )
    container.register(
        _DefaultService,
        lambda: _DefaultService(container.resolve(SessionManager)),
        scope=Scope.APPLICATION,
    )


class _ViaLedgerService(UseCase[_Entry, bool]):
    def __init__(self, service: _LedgerService) -> None:
        self.service = service

    async def execute(self) -> bool:
        return await self.service.joined()


class _ViaDefaultService(UseCase[_Entry, bool]):
    def __init__(self, service: _DefaultService) -> None:
        self.service = service

    async def execute(self) -> bool:
        return await self.service.joined()


@pytest.fixture
def databases(tmp_path: Path) -> tuple[Path, Path]:
    return tmp_path / "default.db", tmp_path / "ledger.db"


def _source(databases: tuple[Path, Path], second: str = _LEDGER) -> CompositionSource:
    default, other = databases
    stores = {
        "default": {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{default}"},
        second: {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{other}"},
    }
    ctx = ConfigContext.from_dict(
        {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )
    return composition_source(ctx, app=AppConfig(name="ledger"))


def _compose(
    databases: tuple[Path, Path], modules: tuple[Callable[[LoomContainer], None], ...] = ()
) -> Application:
    use_cases = (_RecordEntry, _ViaLedgerService, _ViaDefaultService)
    adapter = Serving(_Account, _Entry, use_cases=use_cases)
    return compose_application(_source(databases), adapter, modules=(_services, *modules))


@pytest.fixture
def application(databases: tuple[Path, Path]) -> Application:
    return _compose(databases)


def _rows(database: Path) -> dict[str, int]:
    with sqlite3.connect(database) as connection:
        tables = [n for (n,) in connection.execute("SELECT name FROM sqlite_master")]
        return {t: connection.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}


async def test_a_model_of_another_store_is_written_through_that_stores_engine(
    application: Application, databases: tuple[Path, Path]
) -> None:
    async with application.running():
        await application.kernel.container.resolve_repo(_Entry).create(_Named(name="e"))
        await application.kernel.container.resolve_repo(_Account).create(_Named(name="a"))

    assert [_rows(database) for database in databases] == [
        {"per_store_accounts": 1},
        {"per_store_entries": 1},
    ]


def test_the_default_session_manager_is_unkeyed_and_the_others_are_keyed_by_store(
    application: Application, databases: tuple[Path, Path]
) -> None:
    container = application.kernel.container

    managers = [container.resolve(SessionManager), container.resolve((SessionManager, _LEDGER))]

    assert [m.engine.url.database for m in managers] == [str(d) for d in databases]


async def test_a_use_case_whose_primary_store_is_not_the_default_commits_there(
    application: Application, databases: tuple[Path, Path]
) -> None:
    async with application.running():
        await application.kernel.app.invoke(_RecordEntry, payload={"name": "e"})

    assert [_rows(database) for database in databases] == [
        {"per_store_accounts": 0},
        {"per_store_entries": 1},
    ]


async def test_a_service_joins_the_primary_store_through_its_keyed_session_manager(
    application: Application,
) -> None:
    async with application.running():
        joined = await application.kernel.app.invoke(_ViaLedgerService)

    assert joined is True


async def test_a_service_on_the_default_session_manager_cannot_write_in_another_store(
    application: Application,
) -> None:
    async with application.running():
        with pytest.raises(UndeclaredStoreWrite, match="default"):
            await application.kernel.app.invoke(_ViaDefaultService)


class _RecordingBuilder:
    def __init__(self) -> None:
        self.models: set[type[Any]] = set()

    def __call__(self, context: RepositoryBuildContext) -> object:
        self.models.add(context.model)
        return object()


@pytest.mark.parametrize(
    ("key", "served"),
    [
        pytest.param(DefaultRepositoryBuilder, _Account, id="default-store"),
        pytest.param((DefaultRepositoryBuilder, _LEDGER), _Entry, id="ledger-store"),
    ],
)
def test_a_caller_builder_builds_the_repositories_of_its_store_only(
    databases: tuple[Path, Path], key: BindingKey, served: type[BaseModel]
) -> None:
    builder = _RecordingBuilder()

    container = _compose(databases, (lambda c: c.register_instance(key, builder),)).kernel.container
    for model in (_Account, _Entry):
        container.resolve_repo(model)

    assert builder.models == {served}


async def test_two_stores_on_one_backend_keep_one_readiness_probe_each(
    application: Application,
) -> None:
    async with application.running():
        ready = {key: await probe() for key, probe in application.readiness_probes().items()}

    assert ready == {"sqlalchemy": True, "ledger": True}


class _Audit(BaseModel):
    __tablename__ = "per_store_audit"
    __store__ = "sqlalchemy"

    id: int = ColumnField(primary_key=True, autoincrement=True)


def test_a_store_named_after_the_default_stores_backend_is_refused_before_any_is_built(
    databases: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    def build(*args: object) -> None:
        raise AssertionError("a store was built")

    monkeypatch.setattr(SQLAlchemyBackend, "build", build)
    source, adapter = _source(databases, second="sqlalchemy"), Serving(_Audit)

    with pytest.raises(ConfigError, match=r"stores\.sqlalchemy would report its readiness"):
        compose_application(source, adapter)


class _Entries(RestInterface[_Entry]):
    prefix = "/entries"
    auto = True


_SQLITE_LEDGER = {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}


def _rest_source(stores: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> CompositionSource:
    discovered = DiscoveryResult(models=(_Entry,), use_cases=(), interfaces=cast(Any, (_Entries,)))
    monkeypatch.setattr("loom.composition.source.discover", lambda config: discovered)
    ctx = ConfigContext.from_dict(
        {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )
    return composition_source(ctx, app=AppConfig(name="ledger"))


def test_autocrud_routes_of_a_model_follow_the_backend_of_its_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _rest_source({"default": {"backend": "none"}, "ledger": _SQLITE_LEDGER}, monkeypatch)

    application = compose_application(source, FastAPIAdapter())

    assert {route.path for route in _Entries.routes} >= {"/", "/{id}"}
    assert application.kernel.container.resolve_repo(_Entry) is not None


def test_autocrud_routes_of_a_model_in_a_store_without_repositories_are_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stores = {"default": _SQLITE_LEDGER, "ledger": {"backend": "none"}}
    source = _rest_source(stores, monkeypatch)
    adapter = FastAPIAdapter()

    with pytest.raises(RuntimeError, match=r"stores\.ledger\.backend 'none' serves no"):
        compose_application(source, adapter)


class _AuditRow(BaseModel):
    __tablename__ = "per_store_audit_rows"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=20)


class _RecordAudit(Job[int]):
    """A job without a main model, whose primary store is ``audit``."""

    __store__ = "audit"

    def __init__(self, rows: RepoFor[_AuditRow]) -> None:
        self.rows = rows

    async def execute(self) -> int:
        created = await self.rows.create(_Named(name="a"))
        return int(created.id)


class _RecordAuditUndeclared(_RecordAudit):
    __store__ = "ledger"


class _AuditUnstored(Job[int]):
    def __init__(self, rows: RepoFor[_AuditRow]) -> None:
        self.rows = rows

    async def execute(self) -> int:
        return 0


class _AuditMissing(_AuditUnstored):
    __store__ = "missing"


class _AuditBesideModel(UseCase[_AuditRow, int]):
    __store__ = "ledger"

    async def execute(self) -> int:
        return 0


def _compose_audit(tmp_path: Path, component: type[object]) -> Application:
    stores = {
        "default": {"backend": "none"},
        "audit": {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{tmp_path / 'audit.db'}"},
        "ledger": {"backend": "sqlalchemy", "url": f"sqlite+aiosqlite:///{tmp_path / 'ledger.db'}"},
    }
    ctx = ConfigContext.from_dict(
        {"stores": stores, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )
    source = composition_source(ctx, app=AppConfig(name="audit"))
    jobs = (component,) if issubclass(component, Job) else ()
    use_cases = () if jobs else (component,)
    return compose_application(source, Serving(_AuditRow, _Entry, use_cases=use_cases, jobs=jobs))


async def test_a_job_without_a_main_model_writes_to_the_store_it_declares(tmp_path: Path) -> None:
    application = _compose_audit(tmp_path, _RecordAudit)
    kernel = application.kernel

    async with application.running():
        await kernel.executor.execute(kernel.factory.build(_RecordAudit))

    assert _rows(tmp_path / "audit.db") == {"per_store_audit_rows": 1}


async def test_a_job_declaring_another_store_may_still_not_write_to_audit(tmp_path: Path) -> None:
    application = _compose_audit(tmp_path, _RecordAuditUndeclared)
    kernel = application.kernel

    async with application.running():
        job = kernel.factory.build(_RecordAuditUndeclared)
        with pytest.raises(UndeclaredStoreWrite, match="audit"):
            await kernel.executor.execute(job)

    assert _rows(tmp_path / "audit.db") == {"per_store_audit_rows": 0}


@pytest.mark.parametrize(
    ("component", "error", "match"),
    [
        (_AuditUnstored, UncomposedWriteStore, "default"),
        (_AuditMissing, ConfigError, r"__store__ of _AuditMissing needs stores\.missing"),
        (_AuditBesideModel, BootstrapError, "declares __store__ but writes to the store"),
    ],
    ids=["no-store", "undeclared-store", "beside-a-main-model"],
)
def test_a_store_a_component_declares_is_checked_at_composition(
    tmp_path: Path, component: type[object], error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        _compose_audit(tmp_path, component)
