"""A named ``__database__`` opens its own engine and none of the unused sections."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.pool import QueuePool

import loom.core.repository.sqlalchemy.backend as backend_module
from loom.core.authz import Permission, Role, RoleCatalog
from loom.core.authz.product import clear_authz_product, register_authz_product
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    RelationField,
    RowScoped,
    ScopedField,
)
from loom.core.model.types import Integer
from loom.core.persistence.database import load_database_config
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository import DefaultRepositoryBuilder, repository_for
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend, assemble_databases
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager, build_session_manager
from loom.rest.fastapi.auto import _serving_repository_type


def _context(database: dict[str, object]) -> ConfigContext:
    return ConfigContext.from_dict(
        {
            "app": {"name": "demo"},
            "database": database,
            "persistence": {"cursors": {"signing": "ephemeral"}},
        }
    )


def _url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


class _Built:
    def __init__(self) -> None:
        self.loaded: list[str] = []
        self.managers: list[SessionManager] = []
        self.labels: list[str] = []


def _spy(monkeypatch: pytest.MonkeyPatch) -> _Built:
    built = _Built()

    def load(ctx: ConfigContext, key: str = "database") -> Any:
        built.loaded.append(str(key))
        return load_database_config(ctx, key)

    def build(config: Any, profile: Any, **kwargs: Any) -> SessionManager:
        built.labels.append(str(kwargs.get("database")))
        manager = build_session_manager(config, profile, **kwargs)
        built.managers.append(manager)
        return manager

    monkeypatch.setattr(backend_module, "load_database_config", load)
    monkeypatch.setattr(backend_module, "build_session_manager", build)
    return built


def _queue(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loom.core.repository.sqlalchemy.session_manager._default_pool_class",
        lambda _url: QueuePool,
    )


def _close(built: _Built) -> None:
    for manager in built.managers:
        manager.engine.sync_engine.dispose()


async def _table_names(manager: SessionManager) -> set[str]:
    async with manager.session() as session:
        rows = await session.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'"))
        return set(rows.scalars())


async def _dispose(managers: list[SessionManager]) -> None:
    for manager in managers:
        await manager.dispose()


def test_a_bad_named_section_builds_no_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Event(BaseModel):
        __tablename__ = "bad_section_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    built = _spy(monkeypatch)
    config = _context(
        {"url": "sqlite+aiosqlite://", "tracking": {"url": "sqlite+aiosqlite://", "pool_size": 1}}
    )

    with pytest.raises(ConfigError, match="database.tracking"):
        assemble_databases(config, (_Event,), BackendRuntime(SERVER_PROFILE))

    assert built.managers == []


def test_a_missing_named_section_builds_no_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Event(BaseModel):
        __tablename__ = "missing_section_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    built = _spy(monkeypatch)

    with pytest.raises(ConfigError, match="database.tracking"):
        assemble_databases(
            _context({"url": "sqlite+aiosqlite://"}),
            (_Event,),
            BackendRuntime(SERVER_PROFILE),
        )

    assert built.managers == []


def test_a_shared_table_name_builds_no_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Item(BaseModel):
        __tablename__ = "shared_rows"
        id: int = ColumnField(Integer, primary_key=True)

    class _Event(BaseModel):
        __tablename__ = "shared_rows"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    built = _spy(monkeypatch)

    with pytest.raises(ConfigError, match="shared_rows"):
        assemble_databases(
            _context({"url": "sqlite+aiosqlite://", "tracking": {"url": "sqlite+aiosqlite://"}}),
            (_Item, _Event),
            BackendRuntime(SERVER_PROFILE),
        )

    assert built.loaded == []
    assert built.managers == []


def test_every_named_model_leaves_the_default_pool_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Event(BaseModel):
        __tablename__ = "only_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context({"tracking": {"url": "sqlite+aiosqlite://"}}),
            (_Event,),
            BackendRuntime(SERVER_PROFILE),
        )

        assert built.loaded == ["database.tracking"]
        assert built.labels == ["tracking"]
        assert wiring.uow_factory is None
        assert len(built.managers) == 1
        wiring.prepare_models((_Event,))
        container = LoomContainer()
        wiring.repo_registration_module(container)
        assert container.is_registered(SessionManager) is False
        assert container.is_registered(DefaultRepositoryBuilder) is False
        assert container.resolve_repo(_Event).session_manager is built.managers[0]
    finally:
        _close(built)


_MANAGE = Permission("members.manage")
_CATALOG = RoleCatalog((_MANAGE,), (Role("admin", (_MANAGE,)),))


@dataclass
class _Product:
    elevations: dict[str, Permission]
    catalog: RoleCatalog = _CATALOG
    delegate: Permission = _MANAGE


def test_an_elevation_on_a_later_section_uses_the_whole_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Kept(BaseModel, RowScoped):
        __tablename__ = "elev_kept"
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")

    class _Tracked(BaseModel, RowScoped):
        __tablename__ = "elev_tracked"
        __database__ = "tracking"
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")
        editor: int = ScopedField(Integer, scope="editor", on="write", elevable=True)

    database = {
        "url": "postgresql+asyncpg://app:secret@localhost/app",
        "schema": {"mode": "external", "scopes": {"holder": "identity.subject"}},
        "tracking": {
            "url": "postgresql+asyncpg://app:secret@localhost/tracking",
            "schema": {
                "mode": "external",
                "scopes": {"holder": "identity.subject", "editor": "identity.subject"},
            },
        },
    }
    register_authz_product(_Product(elevations={"editor": _MANAGE}))
    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context(database),
            (_Kept, _Tracked),
            BackendRuntime(SERVER_PROFILE),
        )
        wiring.prepare_models((_Kept, _Tracked))
        clear_authz_product()
        register_authz_product(_Product(elevations={"nope": _MANAGE}))
        with pytest.raises(ConfigError, match="nope"):
            wiring.prepare_models((_Kept, _Tracked))
    finally:
        clear_authz_product()
        _close(built)


def test_a_later_external_section_without_scoped_tables_is_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Kept(BaseModel, RowScoped):
        __tablename__ = "bound_kept"
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")

    class _Log(BaseModel):
        __tablename__ = "bound_logs"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    database = {
        "url": "postgresql+asyncpg://app:secret@localhost/app",
        "schema": {"mode": "external", "scopes": {"holder": "identity.subject"}},
        "tracking": {
            "url": "postgresql+asyncpg://app:secret@localhost/tracking",
            "schema": {"mode": "external", "scopes": {"holder": "identity.subject"}},
        },
    }
    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context(database),
            (_Kept, _Log),
            BackendRuntime(SERVER_PROFILE),
        )
        wiring.prepare_models((_Kept, _Log))
        assert built.managers[1].has_session_settings is True
        assert built.managers[1].current_settings() == {}
    finally:
        _close(built)


def test_a_later_naming_convention_must_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Item(BaseModel):
        __tablename__ = "convention_items"
        id: int = ColumnField(Integer, primary_key=True)

    class _Event(BaseModel):
        __tablename__ = "convention_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    _queue(monkeypatch)
    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context(
                {
                    "url": _url(tmp_path / "default.db"),
                    "tracking": {
                        "url": _url(tmp_path / "tracking.db"),
                        "schema": {"naming_convention": {"pk": "pk_%(table_name)s"}},
                    },
                }
            ),
            (_Item, _Event),
            BackendRuntime(SERVER_PROFILE),
        )

        with pytest.raises(ConfigError, match="naming_convention"):
            wiring.prepare_models((_Item, _Event))

        for manager in built.managers:
            pool = manager.engine.sync_engine.pool
            assert isinstance(pool, QueuePool)
            assert pool.checkedout() == 0
            assert pool.checkedin() == 0
    finally:
        _close(built)


def test_a_foreign_key_cannot_cross_databases(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Parent(BaseModel):
        __tablename__ = "fk_parents"
        id: int = ColumnField(Integer, primary_key=True)

    class _Child(BaseModel):
        __tablename__ = "fk_children"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)
        parent_id: int = ColumnField(Integer, foreign_key="fk_parents.id")

    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context({"url": "sqlite+aiosqlite://", "tracking": {"url": "sqlite+aiosqlite://"}}),
            (_Parent, _Child),
            BackendRuntime(SERVER_PROFILE),
        )

        with pytest.raises(ConfigError, match="another database"):
            wiring.prepare_models((_Parent, _Child))
    finally:
        _close(built)


class _PullChild(BaseModel):
    __tablename__ = "pulled_children"
    __database__ = "tracking"
    id: int = ColumnField(Integer, primary_key=True)
    parent_id: int = ColumnField(Integer, foreign_key="pulled_parents.id")


class _PullParent(BaseModel):
    __tablename__ = "pulled_parents"
    id: int = ColumnField(Integer, primary_key=True)
    children: list[_PullChild] = RelationField(
        foreign_key="parent_id",
        cardinality=Cardinality.ONE_TO_MANY,
    )


def test_a_relation_cannot_pull_another_database_into_this_compile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context({"url": "sqlite+aiosqlite://", "tracking": {"url": "sqlite+aiosqlite://"}}),
            (_PullParent, _PullChild),
            BackendRuntime(SERVER_PROFILE),
        )

        with pytest.raises(ConfigError, match="pulled_children"):
            wiring.prepare_models((_PullParent, _PullChild))
    finally:
        _close(built)


async def test_each_database_creates_only_its_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Item(BaseModel):
        __tablename__ = "kept_items"
        id: int = ColumnField(Integer, primary_key=True)

    class _Event(BaseModel):
        __tablename__ = "tracked_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    default_path = tmp_path / "default.db"
    tracking_path = tmp_path / "tracking.db"
    warehouse_path = tmp_path / "warehouse.db"
    _queue(monkeypatch)
    built = _spy(monkeypatch)
    wiring = assemble_databases(
        _context(
            {
                "url": _url(default_path),
                "tracking": {"url": _url(tracking_path)},
                "warehouse": {"url": _url(warehouse_path)},
            }
        ),
        (_Item, _Event),
        BackendRuntime(SERVER_PROFILE),
    )

    try:
        wiring.prepare_models((_Item, _Event))
        await wiring.startup_checks()
        assert built.loaded == ["database", "database.tracking"]
        assert built.labels == ["default", "tracking"]
        assert not warehouse_path.exists()
        for manager in built.managers:
            pool = manager.engine.sync_engine.pool
            assert isinstance(pool, QueuePool)
            assert pool.checkedin() == 0
            assert pool.checkedout() == 0
        default_tables = await _table_names(built.managers[0])
        tracking_tables = await _table_names(built.managers[1])
        assert "kept_items" in default_tables
        assert "tracked_events" not in default_tables
        assert "tracked_events" in tracking_tables
        assert "kept_items" not in tracking_tables
    finally:
        await _dispose(built.managers)


async def test_a_second_database_does_not_join_the_default_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Item(BaseModel):
        __tablename__ = "affinity_items"
        id: int = ColumnField(Integer, primary_key=True)

    class _Event(BaseModel):
        __tablename__ = "affinity_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    @repository_for(_Event)
    class _EventRepository(RepositorySQLAlchemy[_Event, int]):
        pass

    built = _spy(monkeypatch)
    try:
        wiring = assemble_databases(
            _context(
                {
                    "url": _url(tmp_path / "default.db"),
                    "tracking": {"url": _url(tmp_path / "tracking.db")},
                }
            ),
            (_Item, _Event),
            BackendRuntime(SERVER_PROFILE),
        )
        wiring.prepare_models((_Item, _Event))
        container = LoomContainer()
        wiring.repo_registration_module(container)
        item_repo = container.resolve_repo(_Item)
        event_repo = container.resolve_repo(_Event)

        assert isinstance(event_repo, _EventRepository)
        assert item_repo.session_manager is not event_repo.session_manager
        assert item_repo.cursor_codec is event_repo.cursor_codec
        assert container.resolve(SessionManager) is item_repo.session_manager
        assert container.is_registered(DefaultRepositoryBuilder) is True
        assert wiring.uow_factory is not None
        assert wiring.uow_factory._session_manager is item_repo.session_manager  # type: ignore[union-attr]
        async with wiring.uow_factory.create() as uow:
            async with item_repo._session_scope() as joined:
                assert joined is uow._session
            assert item_repo.has_caller_scoped_session() is True
            assert event_repo.has_caller_scoped_session() is False
            async with event_repo._session_scope() as own:
                assert own is not uow._session
                assert _bound(own, event_repo.session_manager)
    finally:
        await _dispose(built.managers)


def _bound(session: object, manager: SessionManager) -> bool:
    bind = getattr(session, "bind", None)
    if bind is None:
        bind = getattr(getattr(session, "sync_session", None), "bind", None)
    engine = manager.engine
    if bind is engine or bind is engine.sync_engine:
        return True
    nested = getattr(bind, "engine", None)
    return nested is engine or nested is engine.sync_engine


def test_a_sync_bind_is_the_same_database() -> None:
    class _Engine:
        def __init__(self) -> None:
            self.sync_engine = object()

    class _Manager:
        engine = _Engine()

    class _Repo:
        session_manager = _Manager()

    class _Connection:
        def __init__(self, engine: object) -> None:
            self.engine = engine

    class _Session:
        def __init__(self, bind: object) -> None:
            self.bind = bind

    class _Legacy:
        def __init__(self, bind: object) -> None:
            self.sync_session = _Session(bind)

    engine = _Repo.session_manager.engine
    bound = RepositorySQLAlchemy._bound_to_this_engine
    assert bound(_Repo(), _Session(engine)) is True  # type: ignore[arg-type]
    assert bound(_Repo(), _Session(engine.sync_engine)) is True  # type: ignore[arg-type]
    assert bound(_Repo(), _Session(_Connection(engine))) is True  # type: ignore[arg-type]
    assert bound(_Repo(), _Session(_Connection(engine.sync_engine))) is True  # type: ignore[arg-type]
    assert bound(_Repo(), _Legacy(engine.sync_engine)) is True  # type: ignore[arg-type]
    assert bound(_Repo(), _Session(object())) is False  # type: ignore[arg-type]
    assert bound(_Repo(), _Session(None)) is False  # type: ignore[arg-type]


def test_the_gate_keeps_a_custom_repository() -> None:
    class _Event(BaseModel):
        __tablename__ = "custom_events"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    @repository_for(_Event)
    class _EventRepository(RepositorySQLAlchemy[_Event, int]):
        pass

    assert _serving_repository_type(_Event, SQLAlchemyBackend()) is _EventRepository
