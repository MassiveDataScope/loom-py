"""Prepare, check and open are three steps. Open does not check, and close does not forget."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.pool import QueuePool

from loom.core.backend.sqlalchemy import get_compiled
from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend


class Plain(BaseModel):
    __tablename__ = "phase_items"
    id: int = ColumnField(Integer, primary_key=True)


class Scoped(BaseModel, RowScoped):
    __tablename__ = "phase_scoped"
    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True)


def _queued(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    *models: type[BaseModel],
    **schema: object,
) -> PersistenceWiring:
    """Build a file-SQLite backend whose pool is a queue, as Postgres is."""
    monkeypatch.setattr(
        "loom.core.repository.sqlalchemy.session_manager._default_pool_class",
        lambda _url: QueuePool,
    )
    wiring = _wiring(f"sqlite+aiosqlite:///{path}", *models, **schema)
    wiring.prepare_models(models)
    return wiring


def _wiring(url: str, *models: type[BaseModel], **schema: object):
    config = {
        "app": {"name": "demo"},
        "database": {"url": url, "schema": schema},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    return SQLAlchemyBackend().build(
        ConfigContext.from_dict(config), models, BackendRuntime(SERVER_PROFILE)
    )


async def test_startup_checks_keep_the_memory_database() -> None:
    """SQLite's one connection is the database: checking must not dispose it."""
    wiring = _wiring("sqlite+aiosqlite://", Plain)
    wiring.prepare_models((Plain,))

    await wiring.startup_checks()
    manager = wiring.uow_factory._session_manager  # type: ignore[union-attr]
    async with manager.session() as session:
        names = (await session.execute(text("SELECT name FROM sqlite_master"))).scalars()

    assert "phase_items" in set(names)


async def test_opening_does_not_verify_an_external_schema() -> None:
    wiring = _wiring("sqlite+aiosqlite://", Plain, mode="external")
    wiring.prepare_models((Plain,))

    async with wiring.lifespan_init():
        pass
    with pytest.raises(ConfigError, match="phase_items"):
        await wiring.startup_checks()


async def test_closing_leaves_the_compiled_model() -> None:
    wiring = _wiring("sqlite+aiosqlite://", Plain)
    wiring.prepare_models((Plain,))

    async with wiring.lifespan_init():
        pass

    assert get_compiled(Plain) is not None


def test_prepare_binds_scoped_settings_before_any_check() -> None:
    wiring = _wiring(
        "postgresql+asyncpg://app:secret@localhost/app",
        Scoped,
        mode="external",
        scopes={"holder": "identity.subject"},
    )
    manager = wiring.uow_factory._session_manager  # type: ignore[union-attr]
    with pytest.raises(RuntimeError, match="before startup"):
        manager.current_settings()

    wiring.prepare_models((Scoped,))

    assert manager.current_settings()


async def test_startup_checks_empty_a_queued_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A queued pool gives its connection back, and a file database keeps the tables."""
    wiring = _queued(monkeypatch, tmp_path / "phase.db", Plain)
    manager = wiring.uow_factory._session_manager  # type: ignore[union-attr]
    try:
        await wiring.startup_checks()
        pool = manager.engine.sync_engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.checkedin() == 0
        assert pool.checkedout() == 0
        async with manager.session() as session:
            names = (await session.execute(text("SELECT name FROM sqlite_master"))).scalars()
        assert "phase_items" in set(names)
    finally:
        await manager.dispose()


async def test_a_failed_check_empties_the_queued_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wiring = _queued(monkeypatch, tmp_path / "missing.db", Plain, mode="external")
    manager = wiring.uow_factory._session_manager  # type: ignore[union-attr]
    try:
        with pytest.raises(ConfigError, match="phase_items"):
            await wiring.startup_checks()
        pool = manager.engine.sync_engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.checkedin() == 0
        assert pool.checkedout() == 0
    finally:
        await manager.dispose()
