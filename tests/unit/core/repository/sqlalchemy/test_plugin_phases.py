"""Prepare, check and open are three steps. Open does not check, and close does not forget."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.pool import QueuePool

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer
from loom.core.stores.plugin import StoreWiring
from loom.core.stores.profile import SERVER_PROFILE
from loom.stores.sqlalchemy.plugin import SQLAlchemyStorePlugin
from loom.stores.sqlalchemy.schema import get_compiled
from loom.stores.sqlalchemy.session_manager import DatabaseDial
from tests.helpers.persistence import build_backend


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
) -> StoreWiring:
    """Build a file-SQLite backend whose pool is a queue, as Postgres is."""
    monkeypatch.setattr(DatabaseDial, "default_pool_class", lambda _dial: QueuePool)
    wiring = _wiring(f"sqlite+aiosqlite:///{path}", *models, **schema)
    wiring.prepare_models(models)
    return wiring


def _wiring(url: str, *models: type[BaseModel], **schema: object):
    config = {
        "app": {"name": "demo"},
        "stores": {
            "default": {"backend": "sqlalchemy", "url": url, "schema": schema},
            "settings": {"cursors": {"signing": "ephemeral"}},
        },
    }
    return build_backend(
        SQLAlchemyStorePlugin(), ConfigContext.from_dict(config), models, SERVER_PROFILE
    )


class TestBackendPhases:
    async def test_startup_checks_keep_the_memory_database(self) -> None:
        """SQLite's one connection is the database: checking must not dispose it."""
        wiring = _wiring("sqlite+aiosqlite://", Plain)
        wiring.prepare_models((Plain,))

        await wiring.startup_checks()
        manager = wiring.uow_factory._session_manager  # type: ignore[union-attr]
        async with manager.session() as session:
            names = (await session.execute(text("SELECT name FROM sqlite_master"))).scalars()

        assert "phase_items" in set(names)

    async def test_opening_does_not_verify_an_external_schema(self) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Plain, mode="external")
        wiring.prepare_models((Plain,))

        async with wiring.lifespan_init():
            pass
        with pytest.raises(ConfigError, match="phase_items"):
            await wiring.startup_checks()

    async def test_closing_leaves_the_compiled_model(self) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Plain)
        wiring.prepare_models((Plain,))
        metadata = wiring.uow_factory._session_manager.metadata  # type: ignore[union-attr]

        async with wiring.lifespan_init():
            pass

        assert get_compiled(Plain, metadata=metadata) is not None

    def test_prepare_compiles_into_the_store_and_leaves_the_shared_registry_alone(self) -> None:
        wiring = _wiring("sqlite+aiosqlite://", Plain)

        wiring.prepare_models((Plain,))

        metadata = wiring.uow_factory._session_manager.metadata  # type: ignore[union-attr]
        assert set(metadata.tables) == {"phase_items"}
        assert get_compiled(Plain) is None

    def test_prepare_binds_scoped_settings_before_any_check(self) -> None:
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
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
            await manager.close()

    async def test_a_failed_check_empties_the_queued_pool(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
            await manager.close()
