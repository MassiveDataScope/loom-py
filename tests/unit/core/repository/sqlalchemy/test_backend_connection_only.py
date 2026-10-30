"""``SQLAlchemyBackend`` built with ``BackendRuntime(connection_only=True)``.

A host with no model (a worker running raw-SQL jobs) gets a unit of work and
nothing more: no cursor codec, no repository, and the compiled registry left
as the caller compiled it. Its startup checks only probe the connection: no
table of the process-wide metadata is created or inspected.
"""

from __future__ import annotations

import sqlite3
from datetime import time
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.exc import OperationalError

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel, ColumnField, ColumnType, RowScoped, ScopedField
from loom.core.model.types import Integer
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.profile import WORKER_PROFILE, BackendRuntime
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.session_manager import SessionManager

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


class _CallerCompiled(BaseModel):
    __tablename__ = "caller_compiled_connection_only"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class _CallerZoned(BaseModel):
    """A column startup refuses on SQLite, when it checks the models."""

    __tablename__ = "caller_zoned_connection_only"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    closes_at: time = ColumnField(ColumnType("Time", kwargs={"timezone": True}))


class _CallerScoped(BaseModel, RowScoped):
    """A scoped table some other host of the process compiled."""

    __tablename__ = "caller_scoped_connection_only"

    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True)


def _wiring(database: dict[str, Any]) -> PersistenceWiring:
    # No persistence.cursors at all: a codec built here would fail.
    ctx = ConfigContext.from_dict({"database": database})
    return SQLAlchemyBackend().build(ctx, (), BackendRuntime(WORKER_PROFILE, connection_only=True))


def _registered(wiring: PersistenceWiring) -> LoomContainer:
    container = LoomContainer()
    wiring.repo_registration_module(container)
    return container


def test_no_cursor_codec_is_built_or_registered() -> None:
    container = _registered(_wiring({"url": "sqlite+aiosqlite:///"}))

    assert not container.is_registered(CursorCodec)


def test_the_session_manager_and_the_unit_of_work_are_still_served() -> None:
    wiring = _wiring({"url": "sqlite+aiosqlite:///"})

    assert wiring.uow_factory is not None
    assert isinstance(_registered(wiring).resolve(SessionManager), SessionManager)


def test_preparing_leaves_the_compiled_registry_alone() -> None:
    compile_all(_CallerCompiled)

    _wiring({"url": "sqlite+aiosqlite:///"}).prepare_models(())

    assert get_compiled(_CallerCompiled) is not None


def test_row_level_security_stays_unbound_and_fails_closed() -> None:
    """Postgres with an external schema: no session may run without the scoped settings."""
    database = {"url": _POSTGRES, "schema": {"mode": "external", "scopes": {"owner": "x.y"}}}
    wiring = _wiring(database)
    wiring.prepare_models(())
    manager = _registered(wiring).resolve(SessionManager)

    assert manager.has_session_settings
    with pytest.raises(RuntimeError, match="before startup bound them"):
        manager.current_settings()


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        return {name for (name,) in rows}


def _idle(wiring: PersistenceWiring) -> tuple[int, int]:
    pool = _registered(wiring).resolve(SessionManager).engine.sync_engine.pool
    return pool.checkedout(), pool.checkedin()


async def test_startup_probes_the_connection_and_creates_no_table(tmp_path: Path) -> None:
    """The compiled models belong to the caller: neither created nor checked here."""
    compile_all(_CallerCompiled, _CallerZoned)
    path = tmp_path / "worker.db"
    wiring = _wiring({"url": f"sqlite+aiosqlite:///{path}"})
    wiring.prepare_models(())

    await wiring.startup_checks()

    assert path.exists()
    assert _tables(path) == set()
    assert _idle(wiring) == (0, 0)


async def test_an_unreachable_database_fails_the_startup(tmp_path: Path) -> None:
    wiring = _wiring({"url": f"sqlite+aiosqlite:///{tmp_path / 'missing' / 'worker.db'}"})
    wiring.prepare_models(())

    with pytest.raises(OperationalError, match="unable to open database file"):
        await wiring.startup_checks()

    assert _idle(wiring) == (0, 0)


async def test_postgres_refuses_scoped_tables_outside_an_external_schema() -> None:
    """The refusal ``startup_checks`` makes for create_all, before any connection."""
    compile_all(_CallerScoped)
    wiring = _wiring({"url": _POSTGRES})
    wiring.prepare_models(())

    with pytest.raises(ConfigError, match="create_all cannot protect scoped tables"):
        await wiring.startup_checks()

    assert _idle(wiring) == (0, 0)


async def test_scoped_tables_on_sqlite_are_not_this_refusal(tmp_path: Path) -> None:
    compile_all(_CallerScoped)
    wiring = _wiring({"url": f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}"})
    wiring.prepare_models(())

    await wiring.startup_checks()
