"""The API, the worker and a streaming sink build the same pool from the same section."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext
from loom.core.model import LoomStruct
from loom.core.persistence.database import load_database_config
from loom.core.persistence.profile import SERVER_PROFILE, STREAMING_PROFILE, BackendRuntime
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.session_manager import SessionManager, build_session_manager
from loom.streaming.nodes._table import IntoTable
from loom.streaming.nodes._table.config import resolve_sqlalchemy_table_config

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"
_FULL_POOL = {"size": 3, "max_overflow": 1, "timeout": 4, "recycle": 60, "pre_ping": False}


class _Row(LoomStruct):
    id: int


def _pool_settings(manager: SessionManager) -> tuple[Any, ...]:
    pool: Any = manager.engine.sync_engine.pool
    return (pool.size(), pool._max_overflow, pool._timeout, pool._recycle, pool._pre_ping)


def _api_manager(database: dict[str, Any]) -> SessionManager:
    ctx = ConfigContext.from_dict(
        {"database": database, "persistence": {"cursors": {"signing": "ephemeral"}}}
    )
    wiring = SQLAlchemyBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))
    factory: Any = wiring.uow_factory
    return factory._session_manager


def _worker_manager(database: dict[str, Any]) -> SessionManager:
    pytest.importorskip("celery")
    from loom.celery.bootstrap import _resolve_uow_factory

    _factory, manager = _resolve_uow_factory(ConfigContext.from_dict({"database": database}))
    assert manager is not None
    return manager


def _streaming_manager(database: dict[str, Any]) -> SessionManager:
    ctx = ConfigContext.from_dict(
        {
            "database": {"warehouse": database},
            "streaming": {"sinks": {"orders_sink": {"database": "warehouse"}}},
        }
    )
    node = IntoTable(payload=_Row, table="orders", name="orders_sink")
    resolved = resolve_sqlalchemy_table_config(node, ctx)
    return build_session_manager(resolved.database, STREAMING_PROFILE)


def test_a_fully_declared_pool_is_identical_in_every_entry_point() -> None:
    database = {"url": _POSTGRES, "pool": _FULL_POOL}

    settings = {
        _pool_settings(_api_manager(database)),
        _pool_settings(_worker_manager(database)),
        _pool_settings(_streaming_manager(database)),
    }

    assert settings == {(3, 1, 4, 60, False)}


def test_the_same_section_reads_identically_wherever_it_lives() -> None:
    database = {"url": _POSTGRES, "pool": _FULL_POOL, "connect_args": {"command_timeout": 5}}
    ctx = ConfigContext.from_dict({"database": {**database, "warehouse": database}})

    assert load_database_config(ctx) == load_database_config(ctx, "database.warehouse")


def test_without_a_pool_the_api_takes_the_server_profile() -> None:
    assert _pool_settings(_api_manager({"url": _POSTGRES})) == (5, 5, 10, 1800, True)


def test_without_a_pool_the_worker_takes_the_worker_profile() -> None:
    # SC-003: a prefork worker of 8 processes opens at most 8 x (2 + 0) = 16 connections.
    assert _pool_settings(_worker_manager({"url": _POSTGRES})) == (2, 0, 30, 1800, True)


@pytest.mark.parametrize(
    ("build", "expected"),
    [(_api_manager, (1, 5, 10, 1800, True)), (_worker_manager, (1, 0, 30, 1800, True))],
)
def test_one_declared_key_changes_only_that_key(build: Any, expected: tuple[Any, ...]) -> None:
    assert _pool_settings(build({"url": _POSTGRES, "pool": {"size": 1}})) == expected
