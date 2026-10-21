"""``build_session_manager``: the one constructor of a configured engine."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.pool import NullPool

from loom.core.persistence.database import DatabaseConfig, PoolConfig
from loom.core.persistence.profile import (
    SERVER_PROFILE,
    WORKER_PROFILE,
    PoolDefaults,
    RuntimeProfile,
)
from loom.core.repository.sqlalchemy import session_manager as session_manager_module
from loom.core.repository.sqlalchemy.session_manager import (
    SessionManager,
    build_session_manager,
)

_POSTGRES = "postgresql+asyncpg://app:secret@db.invalid/app"


def _pool_of(manager: SessionManager) -> Any:
    return manager.engine.sync_engine.pool


async def test_a_queue_pool_takes_the_resolved_values() -> None:
    config = DatabaseConfig(
        url=_POSTGRES,
        pool=PoolConfig(size=3, max_overflow=1, timeout=4, recycle=60, pre_ping=False),
    )
    manager = build_session_manager(config, SERVER_PROFILE)
    try:
        pool = _pool_of(manager)
        assert pool.size() == 3
        assert pool._max_overflow == 1
        assert pool._timeout == 4
        assert pool._recycle == 60
        assert pool._pre_ping is False
    finally:
        await manager.dispose()


async def test_undeclared_keys_come_from_the_profile() -> None:
    manager = build_session_manager(DatabaseConfig(url=_POSTGRES), WORKER_PROFILE)
    try:
        pool = _pool_of(manager)
        assert (pool.size(), pool._max_overflow, pool._timeout, pool._recycle) == (2, 0, 30, 1800)
        assert pool._pre_ping is True
    finally:
        await manager.dispose()


async def test_an_in_memory_sqlite_omits_the_sizing_keys_and_starts() -> None:
    manager = build_session_manager(DatabaseConfig(url="sqlite+aiosqlite:///"), SERVER_PROFILE)
    try:
        assert type(_pool_of(manager)).__name__ == "StaticPool"
    finally:
        await manager.dispose()


async def test_a_sqlite_file_takes_every_key(tmp_path: Path) -> None:
    config = DatabaseConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    manager = build_session_manager(config, SERVER_PROFILE)
    try:
        pool = _pool_of(manager)
        assert (pool.size(), pool._max_overflow, pool._timeout) == (5, 5, 10)
    finally:
        await manager.dispose()


def test_connect_args_and_echo_reach_the_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}
    real = session_manager_module.create_async_engine

    def capture(url: str, **kwargs: Any) -> Any:
        seen.update(kwargs)
        return real(url, **kwargs)

    monkeypatch.setattr(session_manager_module, "create_async_engine", capture)
    config = DatabaseConfig(url=_POSTGRES, echo=True, connect_args={"command_timeout": 5})

    build_session_manager(config, SERVER_PROFILE)

    assert seen["connect_args"] == {"command_timeout": 5}
    assert seen["echo"] is True


def test_manager_options_pass_through(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class _Recording(SessionManager):
        def __init__(self, url: str, **kwargs: Any) -> None:
            seen.update(kwargs)
            super().__init__(url, **kwargs)

    monkeypatch.setattr(session_manager_module, "SessionManager", _Recording)

    build_session_manager(DatabaseConfig(url=_POSTGRES), SERVER_PROFILE, inject_trace_id=False)

    assert seen["inject_trace_id"] is False


async def test_a_new_profile_applies_without_touching_the_contract() -> None:
    lambda_profile = RuntimeProfile(
        name="lambda",
        pool=PoolDefaults(size=1, max_overflow=0, timeout_s=5, recycle_s=300, pre_ping=True),
    )
    manager = build_session_manager(DatabaseConfig(url=_POSTGRES), lambda_profile)
    try:
        pool = _pool_of(manager)
        assert (pool.size(), pool._max_overflow, pool._timeout, pool._recycle) == (1, 0, 5, 300)
    finally:
        await manager.dispose()


async def test_a_caller_pool_class_is_honoured() -> None:
    manager = build_session_manager(
        DatabaseConfig(url=_POSTGRES), SERVER_PROFILE, poolclass=NullPool
    )
    try:
        assert type(_pool_of(manager)) is NullPool
    finally:
        await manager.dispose()
