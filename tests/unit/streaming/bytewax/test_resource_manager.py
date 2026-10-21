"""Tests for Bytewax resource caching."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.async_bridge import AsyncBridge  # noqa: E402
from loom.core.persistence.database import DatabaseConfig, PoolConfig  # noqa: E402
from loom.core.persistence.profile import STREAMING_PROFILE, RuntimeProfile  # noqa: E402
from loom.core.repository.sqlalchemy import session_manager as session_manager_module  # noqa: E402
from loom.streaming.bytewax._resource_manager import ResourceManager  # noqa: E402


class _FakeManager:
    async def dispose(self) -> None:
        await asyncio.sleep(0)
        return None


def test_session_manager_for_reuses_same_config(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[DatabaseConfig, RuntimeProfile]] = []

    def _build(config: DatabaseConfig, profile: RuntimeProfile) -> _FakeManager:
        calls.append((config, profile))
        return _FakeManager()

    monkeypatch.setattr(session_manager_module, "build_session_manager", _build)

    manager = ResourceManager(cast(AsyncBridge, object()))
    config = DatabaseConfig(url="sqlite+aiosqlite:///:memory:", connect_args={"timeout": 1})
    first = manager.session_manager_for(config)
    second = manager.session_manager_for(
        DatabaseConfig(url="sqlite+aiosqlite:///:memory:", connect_args={"timeout": 1})
    )

    assert first is second
    assert calls == [(config, STREAMING_PROFILE)]


def test_a_different_pool_gets_its_own_session_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        session_manager_module, "build_session_manager", lambda config, profile: _FakeManager()
    )
    manager = ResourceManager(cast(AsyncBridge, object()))

    first = manager.session_manager_for(DatabaseConfig(url="sqlite+aiosqlite:///"))
    second = manager.session_manager_for(
        DatabaseConfig(url="sqlite+aiosqlite:///", pool=PoolConfig(size=1))
    )

    assert first is not second
