"""Every SQLAlchemy store opens no transaction as a caller past its delegation (T09a)."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text

from loom.core.errors import Unauthenticated
from loom.core.identity import Identity
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.helpers.delegation import acting_as

_ALICE = Identity(subject="acme-alice", mechanism="test")


@pytest.fixture
async def manager() -> AsyncIterator[SessionManager]:
    """A store with no row-level security and no session settings."""
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        store=DEFAULT_STORE,
    )
    yield manager
    await manager.dispose()


class TestATransactionBegin:
    async def test_runs_within_the_caller_s_delegation(self, manager: SessionManager) -> None:
        with acting_as(_ALICE):
            async with manager.session() as session:
                assert (await session.execute(text("SELECT 1"))).scalar() == 1

    async def test_is_refused_past_the_caller_s_deadline(self, manager: SessionManager) -> None:
        with acting_as(_ALICE, until=time.time() - 1):
            async with manager.session() as session:
                with pytest.raises(Unauthenticated, match="system identity"):
                    await session.execute(text("SELECT 1"))
