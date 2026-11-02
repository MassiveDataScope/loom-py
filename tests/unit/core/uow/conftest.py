"""Arrangements for the unit-of-work suite.

The session double and the recording session manager come from the shared
``tests/unit/core`` conftest; only what the unit-of-work tests alone need
lives here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.engine import make_url

from loom.core.persistence.stores import DEFAULT_STORE

from ..conftest import RecordingSessionManager


def make_session_manager(session: MagicMock) -> MagicMock:
    """Return a session manager double whose ``.session()`` yields ``session``."""
    sm = MagicMock()
    sm.store = DEFAULT_STORE
    sm.engine.url = make_url("sqlite+aiosqlite://")

    @asynccontextmanager
    async def _session_ctx() -> AsyncIterator[MagicMock]:
        yield session

    sm.session = _session_ctx
    return sm


@dataclass(frozen=True)
class SessionPair:
    """A session double together with the recording manager that hands it out."""

    session: MagicMock
    manager: RecordingSessionManager


@pytest.fixture
def broken_rollback(session_double: MagicMock) -> SessionPair:
    """A session whose ``rollback`` fails, with the manager that hands it out."""
    session_double.rollback = AsyncMock(side_effect=ConnectionError("pool gone"))
    return SessionPair(session=session_double, manager=RecordingSessionManager(session_double))
