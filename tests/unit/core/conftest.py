"""Session doubles and SQLite session managers shared by the core suites.

The unit of work and the ``@transactional`` decorator both drive a session
through a session manager, so the controllable session and the manager that
records its scope exits live here, once, for both subtrees. The repository and
cache suites open real in-memory SQLite managers through one factory.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.repository.sqlalchemy.session_manager import SessionManager

SessionManagerFactory = Callable[[type], Awaitable[SessionManager]]


def make_session() -> MagicMock:
    """Return a session double whose ``commit``/``rollback``/``close`` are awaitable."""
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


class RecordingSessionManager:
    """Session manager double whose scope counts its exits and may fail to close.

    Args:
        session: Session yielded by the scope.
        exit_error: Raised when the scope closes, standing in for a broken pool.
        log: Appended with ``"session_closed"`` on every exit, so a test can
            order the close against the post-commit hooks.
    """

    def __init__(
        self,
        session: Any,
        *,
        exit_error: Exception | None = None,
        log: list[str] | None = None,
    ) -> None:
        self._session = session
        self._exit_error = exit_error
        self._log = log
        self.exits = 0

    def session(self) -> RecordingSessionManager:
        return self

    async def __aenter__(self) -> Any:
        return self._session

    async def __aexit__(self, *args: object) -> None:
        self.exits += 1
        if self._log is not None:
            self._log.append("session_closed")
        if self._exit_error is not None:
            raise self._exit_error


@pytest.fixture
def session_double() -> MagicMock:
    """A fresh session double with awaitable ``commit``/``rollback``/``close``."""
    return make_session()


@pytest.fixture
async def sqlite_session_manager() -> AsyncIterator[SessionManagerFactory]:
    """Open in-memory SQLite managers with a model's tables created.

    Every manager the factory opened is disposed when the test ends.
    """
    managers: list[SessionManager] = []

    async def open_for(model: type) -> SessionManager:
        compile_all(model)
        manager = SessionManager(
            "sqlite+aiosqlite:///:memory:",
            pool_size=None,
            max_overflow=None,
            pool_timeout=None,
            pool_recycle=None,
            connect_args={},
        )
        managers.append(manager)
        compiled = get_compiled(model)
        assert compiled is not None
        async with manager.engine.begin() as connection:
            await connection.run_sync(compiled.metadata.create_all)
        return manager

    yield open_for
    for manager in managers:
        await manager.dispose()
