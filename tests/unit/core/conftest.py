"""Session doubles and SQLite session managers shared by the core suites.

The unit of work and the ``@transactional`` decorator both drive a session
through a session manager, so the controllable session and the manager that
records its scope exits live here, once, for both subtrees. The repository and
cache suites open real in-memory SQLite managers through one factory.

SQLAlchemy is imported only inside that factory: CI runs ``tests/unit/core/model``
with nothing but the ``testing`` extra, and this conftest loads for it too.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from loom.core.stores.catalog import DEFAULT_STORE

if TYPE_CHECKING:
    from loom.stores.sqlalchemy.session_manager import SessionManager

SessionManagerFactory = Callable[[type], Awaitable["SessionManager"]]


class RecordingSession:
    """A session double that records its transaction steps and fails or waits when scripted.

    Args:
        in_transaction: What ``in_transaction()`` answers.
        bind: What ``get_bind()`` answers, for the code that reads the engine of a session.
    """

    def __init__(self, *, in_transaction: bool = False, bind: Any = None) -> None:
        self.calls: list[str] = []
        self.is_in_transaction = in_transaction
        self._bind = bind
        self._scripted: dict[str, Callable[[], Awaitable[None]]] = {}

    def count(self, step: str) -> int:
        """How many times *step* ran."""
        return self.calls.count(step)

    def fail_on(self, step: str, error: BaseException) -> None:
        """Make *step* raise *error* every time it runs."""

        async def raise_error() -> None:
            raise error

        self._scripted[step] = raise_error

    def run_on(self, step: str, action: Callable[[], Awaitable[None]]) -> None:
        """Replace the body of *step* with *action*, which a test can make wait or fail."""
        self._scripted[step] = action

    def in_transaction(self) -> bool:
        return self.is_in_transaction

    def get_bind(self) -> Any:
        return self._bind

    async def flush(self) -> None:
        await self._run("flush")

    async def commit(self) -> None:
        await self._run("commit")

    async def rollback(self) -> None:
        await self._run("rollback")

    async def invalidate(self) -> None:
        await self._run("invalidate")

    async def close(self) -> None:
        await self._run("close")

    async def _run(self, step: str) -> None:
        self.calls.append(step)
        action = self._scripted.get(step)
        if action is not None:
            await action()


def make_session() -> RecordingSession:
    """Return a session double whose transaction methods are awaitable."""
    return RecordingSession()


class RecordingSessionManager:
    """Session manager double whose scope counts its exits and may fail to close.

    Args:
        session: Session yielded by the scope.
        exit_error: Raised when the scope closes, standing in for a broken pool.
        log: Appended with ``"session_closed"`` on every exit, so a test can
            order the close against the post-commit hooks.
    """

    store = "default"

    def __init__(
        self,
        session: Any,
        *,
        exit_error: Exception | None = None,
        log: list[str] | None = None,
        url: Any = None,
    ) -> None:
        self.engine = SimpleNamespace(
            url=url
            or SimpleNamespace(drivername="sqlite+aiosqlite", host=None, port=None, database=None)
        )
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
def session_double() -> RecordingSession:
    """A fresh session double that records ``commit``, ``rollback`` and the other steps."""
    return make_session()


@pytest.fixture
async def sqlite_manager_for() -> AsyncIterator[SessionManagerFactory]:
    """Open in-memory SQLite managers with a model's tables created.

    Every manager the factory opened is disposed when the test ends.
    """
    from loom.stores.sqlalchemy.schema import compile_all, get_compiled
    from loom.stores.sqlalchemy.session_manager import SessionManager

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
            store=DEFAULT_STORE,
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
