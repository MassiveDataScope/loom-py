"""The executor's invocation scope against a real Postgres (ADR 0008, T045a).

Every write goes through the session the scope's unit of work publishes, and
every check reads from a separate connection: what is asserted is what
another client of the database sees.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import get_active_session
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.transaction import in_atomic_transaction
from loom.core.uow.scope import ScopeOwnershipError, current_scope
from tests.integration.support.scope_doubles import BusinessError, Run


@pytest.fixture
async def table(pg_admin_uri: str) -> AsyncIterator[str]:
    name = f"loom_it_scope_{uuid4().hex[:12]}"
    engine = create_async_engine(pg_admin_uri, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"CREATE TABLE {name}"
                " (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED, payload text)"
            )
        )
    try:
        yield name
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
        await engine.dispose()


@pytest.fixture
async def manager(pg_admin_uri: str) -> AsyncIterator[SessionManager]:
    session_manager = SessionManager(
        pg_admin_uri, pool_size=2, max_overflow=0, pool_timeout=5, pool_recycle=None
    )
    try:
        yield session_manager
    finally:
        await session_manager.dispose()


@pytest.fixture
async def observer(pg_admin_uri: str) -> AsyncIterator[AsyncEngine]:
    """A separate engine: it sees only what was committed."""
    engine = create_async_engine(pg_admin_uri, poolclass=NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


def _executor(manager: SessionManager) -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler(), uow_factory=SQLAlchemyUnitOfWorkFactory(manager))


async def _insert(table: str, row_id: int) -> None:
    session = get_active_session()
    assert session is not None
    await session.execute(
        text(f"INSERT INTO {table} (id, payload) VALUES (:id, 'p')"), {"id": row_id}
    )


async def _committed_ids(observer: AsyncEngine, table: str) -> list[int]:
    async with observer.connect() as conn:
        rows = await conn.execute(text(f"SELECT id FROM {table} ORDER BY id"))
        return [row[0] for row in rows]


async def test_a_clean_execution_commits_its_write(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    async def body() -> None:
        await _insert(table, 1)

    await _executor(manager).execute(Run(body))

    assert await _committed_ids(observer, table) == [1]


async def test_a_failure_after_writing_persists_nothing(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    async def body() -> None:
        await _insert(table, 1)
        raise BusinessError

    executor = _executor(manager)
    use_case = Run(body)

    with pytest.raises(BusinessError):
        await executor.execute(use_case)

    assert await _committed_ids(observer, table) == []


async def test_a_nested_execution_does_not_commit(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    executor = _executor(manager)
    seen_after_nested: list[list[int]] = []

    async def nested() -> None:
        await _insert(table, 1)

    async def outer() -> None:
        await executor.execute(Run(nested))
        seen_after_nested.append(await _committed_ids(observer, table))

    await executor.execute(Run(outer))

    assert seen_after_nested == [[]]


async def test_a_failure_after_a_nested_write_persists_nothing(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    executor = _executor(manager)

    async def nested() -> None:
        await _insert(table, 1)

    async def outer() -> None:
        await executor.execute(Run(nested))
        raise BusinessError

    use_case = Run(outer)

    with pytest.raises(BusinessError):
        await executor.execute(use_case)

    assert await _committed_ids(observer, table) == []


async def test_a_failed_commit_raises_the_driver_error_and_persists_nothing(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    """One store raises what it raised before the scope: the driver's error, at commit."""
    executor = _executor(manager)

    async def first() -> None:
        await _insert(table, 1)

    async def duplicate() -> None:
        await _insert(table, 2)
        await _insert(table, 1)  # the deferred key fails only at COMMIT

    await executor.execute(Run(first))
    use_case = Run(duplicate)

    with pytest.raises(IntegrityError):
        await executor.execute(use_case)

    assert await _committed_ids(observer, table) == [1]


async def test_a_gather_child_running_a_nested_execution_writes_nothing(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    executor = _executor(manager)

    async def nested() -> None:
        await _insert(table, 2)

    async def outer() -> None:
        await _insert(table, 1)
        await asyncio.gather(executor.execute(Run(nested)))

    use_case = Run(outer)

    with pytest.raises(ScopeOwnershipError):
        await executor.execute(use_case)

    assert await _committed_ids(observer, table) == []


async def test_a_hundred_invocations_return_every_connection_and_reset_the_context(
    manager: SessionManager, table: str, observer: AsyncEngine
) -> None:
    executor = _executor(manager)
    clean: list[bool] = []

    async def invoke(index: int) -> None:
        async def body() -> None:
            await _insert(table, index)
            if index % 3 == 1:
                raise BusinessError
            if index % 3 == 2:
                await asyncio.Event().wait()

        try:
            await executor.execute(Run(body))
        except BusinessError:
            pass
        finally:
            clean.append(_context_is_clean())

    for index in range(100):
        task = asyncio.create_task(invoke(index))
        if index % 3 == 2:
            while manager.engine.pool.checkedout() == 0:  # type: ignore[attr-defined]
                await asyncio.sleep(0.001)
            await asyncio.sleep(0.01)
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert manager.engine.pool.checkedout() == 0  # type: ignore[attr-defined]
    assert clean == [True] * 100
    assert await _committed_ids(observer, table) == list(range(0, 100, 3))


def _context_is_clean() -> bool:
    return current_scope() is None and get_active_session() is None and not in_atomic_transaction()
