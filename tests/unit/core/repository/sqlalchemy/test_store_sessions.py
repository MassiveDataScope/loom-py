"""Each store reads its own session through the invocation scope (ADR 0008, D5).

Two SQLite stores of the same backend run in one invocation: the session a
store's manager, its ``@transactional`` services and its elevation sink see
is always the one its own participant holds, never the other store's.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import QueuePool

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.errors.errors import ForeignSessionError
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.elevation import ScopedElevationSink
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec, PageParams
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy, with_session_scope
from loom.core.repository.sqlalchemy.rls import SQLAlchemyStoreElevation
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import transactional
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.coordinator import ROLLBACK_TIMEOUT_S, OrderedCommits
from loom.core.uow.scope import (
    Frame,
    InvocationScope,
    ScopeOwnershipError,
    UndeclaredStoreWrite,
    invocation,
)
from tests.helpers.elevation import declaring
from tests.unit.core.conftest import SessionManagerFactory

AUDIT = StoreKey("audit")
JOINED_AUDIT = Frame(DEFAULT_STORE, frozenset({AUDIT}))


def _manager(store: StoreKey, url: str = "sqlite+aiosqlite:///:memory:") -> SessionManager:
    return SessionManager(
        url,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
        store=store,
    )


@dataclass(frozen=True)
class _Stores:
    default: SessionManager
    audit: SessionManager

    def coordinator(self, rollback_timeout: float = ROLLBACK_TIMEOUT_S) -> OrderedCommits:
        return OrderedCommits(
            {
                DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(self.default),
                AUDIT: SQLAlchemyUnitOfWorkFactory(self.audit),
            },
            rollback_timeout=rollback_timeout,
        )


@pytest.fixture
async def stores() -> AsyncIterator[_Stores]:
    pair = _Stores(default=_manager(DEFAULT_STORE), audit=_manager(AUDIT))
    try:
        yield pair
    finally:
        await pair.default.dispose()
        await pair.audit.dispose()


def _handle(scope: InvocationScope, key: StoreKey) -> object:
    return scope.participants[key].handle


# active_session itself, for both backends: ../test_active_session_per_store.py


class _AuditService:
    def __init__(self, session_manager: SessionManager) -> None:
        self.session_manager = session_manager
        self.sessions: list[object] = []

    @transactional
    async def record(self) -> None:
        self.sessions.append(self.session_manager.active_session)


class TestTransactional:
    async def test_it_joins_the_participant_of_its_own_store(self, stores: _Stores) -> None:
        service = _AuditService(stores.audit)

        async with (
            invocation(stores.coordinator()) as scope,
            scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
        ):
            await service.record()
            expected = _handle(scope, AUDIT)

        assert service.sessions == [expected]

    async def test_a_store_outside_the_frame_is_refused(self, stores: _Stores) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            with pytest.raises(UndeclaredStoreWrite, match="'audit'"):
                await service.record()

        assert service.sessions == []

    async def test_a_store_that_is_not_enlisted_is_refused_even_without_a_frame(
        self, stores: _Stores
    ) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()):
            with pytest.raises(UndeclaredStoreWrite, match="'audit'.*this invocation"):
                await service.record()

        assert service.sessions == []

    async def test_without_any_store_enlisted_the_refusal_says_so(self, stores: _Stores) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()):
            with pytest.raises(UndeclaredStoreWrite, match="with no store enlisted"):
                await service.record()

    async def test_the_refusal_names_the_stores_this_invocation_enlisted(
        self, stores: _Stores
    ) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()) as scope:
            await scope.coordinator.enlist(scope, DEFAULT_STORE)
            with pytest.raises(UndeclaredStoreWrite, match="enlisted only store default$"):
                await service.record()

    async def test_the_refusal_never_prints_the_url_s_query(self, stores: _Stores) -> None:
        manager = _manager(AUDIT, "postgresql+asyncpg://loom@db.internal/audit?sslpassword=hunter2")
        service = _AuditService(manager)
        try:
            async with invocation(stores.coordinator()):
                with pytest.raises(UndeclaredStoreWrite) as raised:
                    await service.record()
        finally:
            await manager.dispose()

        assert "hunter2" not in str(raised.value)

    async def test_a_gather_child_is_refused(self, stores: _Stores) -> None:
        service = _AuditService(stores.default)

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            child = asyncio.gather(service.record())
            with pytest.raises(ScopeOwnershipError):
                await child

    async def test_outside_an_invocation_it_runs_in_one_of_its_own_store(
        self, stores: _Stores
    ) -> None:
        service = _AuditService(stores.audit)

        await service.record()

        assert len(service.sessions) == 1
        assert service.sessions[0] is not None


def _sink(stores: _Stores) -> ScopedElevationSink:
    """``editor`` elevable in the default store, ``reviewer`` in the audit store."""
    return ScopedElevationSink(
        {
            DEFAULT_STORE: declaring(stores.default, "editor"),
            AUDIT: declaring(stores.audit, "reviewer"),
        }
    )


class TestElevationSink:
    async def test_a_store_sees_the_transaction_of_its_own_store_only(
        self, stores: _Stores
    ) -> None:
        async with (
            invocation(stores.coordinator()) as scope,
            scope.frame(Frame(DEFAULT_STORE)),
        ):
            await _begin_transaction(stores.default)
            seen = (
                SQLAlchemyStoreElevation(stores.default).in_transaction(),
                SQLAlchemyStoreElevation(stores.audit).in_transaction(),
            )

        assert seen == (True, False)

    @pytest.mark.parametrize(
        ("begun", "expected"),
        [(DEFAULT_STORE, (True, False)), (AUDIT, (False, True))],
    )
    async def test_the_sink_sees_the_transaction_of_the_store_declaring_the_scope(
        self, stores: _Stores, begun: StoreKey, expected: tuple[bool, bool]
    ) -> None:
        sink = _sink(stores)
        async with (
            invocation(stores.coordinator()) as scope,
            scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
        ):
            await _begin_transaction(stores.default if begun == DEFAULT_STORE else stores.audit)
            seen = (
                sink.in_transaction(frozenset({"editor"})),
                sink.in_transaction(frozenset({"reviewer"})),
            )

        assert seen == expected

    async def test_the_sink_refuses_a_gather_child(self, stores: _Stores) -> None:
        sink = _sink(stores)

        async def child() -> bool:
            return sink.in_transaction(frozenset({"reviewer"}))

        async with (
            invocation(stores.coordinator()) as scope,
            scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
        ):
            pending = asyncio.gather(child())
            with pytest.raises(ScopeOwnershipError, match="task other than"):
                await pending


async def _begin_transaction(manager: SessionManager) -> None:
    session = manager.active_session
    assert session is not None
    await session.connection()


class _Row(BaseModel):
    __tablename__ = "store_session_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=32)


class _NewRow(Command, frozen=True):
    name: str


class _RowUpdate(Command, frozen=True):
    name: str


@pytest.fixture
async def rows(sqlite_manager_for: SessionManagerFactory) -> RepositorySQLAlchemy[_Row, int]:
    manager = await sqlite_manager_for(_Row)
    return RepositorySQLAlchemy(manager, _Row, cursor_codec=CursorCodec(signing="ephemeral"))


_OPERATIONS = {
    "get_by_id": lambda repo: repo.get_by_id(1),
    "count": lambda repo: repo.count(),
    "list_paginated": lambda repo: repo.list_paginated(PageParams(page=1, limit=5)),
    "create": lambda repo: repo.create(_NewRow(name="b")),
    "create_many": lambda repo: repo.create_many([_NewRow(name="b")]),
    "update": lambda repo: repo.update(1, _RowUpdate(name="c")),
    "delete": lambda repo: repo.delete(1),
}


class TestRepositoryWriteIntent:
    @pytest.mark.parametrize(
        ("operation", "writes"),
        [
            ("get_by_id", False),
            ("count", False),
            ("list_paginated", False),
            ("create", True),
            ("create_many", True),
            ("update", True),
            ("delete", True),
        ],
    )
    async def test_only_a_write_marks_the_participant(
        self, rows: RepositorySQLAlchemy[_Row, int], operation: str, writes: bool
    ) -> None:
        await rows.create(_NewRow(name="a"))
        coordinator = OrderedCommits(
            {DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(rows.session_manager)}
        )

        async with invocation(coordinator) as scope, scope.frame(Frame(DEFAULT_STORE)):
            await _OPERATIONS[operation](rows)
            wrote = scope.participants[DEFAULT_STORE].wrote

        assert wrote is writes


_INSERT = text("INSERT INTO store_session_rows (name) VALUES (:name)")
_COUNT = text("SELECT count(*) FROM store_session_rows")


class _AuditRows(RepositorySQLAlchemy[_Row, int]):
    @with_session_scope(write=True)
    async def insert_raw(self, session: AsyncSession, name: str) -> None:
        await session.execute(_INSERT, {"name": name})

    @with_session_scope(write=False)
    async def count_raw(self, session: AsyncSession) -> int:
        result = await session.execute(_COUNT)
        return int(result.scalar_one())

    @with_session_scope(write=False)
    async def first_instance(self, session: AsyncSession) -> Any:
        """An ORM instance, used once the session is gone."""
        return (await session.execute(select(self._effective_sa_model))).scalars().first()


class _InvocationFailed(Exception):
    pass


async def _rows_of(manager: SessionManager) -> _AuditRows:
    """The rows repository of ``manager``'s store, its table created."""
    compile_all(_Row)
    compiled = get_compiled(_Row)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    return _AuditRows(manager, _Row, cursor_codec=CursorCodec(signing="ephemeral"))


@pytest.fixture
async def audit_rows(stores: _Stores) -> _AuditRows:
    return await _rows_of(stores.audit)


class TestWithSessionScope:
    """A custom method declares its write intent (T079)."""

    async def test_a_custom_write_to_an_undeclared_store_is_refused_and_persists_nothing(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            with pytest.raises(UndeclaredStoreWrite, match="'audit'"):
                await audit_rows.insert_raw("outside")

        assert await audit_rows.count() == 0

    async def test_a_custom_write_to_a_joined_store_rolls_back_with_the_invocation(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        with pytest.raises(_InvocationFailed):
            await _write_joined_then_fail(stores, audit_rows)

        assert await audit_rows.count() == 0

    async def test_a_custom_write_to_a_joined_store_is_recorded_for_the_commit_order(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        async with invocation(stores.coordinator()) as scope, scope.frame(JOINED_AUDIT):
            await audit_rows.insert_raw("joined")
            wrote = scope.participants[AUDIT].wrote

        assert (wrote, await audit_rows.count()) == (True, 1)

    async def test_a_custom_read_of_an_undeclared_store_runs_in_a_session_of_its_own(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        await audit_rows.insert_raw("before")

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            counted = await audit_rows.count_raw()
            enlisted = AUDIT in scope.participants

        assert (counted, enlisted) == (1, False)


_READ_ONLY = Frame(None)


async def _insert(manager: SessionManager, name: str, *, write: bool = True) -> None:
    async with manager.operation_session(write=write) as session:
        await session.execute(_INSERT, {"name": name})


@asynccontextmanager
async def _no_invocation(stores: _Stores) -> AsyncIterator[None]:
    yield


@asynccontextmanager
async def _read_only_execution(stores: _Stores) -> AsyncIterator[None]:
    async with (
        invocation(stores.coordinator(), read_only=True) as scope,
        scope.frame(_READ_ONLY),
    ):
        yield


def _count_checkouts(manager: SessionManager) -> list[object]:
    checkouts: list[object] = []
    event.listen(manager.engine.sync_engine.pool, "checkout", lambda *args: checkouts.append(args))
    return checkouts


class TestOperationSession:
    """Raw SQL runs in the session its operation would run in as a repository's.

    What it shares with a repository's operations is covered by
    :class:`TestWithSessionScope`.
    """

    @pytest.mark.parametrize("write", [True, False])
    async def test_an_enlisted_store_yields_its_participant_s_session_and_records_a_write(
        self, stores: _Stores, audit_rows: _AuditRows, write: bool
    ) -> None:
        async with invocation(stores.coordinator()) as scope, scope.frame(JOINED_AUDIT):
            async with stores.audit.operation_session(write=write) as session:
                joined = session is stores.audit.active_session
            wrote = scope.participants[AUDIT].wrote

        assert (joined, wrote) == (True, write)

    async def test_a_read_only_execution_reads_in_a_transaction_of_its_own(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        await audit_rows.insert_raw("before")

        async with (
            invocation(stores.coordinator(), read_only=True) as scope,
            scope.frame(_READ_ONLY),
        ):
            async with stores.audit.operation_session(write=False) as session:
                counted = (await session.execute(_COUNT)).scalar_one()
            enlisted = AUDIT in scope.participants

        assert (counted, enlisted) == (1, False)

    async def test_a_read_only_execution_refuses_a_write_before_any_connection_is_used(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        checkouts = _count_checkouts(stores.audit)

        async with _read_only_execution(stores):
            with pytest.raises(UndeclaredStoreWrite, match="'audit'"):
                await _insert(stores.audit, "refused")

        assert checkouts == []

    @pytest.mark.parametrize("context", [_no_invocation, _read_only_execution])
    async def test_a_read_that_writes_persists_nothing(
        self,
        stores: _Stores,
        audit_rows: _AuditRows,
        context: Callable[[_Stores], AbstractAsyncContextManager[None]],
    ) -> None:
        async with context(stores):
            await _insert(stores.audit, "declared a read", write=False)

        assert await audit_rows.count() == 0

    async def test_an_instance_a_read_loaded_stays_usable_after_its_session(
        self, audit_rows: _AuditRows
    ) -> None:
        await audit_rows.insert_raw("kept")

        instance = await audit_rows.first_instance()

        assert instance.name == "kept"

    async def test_outside_an_invocation_it_persists_nothing_on_error(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        async def write_then_fail() -> None:
            async with stores.audit.operation_session(write=True) as session:
                await session.execute(_INSERT, {"name": "failed"})
                raise _InvocationFailed

        with pytest.raises(_InvocationFailed):
            await write_then_fail()

        assert await audit_rows.count() == 0

    async def test_a_cancelled_write_persists_nothing_and_returns_its_connection(
        self, tmp_path: Path
    ) -> None:
        manager = _manager(AUDIT, f"sqlite+aiosqlite:///{tmp_path / 'audit.db'}")
        try:
            rows = await _rows_of(manager)
            pool = manager.engine.sync_engine.pool
            assert isinstance(pool, QueuePool)
            started = asyncio.Event()

            async def write_then_wait() -> None:
                async with manager.operation_session(write=True) as session:
                    await session.execute(_INSERT, {"name": "cancelled"})
                    started.set()
                    await asyncio.Event().wait()

            task = asyncio.create_task(write_then_wait())
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            checked_out = pool.checkedout()

            assert (checked_out, await rows.count()) == (0, 0)
        finally:
            await manager.dispose()


class TestHandedInSession:
    """A ``session=`` passed by hand must come from the repository's own store (T105)."""

    async def test_a_session_of_another_store_is_refused_and_persists_nothing(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        default_rows = await _rows_of(stores.default)
        async with stores.default.session() as foreign:
            with pytest.raises(ForeignSessionError, match="store 'audit'"):
                await audit_rows.insert_raw("foreign", session=foreign)
            await foreign.commit()

        assert (await audit_rows.count(), await default_rows.count()) == (0, 0)

    async def test_a_session_of_another_engine_is_refused_without_its_secrets(
        self, audit_rows: _AuditRows
    ) -> None:
        manager = _manager(
            AUDIT, "postgresql+asyncpg://loom:s3cret@db.internal/x?sslpassword=hunter2"
        )
        try:
            async with manager.session() as foreign:
                with pytest.raises(ForeignSessionError, match="another engine") as raised:
                    await audit_rows.insert_raw("foreign", session=foreign)
        finally:
            await manager.dispose()

        message = str(raised.value)
        assert "s3cret" not in message
        assert "hunter2" not in message

    async def test_a_session_outside_the_invocation_is_refused_and_persists_nothing(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        with pytest.raises(_InvocationFailed):
            await _hand_in_another_session_then_fail(stores, audit_rows)

        assert await audit_rows.count() == 0

    async def test_a_session_of_its_own_store_is_used(
        self, stores: _Stores, audit_rows: _AuditRows
    ) -> None:
        async with stores.audit.session() as own:
            await audit_rows.insert_raw("own", session=own)
            await own.commit()

        assert await audit_rows.count() == 1


class TestCloseAfterRollback:
    """Close drops the connection only of a transaction its rollback left open."""

    @pytest.mark.parametrize(
        ("fails", "rollback_timeout", "invalidations"),
        [
            pytest.param(False, ROLLBACK_TIMEOUT_S, 0, id="committed"),
            pytest.param(True, ROLLBACK_TIMEOUT_S, 0, id="rolled-back"),
            pytest.param(True, 0.0, 1, id="rollback-out-of-time"),
        ],
    )
    async def test_only_a_transaction_left_open_is_invalidated(
        self,
        stores: _Stores,
        audit_rows: _AuditRows,
        monkeypatch: pytest.MonkeyPatch,
        fails: bool,
        rollback_timeout: float,
        invalidations: int,
    ) -> None:
        invalidated: list[AsyncSession] = []
        invalidate = AsyncSession.invalidate

        async def _spy(session: AsyncSession) -> None:
            invalidated.append(session)
            await invalidate(session)

        monkeypatch.setattr(AsyncSession, "invalidate", _spy)

        with suppress(_InvocationFailed):
            await _write_joined(stores.coordinator(rollback_timeout), audit_rows, fail=fails)

        assert len(invalidated) == invalidations


async def _write_joined(coordinator: OrderedCommits, audit: _AuditRows, *, fail: bool) -> None:
    async with invocation(coordinator) as scope, scope.frame(JOINED_AUDIT):
        await audit.insert_raw("joined")
        if fail:
            raise _InvocationFailed


async def _hand_in_another_session_then_fail(stores: _Stores, audit: _AuditRows) -> None:
    """Write through audit's participant, hand in another audit session and commit it, then fail."""
    async with invocation(stores.coordinator()) as scope, scope.frame(JOINED_AUDIT):
        await audit.insert_raw("joined")
        async with stores.audit.session() as other:
            with pytest.raises(ForeignSessionError, match="did not open"):
                await audit.insert_raw("other", session=other)
            await other.commit()
        raise _InvocationFailed


async def _write_joined_then_fail(stores: _Stores, audit: _AuditRows) -> None:
    """Inside an invocation that joins audit, write to it by hand, then fail."""
    async with invocation(stores.coordinator()) as scope, scope.frame(JOINED_AUDIT):
        await audit.insert_raw("joined")
        raise _InvocationFailed
