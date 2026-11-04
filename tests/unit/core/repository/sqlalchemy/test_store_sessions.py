"""Each store reads its own session through the invocation scope (ADR 0008, D5).

Two SQLite stores of the same backend run in one invocation: the session a
store's manager, its ``@transactional`` services and its elevation sink see
is always the one its own participant holds, never the other store's.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, InvocationScope, ScopeOwnershipError, invocation
from tests.helpers.elevation import declaring
from tests.unit.core.conftest import SessionManagerFactory

AUDIT = StoreKey("audit")


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

    def coordinator(self) -> OrderedCommits:
        return OrderedCommits(
            {
                DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(self.default),
                AUDIT: SQLAlchemyUnitOfWorkFactory(self.audit),
            }
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
            with pytest.raises(ForeignSessionError, match="'audit'"):
                await service.record()

        assert service.sessions == []

    async def test_a_store_that_is_not_enlisted_is_refused_even_without_a_frame(
        self, stores: _Stores
    ) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()):
            with pytest.raises(ForeignSessionError, match="'audit'.*this invocation"):
                await service.record()

        assert service.sessions == []

    async def test_without_any_store_enlisted_the_refusal_says_so(self, stores: _Stores) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()):
            with pytest.raises(ForeignSessionError, match="with no store enlisted"):
                await service.record()

    async def test_the_refusal_names_the_stores_this_invocation_enlisted(
        self, stores: _Stores
    ) -> None:
        service = _AuditService(stores.audit)

        async with invocation(stores.coordinator()) as scope:
            await scope.coordinator.enlist(scope, DEFAULT_STORE)
            with pytest.raises(ForeignSessionError, match="store 'default' \\(this invocation\\)"):
                await service.record()

    async def test_the_refusal_never_prints_the_url_s_query(self, stores: _Stores) -> None:
        manager = _manager(AUDIT, "postgresql+asyncpg://loom@db.internal/audit?sslpassword=hunter2")
        service = _AuditService(manager)
        try:
            async with invocation(stores.coordinator()):
                with pytest.raises(ForeignSessionError) as raised:
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


class _AuditRows(RepositorySQLAlchemy[_Row, int]):
    @with_session_scope
    async def insert_raw(self, session: AsyncSession, name: str) -> None:
        await session.execute(
            text("INSERT INTO store_session_rows (name) VALUES (:name)"), {"name": name}
        )


class _InvocationFailed(Exception):
    pass


async def _write_outside_then_fail(stores: _Stores, audit: _AuditRows) -> None:
    """Inside an invocation of the default store, write to audit by hand, then fail."""
    async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
        await audit.insert_raw("outside")
        raise _InvocationFailed


class TestWithSessionScope:
    async def test_a_custom_write_to_a_store_not_enlisted_commits_per_call_today(
        self, stores: _Stores
    ) -> None:
        """Pins today's behaviour: T079 (PR 6) refuses this write instead."""
        compile_all(_Row)
        compiled = get_compiled(_Row)
        assert compiled is not None
        async with stores.audit.engine.begin() as connection:
            await connection.run_sync(compiled.metadata.create_all)
        audit = _AuditRows(stores.audit, _Row, cursor_codec=CursorCodec(signing="ephemeral"))

        with pytest.raises(_InvocationFailed):
            await _write_outside_then_fail(stores, audit)

        assert await audit.count() == 1
