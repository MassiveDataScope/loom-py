"""SQL→Mongo: each store's post-commit actions run by its own outcome (ADR 0008, T049, D4).

The executor owns an invocation of a SQLite store and a Mongo store on the
replica set. The SQL store writes first, so it commits first; the Mongo
commit fails where a test injects it, after which the server aborts the
transaction for real.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractContextManager, nullcontext
from typing import Any, cast
from uuid import uuid4

import pytest
from pymongo import AsyncMongoClient

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import store_channel
from loom.core.job.context import add_pending_dispatch, add_pending_publish
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec, RepoFor
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWork
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import CommitUncertain, OrderedCommits
from loom.core.uow.scope import StoresNotCommitted
from loom.core.use_case import UseCase

pytestmark = [pytest.mark.integration, pytest.mark.mongo, pytest.mark.timeout(60)]

AUDIT = StoreKey("audit")


class _Order(BaseModel):
    __tablename__ = "sql_mongo_orders"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: str = ColumnField(length=16)


class _Entry(BaseModel):
    __tablename__ = "sql_mongo_entries"
    __store__ = "audit"

    id: int = ColumnField(primary_key=True)
    note: str = ColumnField(length=16)


class _NewOrder(Command, frozen=True):
    ref: str


class _NewEntry(Command, frozen=True):
    id: int
    note: str


class _CommitRefused(Exception):
    pass


class _MongoCommitFails(MongoUnitOfWork):
    """A Mongo unit of work whose commit raises ``error``; the coordinator then aborts it."""

    def __init__(self, client: AsyncMongoClient[Any], error: BaseException) -> None:
        super().__init__(client, AUDIT)
        self._error = error

    async def commit(self) -> None:
        raise self._error


class _MongoFactory:
    def __init__(self, client: AsyncMongoClient[Any], error: BaseException | None) -> None:
        self._client, self._error = client, error

    def create(self) -> UnitOfWork:
        if self._error is None:
            return MongoUnitOfWork(self._client, AUDIT)
        return _MongoCommitFails(self._client, self._error)


def _queue(ran: list[str], store: str) -> None:
    """Queue one action per lane for the running execution's store."""
    channel = store_channel()
    assert channel is not None
    channel.enqueue_priority(lambda: ran.append(f"{store}.priority"))
    add_pending_publish(lambda: ran.append(f"{store}.publish"))
    add_pending_dispatch(lambda: ran.append(f"{store}.plain"))


class _WriteEntry(UseCase[_Entry, None]):
    """Primary store ``audit`` (Mongo): writes an entry and queues its actions."""

    def __init__(self, entries: RepoFor[_Entry], ran: list[str]) -> None:
        self.entries = entries
        self.ran = ran

    async def execute(self) -> None:
        await self.entries.create(_NewEntry(id=1, note="mongo"))
        _queue(self.ran, "mongo")


class _WriteOrderThenEntry(UseCase[_Order, None]):
    """Primary store ``default`` (SQL): writes an order, then runs :class:`_WriteEntry`."""

    def __init__(
        self, executor: RuntimeExecutor, orders: RepoFor[_Order], nested: _WriteEntry
    ) -> None:
        self.executor = executor
        self.orders = orders
        self.nested = nested

    async def execute(self) -> None:
        await self.orders.create(_NewOrder(ref="sql"))
        _queue(self.nested.ran, "sql")
        await self.executor.execute(self.nested)


@pytest.fixture
async def sql() -> AsyncIterator[SessionManager]:
    compile_all(_Order)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        store=DEFAULT_STORE,
    )
    compiled = get_compiled(_Order)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    try:
        yield manager
    finally:
        await manager.dispose()


@pytest.fixture
async def entries(
    mongo_client: AsyncMongoClient[Any], mongo_database: Any
) -> RepositoryMongo[_Entry, int]:
    collection = mongo_client[mongo_database.name][f"entries_{uuid4().hex}"]
    # A transaction cannot create the collection implicitly on 4.2.
    await collection.database.create_collection(collection.name)
    return RepositoryMongo(
        _Entry,
        collection,
        session_provider=MongoSessions(mongo_client, AUDIT),
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


def _all_lanes(*stores: str) -> list[str]:
    return [f"{store}.{lane}" for lane in ("priority", "publish", "plain") for store in stores]


def _raises(error: type[BaseException] | None) -> AbstractContextManager[object]:
    return nullcontext() if error is None else pytest.raises(error)


@pytest.mark.parametrize(
    ("mongo_error", "ran", "error", "entries_left"),
    [
        pytest.param(None, _all_lanes("sql", "mongo"), None, 1, id="both-commit"),
        pytest.param(
            _CommitRefused(), _all_lanes("sql"), StoresNotCommitted, 0, id="mongo-rolls-back"
        ),
        pytest.param(
            CommitUncertain(ConnectionError("ack lost")),
            ["sql.priority", "mongo.priority", "sql.publish", "sql.plain"],
            StoresNotCommitted,
            0,
            id="mongo-uncertain",
        ),
    ],
)
async def test_each_store_runs_what_its_outcome_lets_run(
    sql: SessionManager,
    entries: RepositoryMongo[_Entry, int],
    mongo_client: AsyncMongoClient[Any],
    mongo_error: BaseException | None,
    ran: list[str],
    error: type[BaseException] | None,
    entries_left: int,
) -> None:
    executor = RuntimeExecutor(UseCaseCompiler())
    # Until the composition builds it (PR 11), one coordinator for both stores.
    executor._coordinator = OrderedCommits(
        {
            DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(sql),
            AUDIT: _MongoFactory(mongo_client, mongo_error),
        }
    )
    codec = CursorCodec(signing="ephemeral")
    orders = cast("RepoFor[_Order]", RepositorySQLAlchemy(sql, _Order, cursor_codec=codec))
    labels: list[str] = []
    nested = _WriteEntry(cast("RepoFor[_Entry]", entries), labels)
    use_case = _WriteOrderThenEntry(executor, orders, nested)

    with _raises(error):
        await executor.execute(use_case)

    assert labels == ran
    assert await entries.count() == entries_left
