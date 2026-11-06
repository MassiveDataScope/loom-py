"""SQL→Mongo: each store's post-commit actions run by its own outcome (ADR 0008, T049).

The executor owns an invocation of a SQLite store and a Mongo store on the
replica set. The SQL store writes first, so it commits first; where a test
injects it, the Mongo commit fails and the server aborts the transaction.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import Any
from uuid import uuid4

import pytest
from pymongo import AsyncMongoClient

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import StoresNotCommitted
from loom.core.job.context import add_pending_dispatch
from loom.core.model import BaseModel, ColumnField
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWork
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import OrderedCommits
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
    ref: str = ColumnField(length=16)


class _New(Command, frozen=True):
    ref: str
    id: int | None = None


class _Refused(Exception):
    pass


class _RefusingCommit(MongoUnitOfWork):
    async def commit(self) -> None:
        raise _Refused


class _Mongo:
    def __init__(self, client: AsyncMongoClient[Any], refuse: bool) -> None:
        self._client, self._refuse = client, refuse

    def create(self) -> UnitOfWork:
        return (_RefusingCommit if self._refuse else MongoUnitOfWork)(self._client, AUDIT)


class _WriteEntry(UseCase[_Entry, None]):
    def __init__(self, entries: Any, ran: list[str]) -> None:
        self.entries, self.ran = entries, ran

    async def execute(self) -> None:
        await self.entries.create(_New(ref="mongo", id=1))
        add_pending_dispatch(lambda: self.ran.append("mongo"))


class _WriteOrderThenEntry(UseCase[_Order, None]):
    def __init__(self, executor: RuntimeExecutor, orders: Any, nested: _WriteEntry) -> None:
        self.executor, self.orders, self.nested = executor, orders, nested

    async def execute(self) -> None:
        await self.orders.create(_New(ref="sql"))
        add_pending_dispatch(lambda: self.nested.ran.append("sql"))
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


@pytest.mark.parametrize(
    ("refuse", "ran", "error", "stored"),
    [
        pytest.param(False, ["sql", "mongo"], None, 1, id="both-commit"),
        pytest.param(True, ["sql"], StoresNotCommitted, 0, id="mongo-rolls-back"),
    ],
)
async def test_each_store_runs_its_actions_by_its_own_outcome(
    sql: SessionManager,
    mongo_client: AsyncMongoClient[Any],
    mongo_database: Any,
    refuse: bool,
    ran: list[str],
    error: type[BaseException] | None,
    stored: int,
) -> None:
    collection = mongo_client[mongo_database.name][f"entries_{uuid4().hex}"]
    # A transaction cannot create the collection implicitly on 4.2.
    await collection.database.create_collection(collection.name)
    codec = CursorCodec(signing="ephemeral")
    entries = RepositoryMongo(
        _Entry, collection, session_provider=MongoSessions(mongo_client, AUDIT), cursor_codec=codec
    )
    coordinator = OrderedCommits(
        {DEFAULT_STORE: SQLAlchemyUnitOfWorkFactory(sql), AUDIT: _Mongo(mongo_client, refuse)}
    )
    executor = RuntimeExecutor(UseCaseCompiler(), coordinator=coordinator)
    labels: list[str] = []
    orders = RepositorySQLAlchemy(sql, _Order, cursor_codec=codec)
    use_case = _WriteOrderThenEntry(executor, orders, _WriteEntry(entries, labels))

    with nullcontext() if error is None else pytest.raises(error):
        await executor.execute(use_case)

    assert (labels, await entries.count()) == (ran, stored)
