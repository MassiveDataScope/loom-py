"""``MongoSessions``: each Mongo store reads its own session through the invocation scope (D5)."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

from loom.core.errors import ScopeOwnershipError
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWork, MongoUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, invocation

from ._fake import FakeMongoClient
from .conftest import Article

AUDIT = StoreKey("audit")


def _client(fake: FakeMongoClient) -> AsyncMongoClient[Any]:
    return cast(AsyncMongoClient[Any], fake)


class _Stores:
    """Two transactional Mongo stores, each on a client of its own."""

    def __init__(self) -> None:
        self.default = FakeMongoClient()
        self.audit = FakeMongoClient()
        self.default_sessions = MongoSessions(_client(self.default), DEFAULT_STORE)
        self.audit_sessions = MongoSessions(_client(self.audit), AUDIT)

    def coordinator(self) -> OrderedCommits:
        return OrderedCommits(
            {
                DEFAULT_STORE: MongoUnitOfWorkFactory.transactional(
                    _client(self.default), DEFAULT_STORE
                ),
                AUDIT: MongoUnitOfWorkFactory.transactional(_client(self.audit), AUDIT),
            }
        )


# active_session itself, for both backends: ../test_active_session_per_store.py


class TestWriteIntent:
    async def test_a_write_from_a_gather_child_is_refused_even_when_not_enlisted(self) -> None:
        stores = _Stores()

        async def write() -> object:
            return stores.audit_sessions(True)

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            child = asyncio.gather(write())
            with pytest.raises(ScopeOwnershipError):
                await child

    async def test_a_write_marks_the_participant(self) -> None:
        stores = _Stores()

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            stores.default_sessions(True)
            wrote = scope.participants[DEFAULT_STORE].wrote

        assert wrote is True


def _repository(fake: FakeMongoClient) -> RepositoryMongo[Article, str]:
    return RepositoryMongo(
        Article,
        fake.collection("articles"),
        session_provider=MongoSessions(_client(fake), store=DEFAULT_STORE),
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


class TestRepositoryInAGather:
    async def test_reads_from_a_gather_inside_the_unit_of_work_are_refused(self) -> None:
        fake = FakeMongoClient()
        repository = _repository(fake)

        async with MongoUnitOfWork(_client(fake), store=DEFAULT_STORE):
            reads = asyncio.gather(repository.get_by_id("a"), repository.count())
            with pytest.raises(ScopeOwnershipError):
                await reads

    async def test_the_same_reads_awaited_in_the_owner_task_join_the_session(self) -> None:
        fake = FakeMongoClient()
        repository = _repository(fake)

        async with MongoUnitOfWork(_client(fake), store=DEFAULT_STORE):
            await repository.get_by_id("a")

        assert fake.collection("articles").last_session is fake.sessions[0]


class TestAStoreWithoutTransactions:
    """``transactions: false``: the unit of work holds no session, so there is none to protect."""

    @staticmethod
    def _coordinator() -> OrderedCommits:
        return OrderedCommits({DEFAULT_STORE: MongoUnitOfWorkFactory.without_transactions()})

    async def test_a_gather_of_reads_runs_without_a_session(self) -> None:
        fake = FakeMongoClient()
        repository = _repository(fake)

        async with invocation(self._coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            await asyncio.gather(repository.get_by_id("a"), repository.count())

        assert fake.collection("articles").last_session is None

    async def test_a_write_from_a_gather_child_is_refused(self) -> None:
        fake = FakeMongoClient()
        repository = _repository(fake)

        async with invocation(self._coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            child = asyncio.gather(repository.delete("a"))
            with pytest.raises(ScopeOwnershipError):
                await child
