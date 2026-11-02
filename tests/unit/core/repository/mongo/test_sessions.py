"""``MongoSessions``: each Mongo store reads its own session through the invocation scope (D5)."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

from loom.core.errors.errors import ForeignSessionError
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWork, MongoUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, ScopeOwnershipError, invocation

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


class TestActiveSession:
    def test_outside_an_invocation_there_is_none(self) -> None:
        assert _Stores().default_sessions.active_session is None

    async def test_each_store_reads_the_session_of_its_own_client(self) -> None:
        stores = _Stores()

        async with (
            invocation(stores.coordinator()) as scope,
            scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
        ):
            seen = (stores.default_sessions.active_session, stores.audit_sessions.active_session)

        assert seen == (stores.default.sessions[0], stores.audit.sessions[0])

    async def test_a_store_that_is_not_enlisted_has_none(self) -> None:
        stores = _Stores()

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            audit = stores.audit_sessions.active_session

        assert audit is None

    async def test_a_gather_child_reading_it_is_refused(self) -> None:
        stores = _Stores()

        async def read() -> object:
            return stores.default_sessions.active_session

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            child = asyncio.gather(read())
            with pytest.raises(ScopeOwnershipError):
                await child

    async def test_another_client_of_an_enlisted_store_is_refused(self) -> None:
        stores = _Stores()
        other = MongoSessions(_client(FakeMongoClient()), DEFAULT_STORE)

        async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
            with pytest.raises(ForeignSessionError, match="cannot join the active session"):
                _ = other.active_session


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
        session_provider=MongoSessions(_client(fake)),
        cursor_codec=CursorCodec(signing="ephemeral"),
    )


class TestRepositoryInAGather:
    async def test_reads_from_a_gather_inside_the_unit_of_work_are_refused(self) -> None:
        fake = FakeMongoClient()
        repository = _repository(fake)

        async with MongoUnitOfWork(_client(fake)):
            reads = asyncio.gather(repository.get_by_id("a"), repository.count())
            with pytest.raises(ScopeOwnershipError):
                await reads

    async def test_the_same_reads_awaited_in_the_owner_task_join_the_session(self) -> None:
        fake = FakeMongoClient()
        repository = _repository(fake)

        async with MongoUnitOfWork(_client(fake)):
            await repository.get_by_id("a")

        assert fake.collection("articles").last_session is fake.sessions[0]
