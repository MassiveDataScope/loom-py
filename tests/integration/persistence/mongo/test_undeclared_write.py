"""Two Mongo stores on one replica set, each with a client of its own (ADR 0008, T046, T048).

The audit client's command listener sees every command sent through it.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

import pytest
from pymongo import AsyncMongoClient
from pymongo.monitoring import (
    CommandFailedEvent,
    CommandListener,
    CommandStartedEvent,
    CommandSucceededEvent,
)

from loom.core.errors import ForeignSessionError, UndeclaredStoreWrite
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWorkFactory
from loom.core.uow.coordinator import InvocationOutcome, OrderedCommits, RolledBack, StoreOutcome
from loom.core.uow.scope import Frame, invocation
from tests.unit.core.repository.contract.conftest import CreateOrder, Order, OrderStatus

pytestmark = [pytest.mark.integration, pytest.mark.mongo, pytest.mark.timeout(60)]

AUDIT = StoreKey("audit")


class _Commands(CommandListener):
    """Names of the commands a client started, in order."""

    def __init__(self) -> None:
        self.names: list[str] = []

    def started(self, event: CommandStartedEvent) -> None:
        self.names.append(event.command_name)

    def succeeded(self, event: CommandSucceededEvent) -> None:
        del event

    def failed(self, event: CommandFailedEvent) -> None:
        del event


@dataclass
class _Audit:
    client: AsyncMongoClient[Any]
    commands: _Commands
    orders: RepositoryMongo[Order, int]


@pytest.fixture
async def audit(mongo_database: Any) -> AsyncIterator[_Audit]:
    if mongo_database.skip_reason is not None:
        pytest.skip(mongo_database.skip_reason)
    commands = _Commands()
    client: AsyncMongoClient[Any] = AsyncMongoClient(
        mongo_database.uri,
        tz_aware=True,
        serverSelectionTimeoutMS=2_000,
        event_listeners=[commands],
    )
    collection = client[mongo_database.name][f"audit_{uuid4().hex}"]
    # A transaction cannot create the collection implicitly on 4.2.
    await collection.database.create_collection(collection.name)
    orders = RepositoryMongo(
        Order,
        collection,
        session_provider=MongoSessions(client, AUDIT),
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    commands.names.clear()
    try:
        yield _Audit(client, commands, orders)
    finally:
        await client.close()


def _coordinator(default: AsyncMongoClient[Any], audit: _Audit) -> OrderedCommits:
    return OrderedCommits(
        {
            DEFAULT_STORE: MongoUnitOfWorkFactory.transactional(default, store=DEFAULT_STORE),
            AUDIT: MongoUnitOfWorkFactory.transactional(audit.client, AUDIT),
        }
    )


def _order() -> CreateOrder:
    return CreateOrder(
        id=1,
        customer="c",
        amount=1,
        status=OrderStatus.PENDING,
        created_at=datetime(2026, 1, 1, 12, 0),
    )


async def test_an_undeclared_write_sends_no_command(
    mongo_client: AsyncMongoClient[Any], audit: _Audit
) -> None:
    order = _order()
    async with (
        invocation(_coordinator(mongo_client, audit)) as scope,
        scope.frame(Frame(DEFAULT_STORE)),
    ):
        with pytest.raises(UndeclaredStoreWrite, match="store 'audit'"):
            await audit.orders.create(order)

    assert audit.commands.names == []


async def test_a_joined_write_commits_with_the_invocation(
    mongo_client: AsyncMongoClient[Any], audit: _Audit
) -> None:
    async with (
        invocation(_coordinator(mongo_client, audit)) as scope,
        scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
    ):
        await audit.orders.create(_order())

    assert audit.commands.names[-2:] == ["insert", "commitTransaction"]
    assert await audit.orders.count() == 1


async def test_each_store_joins_a_session_of_its_own_client(
    mongo_client: AsyncMongoClient[Any], audit: _Audit
) -> None:
    async with (
        invocation(_coordinator(mongo_client, audit)) as scope,
        scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))),
    ):
        default = MongoSessions(mongo_client, store=DEFAULT_STORE).active_session
        joined = MongoSessions(audit.client, AUDIT).active_session
        assert default is not None
        assert joined is not None
        assert default.client is mongo_client
        assert joined.client is audit.client
        foreign = MongoSessions(audit.client, store=DEFAULT_STORE)
        with pytest.raises(ForeignSessionError):
            foreign(True)


async def test_a_commit_the_server_refuses_sends_no_abort(
    mongo_client: AsyncMongoClient[Any], audit: _Audit, caplog: pytest.LogCaptureFixture
) -> None:
    async with invocation(_audit_only(audit)) as scope:
        async with scope.frame(Frame(AUDIT)):
            await audit.orders.create(_order())
            await mongo_client.admin.command({"killSessions": [_lsid(audit)]})
        with caplog.at_level(logging.ERROR):
            outcome = await scope.coordinator.finish(scope, None)

    assert _outcome_types(outcome) == [RolledBack]
    assert audit.commands.names[-1] == "commitTransaction"
    assert "abortTransaction" not in audit.commands.names
    assert "UoWRollbackFailed" not in caplog.text
    assert await audit.orders.count() == 0


def _audit_only(audit: _Audit) -> OrderedCommits:
    return OrderedCommits({AUDIT: MongoUnitOfWorkFactory.transactional(audit.client, AUDIT)})


def _lsid(audit: _Audit) -> Any:
    session = MongoSessions(audit.client, AUDIT).active_session
    assert session is not None
    return session.session_id


def _outcome_types(outcome: InvocationOutcome) -> list[type[StoreOutcome]]:
    return [type(store) for store in outcome.stores]
