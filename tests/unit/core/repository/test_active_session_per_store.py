"""``active_session``: each store reads its own session through the invocation scope (D5).

The same contract for every backend that holds a session: SQLAlchemy's
``SessionManager.active_session`` and Mongo's ``MongoSessions.active_session``.
Two stores of one backend run in one invocation.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

from loom.core.errors.errors import ForeignSessionError
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWorkFactory
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, InvocationScope, ScopeOwnershipError, invocation

from .mongo._fake import FakeMongoClient

AUDIT = StoreKey("audit")
BOTH = Frame(DEFAULT_STORE, frozenset({AUDIT}))


@dataclass(frozen=True)
class _Stores:
    """Two stores of one backend: how to read each one's session, and their coordinator."""

    read: dict[StoreKey, Callable[[], object]]
    foreign: Callable[[], object]
    """Reads the default store's session through another manager or client."""
    coordinator: Callable[[], OrderedCommits]


def _sqlite(store: StoreKey) -> SessionManager:
    return SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        store=store,
    )


def _sqlalchemy_stores(managers: dict[StoreKey, SessionManager]) -> _Stores:
    other = managers[StoreKey("other")]
    return _Stores(
        read={key: (lambda m=managers[key]: m.active_session) for key in (DEFAULT_STORE, AUDIT)},
        foreign=lambda: other.active_session,
        coordinator=lambda: OrderedCommits(
            {key: SQLAlchemyUnitOfWorkFactory(managers[key]) for key in (DEFAULT_STORE, AUDIT)}
        ),
    )


def _mongo_stores() -> _Stores:
    clients = {
        key: cast(AsyncMongoClient[Any], FakeMongoClient()) for key in (DEFAULT_STORE, AUDIT)
    }
    sessions = {key: MongoSessions(client, key) for key, client in clients.items()}
    other = MongoSessions(cast(AsyncMongoClient[Any], FakeMongoClient()), DEFAULT_STORE)
    return _Stores(
        read={key: (lambda s=sessions[key]: s.active_session) for key in sessions},
        foreign=lambda: other.active_session,
        coordinator=lambda: OrderedCommits(
            {key: MongoUnitOfWorkFactory.transactional(clients[key], key) for key in clients}
        ),
    )


@pytest.fixture(params=["sqlalchemy", "mongo"])
async def stores(request: pytest.FixtureRequest) -> AsyncIterator[_Stores]:
    if request.param == "mongo":
        yield _mongo_stores()
        return
    managers = {key: _sqlite(key) for key in (DEFAULT_STORE, AUDIT)}
    managers[StoreKey("other")] = _sqlite(DEFAULT_STORE)
    try:
        yield _sqlalchemy_stores(managers)
    finally:
        for manager in managers.values():
            await manager.dispose()


def _handles(scope: InvocationScope) -> tuple[object, object]:
    return scope.participants[DEFAULT_STORE].handle, scope.participants[AUDIT].handle


def test_outside_an_invocation_there_is_none(stores: _Stores) -> None:
    assert stores.read[DEFAULT_STORE]() is None


async def test_each_store_reads_the_session_of_its_own_participant(stores: _Stores) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(BOTH):
        seen = (stores.read[DEFAULT_STORE](), stores.read[AUDIT]())
        expected = _handles(scope)

    assert seen == expected


async def test_each_participant_holds_a_session(stores: _Stores) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(BOTH):
        default, audit = _handles(scope)

    assert (default is not None, audit is not None) == (True, True)


async def test_the_two_stores_never_share_a_session(stores: _Stores) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(BOTH):
        default, audit = stores.read[DEFAULT_STORE](), stores.read[AUDIT]()

    assert default is not audit


async def test_a_store_that_is_not_enlisted_has_none(stores: _Stores) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
        audit = stores.read[AUDIT]()

    assert audit is None


async def test_a_gather_child_reading_it_is_refused(stores: _Stores) -> None:
    async def read() -> object:
        return stores.read[DEFAULT_STORE]()

    async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
        child = asyncio.gather(read())
        with pytest.raises(ScopeOwnershipError):
            await child


async def test_another_owner_of_an_enlisted_store_is_refused(stores: _Stores) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
        with pytest.raises(ForeignSessionError, match="cannot join the active session"):
            stores.foreign()
