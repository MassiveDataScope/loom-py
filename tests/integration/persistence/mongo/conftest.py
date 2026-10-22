"""Contract suite (FR-014) against a real MongoDB replica set (US2, SC-002).

The unit lane runs the contract over an in-memory fake; this lane runs the
very same test functions against the ``mongo`` service of
``docker-compose.local.yaml``. Each ``test_*.py`` module here re-exports one
contract module with ``from ... import *`` so a contract test added later
runs here without a copy, and this conftest overrides the ``case`` fixture
with a real-server :class:`BackendCase` (``transactions`` on, so the
rollback test runs for real). ``repository`` and ``seeded`` are the contract
fixtures themselves, re-exported so they resolve against that ``case``.

Gating: the server is probed once per session with a short server-selection
timeout; unreachable means every test skips at once, naming the command that
starts it. Every test gets its own collection inside one database named per
session, dropped at session end.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from pymongo import AsyncMongoClient, MongoClient
from pymongo.errors import PyMongoError

from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import MongoUnitOfWork, active_session
from loom.core.repository.registration import capabilities_of
from tests.unit.core.repository.contract import conftest as contract
from tests.unit.core.repository.contract.conftest import BackendCase, Order

URI_ENV_VAR = "LOOM_MONGO_IT_URI"
DEFAULT_URI = "mongodb://localhost:27017/?replicaSet=rs0"
_SERVER_SELECTION_TIMEOUT_MS = 2_000

repository = contract.repository
seeded = contract.seeded


@dataclass(frozen=True)
class _SessionDatabase:
    """The database of this test session; ``skip_reason`` is set when unreachable."""

    uri: str
    name: str
    skip_reason: str | None


@pytest.fixture(scope="session")
def mongo_database() -> Iterator[_SessionDatabase]:
    uri = os.environ.get(URI_ENV_VAR, DEFAULT_URI)
    name = f"loom_it_{uuid4().hex}"
    with MongoClient(uri, serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS) as client:
        database = _SessionDatabase(uri, name, _probe(client, uri))
        yield database
        if database.skip_reason is None:
            client.drop_database(name)


def redact_uri(uri: str) -> str:
    """Return ``uri`` with its password masked, for messages that may be logged."""
    from loom.core.model.redaction import mask_dsn

    return mask_dsn(uri)


def _probe(client: MongoClient[Any], uri: str) -> str | None:
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        return (
            f"MongoDB not reachable at {redact_uri(uri)} ({type(exc).__name__}) — start it "
            "with 'docker compose -f docker-compose.local.yaml up -d mongo' or point "
            f"{URI_ENV_VAR} at a replica set"
        )
    return None


@pytest.fixture
async def mongo_client(mongo_database: _SessionDatabase) -> AsyncIterator[AsyncMongoClient[Any]]:
    """A client per test: pymongo binds its sockets to the running event loop."""
    if mongo_database.skip_reason is not None:
        pytest.skip(mongo_database.skip_reason)
    client: AsyncMongoClient[Any] = AsyncMongoClient(
        mongo_database.uri, tz_aware=True, serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
def case(mongo_client: AsyncMongoClient[Any], mongo_database: _SessionDatabase) -> BackendCase:
    collection = mongo_client[mongo_database.name][f"{Order.__tablename__}_{uuid4().hex}"]

    @asynccontextmanager
    async def open_repository() -> AsyncIterator[RepositoryMongo[Order, int]]:
        yield RepositoryMongo(
            Order,
            collection,
            session_provider=active_session,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )

    return BackendCase(
        name="mongo",
        repository_factory=open_repository,
        model=Order,
        capabilities=frozenset(capabilities_of(RepositoryMongo)),
        transactions=True,
        unit_of_work=lambda _repository: MongoUnitOfWork(mongo_client),
    )
