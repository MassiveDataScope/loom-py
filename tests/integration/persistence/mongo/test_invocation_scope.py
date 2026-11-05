"""The executor's invocation scope against a real MongoDB replica set (ADR 0008, T045a).

Writes go through the client session the scope's unit of work publishes;
checks read without a session, so they see only what was committed.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWorkFactory
from loom.core.transaction import in_atomic_transaction
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import ScopeOwnershipError, current_scope
from tests.integration.support.scope_doubles import BusinessError, Run


@pytest.fixture
def collection(mongo_client: AsyncMongoClient[Any], mongo_database: Any) -> AsyncCollection[Any]:
    return mongo_client[mongo_database.name][f"scope_{uuid4().hex}"]


@pytest.fixture
async def created(collection: AsyncCollection[Any]) -> AsyncCollection[Any]:
    """The collection, created up front: a transaction cannot create it implicitly on 4.2."""
    await collection.database.create_collection(collection.name)
    return collection


def _executor(client: AsyncMongoClient[Any]) -> RuntimeExecutor:
    return RuntimeExecutor(
        UseCaseCompiler(),
        coordinator=OrderedCommits(
            {DEFAULT_STORE: MongoUnitOfWorkFactory.transactional(client, store=DEFAULT_STORE)}
        ),
    )


def _sessions(collection: AsyncCollection[Any]) -> MongoSessions:
    return MongoSessions(collection.database.client, store=DEFAULT_STORE)


async def _insert(collection: AsyncCollection[Any], row_id: int) -> None:
    session = _sessions(collection)(True)
    assert session is not None
    await collection.insert_one({"_id": row_id}, session=session)


async def _committed_ids(collection: AsyncCollection[Any]) -> list[int]:
    return [document["_id"] async for document in collection.find({}, sort=[("_id", 1)])]


async def test_a_clean_execution_commits_its_write(
    mongo_client: AsyncMongoClient[Any], created: AsyncCollection[Any]
) -> None:
    async def body() -> None:
        await _insert(created, 1)

    await _executor(mongo_client).execute(Run(body))

    assert await _committed_ids(created) == [1]


async def test_a_failure_after_writing_persists_nothing(
    mongo_client: AsyncMongoClient[Any], created: AsyncCollection[Any]
) -> None:
    async def body() -> None:
        await _insert(created, 1)
        raise BusinessError

    executor = _executor(mongo_client)
    use_case = Run(body)

    with pytest.raises(BusinessError):
        await executor.execute(use_case)

    assert await _committed_ids(created) == []


async def test_a_nested_execution_does_not_commit(
    mongo_client: AsyncMongoClient[Any], created: AsyncCollection[Any]
) -> None:
    executor = _executor(mongo_client)
    seen_after_nested: list[list[int]] = []

    async def nested() -> None:
        await _insert(created, 1)

    async def outer() -> None:
        await executor.execute(Run(nested))
        seen_after_nested.append(await _committed_ids(created))

    await executor.execute(Run(outer))

    assert seen_after_nested == [[]]


async def test_a_failure_after_a_nested_write_persists_nothing(
    mongo_client: AsyncMongoClient[Any], created: AsyncCollection[Any]
) -> None:
    executor = _executor(mongo_client)

    async def nested() -> None:
        await _insert(created, 1)

    async def outer() -> None:
        await executor.execute(Run(nested))
        raise BusinessError

    use_case = Run(outer)

    with pytest.raises(BusinessError):
        await executor.execute(use_case)

    assert await _committed_ids(created) == []


async def test_a_hundred_invocations_end_every_session_and_reset_the_context(
    mongo_client: AsyncMongoClient[Any], created: AsyncCollection[Any]
) -> None:
    executor = _executor(mongo_client)
    clean: list[bool] = []

    async def invoke(index: int) -> None:
        async def body() -> None:
            await _insert(created, index)
            if index % 3 == 1:
                raise BusinessError
            if index % 3 == 2:
                await asyncio.Event().wait()

        try:
            await executor.execute(Run(body))
        except BusinessError:
            pass
        finally:
            clean.append(current_scope() is None and not in_atomic_transaction())

    for index in range(100):
        task = asyncio.create_task(invoke(index))
        if index % 3 == 2:
            await asyncio.sleep(0.05)
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert clean == [True] * 100
    assert await _committed_ids(created) == list(range(0, 100, 3))


async def test_a_gather_of_reads_using_the_session_is_refused_and_persists_nothing(
    mongo_client: AsyncMongoClient[Any], created: AsyncCollection[Any]
) -> None:
    sessions = _sessions(created)

    async def read() -> Any:
        return await created.find_one({}, session=sessions(False))

    async def body() -> None:
        await _insert(created, 1)
        await asyncio.gather(read(), read())

    use_case = Run(body)
    executor = _executor(mongo_client)

    with pytest.raises(ScopeOwnershipError):
        await executor.execute(use_case)

    assert await _committed_ids(created) == []
