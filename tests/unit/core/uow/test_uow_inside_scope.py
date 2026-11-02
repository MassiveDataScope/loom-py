"""``async with uow:`` inside an invocation scope is refused (ADR 0008, fault 2).

Characterisation: before the scope existed, entering a transactional unit of
work by hand inside a use case opened a second transaction beside the
executor's, which committed on its own even when the use case then failed.
Inside a scope that is now ``UnitOfWorkInsideScope``, raised before any
session opens. Outside a scope ``async with uow:`` works as it always did
(the T044 matrix in ``tests/unit/core/repository/contract/test_uow.py``).
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.mongo.uow import MongoUnitOfWork, NoOpMongoUnitOfWork
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import UnitOfWorkInsideScope, invocation
from loom.core.use_case.use_case import UseCase

from ..conftest import RecordingSessionManager, make_session
from ..repository.mongo._fake import FakeMongoClient
from ._coordinator_fakes import Factory, Ledger, PhasedStore


def _sqlalchemy(manager: RecordingSessionManager) -> SQLAlchemyUnitOfWork:
    return SQLAlchemyUnitOfWork(manager)  # type: ignore[arg-type]


def _mongo(client: FakeMongoClient) -> MongoUnitOfWork:
    return MongoUnitOfWork(cast(AsyncMongoClient[Any], client))


def _scope_coordinator() -> OrderedCommits:
    return OrderedCommits({DEFAULT_STORE: Factory(PhasedStore(DEFAULT_STORE, Ledger()))})


async def _enter(uow: UnitOfWork) -> None:
    async with uow:
        pass


class TestInsideAScope:
    async def test_a_sqlalchemy_unit_of_work_is_refused(self) -> None:
        uow = _sqlalchemy(RecordingSessionManager(make_session()))

        async with invocation(_scope_coordinator()):
            with pytest.raises(UnitOfWorkInsideScope, match="SQLAlchemyUnitOfWork"):
                await _enter(uow)

    async def test_a_refused_sqlalchemy_unit_of_work_opens_no_session(self) -> None:
        session = make_session()
        manager = RecordingSessionManager(session)

        async with invocation(_scope_coordinator()):
            with pytest.raises(UnitOfWorkInsideScope):
                await _enter(_sqlalchemy(manager))

        assert (manager.exits, session.commit.await_count) == (0, 0)

    async def test_a_mongo_unit_of_work_is_refused(self) -> None:
        async with invocation(_scope_coordinator()):
            with pytest.raises(UnitOfWorkInsideScope, match="MongoUnitOfWork"):
                await _enter(_mongo(FakeMongoClient()))

    async def test_a_refused_mongo_unit_of_work_starts_no_session(self) -> None:
        client = FakeMongoClient()

        async with invocation(_scope_coordinator()):
            with pytest.raises(UnitOfWorkInsideScope):
                await _enter(_mongo(client))

        assert (client.sessions, client.committed) == ([], 0)

    async def test_a_no_op_mongo_unit_of_work_opens_nothing_and_is_allowed(self) -> None:
        """It has no transaction to commit on its own; the coordinator enters it too."""
        async with invocation(_scope_coordinator()):
            await _enter(NoOpMongoUnitOfWork())

    async def test_a_use_case_entering_one_by_hand_fails_with_it(self) -> None:
        manager = RecordingSessionManager(make_session())
        executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=Factory(_store()))

        with pytest.raises(UnitOfWorkInsideScope):
            await executor.execute(_EntersByHand(_sqlalchemy(manager)))

    async def test_a_use_case_entering_one_by_hand_commits_nothing_on_its_own(self) -> None:
        session = make_session()
        executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=Factory(_store()))

        with pytest.raises(UnitOfWorkInsideScope):
            await executor.execute(_EntersByHand(_sqlalchemy(RecordingSessionManager(session))))

        session.commit.assert_not_awaited()


class TestOutsideAScope:
    async def test_a_sqlalchemy_unit_of_work_still_commits(self) -> None:
        session = make_session()

        await _enter(_sqlalchemy(RecordingSessionManager(session)))

        session.commit.assert_awaited_once()

    async def test_a_mongo_unit_of_work_still_commits(self) -> None:
        client = FakeMongoClient()

        await _enter(_mongo(client))

        assert client.committed == 1


def _store() -> PhasedStore:
    return PhasedStore(DEFAULT_STORE, Ledger())


class _EntersByHand(UseCase[Any, None]):
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self) -> None:
        async with self._uow:
            pass
