"""``async with uow:`` inside an invocation scope is refused (ADR 0008, fault 2).

Characterisation: before the scope existed, entering a transactional unit of
work by hand inside a use case opened a second transaction beside the
executor's, which committed on its own even when the use case then failed.
Inside a scope that is now ``UnitOfWorkInsideScope``, raised before any
session opens. Outside a scope ``async with uow:`` works as it always did
(the T044 matrix in ``tests/unit/core/repository/contract/test_uow.py``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.mongo.uow import MongoUnitOfWork, NoOpMongoUnitOfWork, active_session
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.transaction import in_atomic_transaction
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import UnitOfWorkInsideScope, invocation
from loom.core.use_case.use_case import UseCase

from ..conftest import RecordingSessionManager, make_session
from ..repository.mongo._fake import FakeMongoClient
from ._coordinator_fakes import Factory, Ledger, PhasedStore


def _sqlalchemy(manager: RecordingSessionManager) -> SQLAlchemyUnitOfWork:
    return SQLAlchemyUnitOfWork(manager)  # type: ignore[arg-type]


@dataclass(frozen=True)
class _Case:
    """A transactional unit of work and probes of what it did to its backend.

    Attributes:
        uow: The unit of work, not yet entered.
        name: Its class name, as the refusal names it.
        sessions: How many sessions it went through.
        commits: How many commits reached the backend.
    """

    uow: UnitOfWork
    name: str
    sessions: Callable[[], int]
    commits: Callable[[], int]


def _sqlalchemy_case() -> _Case:
    session = make_session()
    manager = RecordingSessionManager(session)
    return _Case(
        _sqlalchemy(manager),
        "SQLAlchemyUnitOfWork",
        lambda: manager.exits,
        lambda: session.commit.await_count,
    )


def _mongo_case() -> _Case:
    client = FakeMongoClient()
    uow = MongoUnitOfWork(cast(AsyncMongoClient[Any], client))
    return _Case(uow, "MongoUnitOfWork", lambda: len(client.sessions), lambda: client.committed)


_CASES = pytest.mark.parametrize(
    "make_case", [_sqlalchemy_case, _mongo_case], ids=["sqlalchemy", "mongo"]
)


def _scope_coordinator() -> OrderedCommits:
    return OrderedCommits({DEFAULT_STORE: Factory(PhasedStore(DEFAULT_STORE, Ledger()))})


async def _enter(uow: UnitOfWork) -> None:
    async with uow:
        pass


class TestInsideAScope:
    @_CASES
    async def test_a_transactional_unit_of_work_is_refused(
        self, make_case: Callable[[], _Case]
    ) -> None:
        case = make_case()

        async with invocation(_scope_coordinator()):
            with pytest.raises(UnitOfWorkInsideScope, match=case.name):
                await _enter(case.uow)

    @_CASES
    async def test_a_refused_unit_of_work_opens_no_session_and_commits_nothing(
        self, make_case: Callable[[], _Case]
    ) -> None:
        case = make_case()

        async with invocation(_scope_coordinator()):
            with pytest.raises(UnitOfWorkInsideScope):
                await _enter(case.uow)

        assert (case.sessions(), case.commits()) == (0, 0)

    async def test_a_no_op_mongo_unit_of_work_opens_nothing_and_is_allowed(self) -> None:
        """It has no transaction to commit on its own; the coordinator enters it too."""
        async with invocation(_scope_coordinator()), NoOpMongoUnitOfWork():
            opened = (active_session(), in_atomic_transaction())

        assert opened == (None, False)

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
    @_CASES
    async def test_a_transactional_unit_of_work_still_commits(
        self, make_case: Callable[[], _Case]
    ) -> None:
        case = make_case()

        await _enter(case.uow)

        assert case.commits() == 1


class TestAfterTheScopeClosed:
    """A task that outlived its use case still sees the closed scope, and may open its own."""

    @_CASES
    async def test_a_transactional_unit_of_work_commits(
        self, make_case: Callable[[], _Case]
    ) -> None:
        case = make_case()

        await _enter_after_the_scope_closed(case.uow)

        assert case.commits() == 1


async def _enter_after_the_scope_closed(uow: UnitOfWork) -> None:
    """Enter ``uow`` from a task spawned inside a scope, once that scope has closed."""
    go = asyncio.Event()
    survivors: list[asyncio.Task[None]] = []

    async def late() -> None:
        await go.wait()
        await _enter(uow)

    async with invocation(_scope_coordinator()):
        survivors.append(asyncio.create_task(late()))
    go.set()
    await survivors[0]


def _store() -> PhasedStore:
    return PhasedStore(DEFAULT_STORE, Ledger())


class _EntersByHand(UseCase[Any, None]):
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self) -> None:
        async with self._uow:
            pass
