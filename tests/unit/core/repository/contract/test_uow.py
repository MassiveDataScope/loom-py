from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy.exc import IntegrityError

from loom.core.repository.abc import Creatable, Readable
from loom.core.repository.mongo.uow import active_session
from loom.core.repository.sqlalchemy.transactional import get_active_session
from loom.core.transaction import in_atomic_transaction

from .conftest import SEED, BackendCase, mongo_client_of, require


class _Abort(Exception):
    pass


def _context_state() -> tuple[bool, object, object]:
    return (in_atomic_transaction(), get_active_session(), active_session())


async def test_commit_makes_writes_visible(case: BackendCase, repository: Any) -> None:
    require(case, Creatable, Readable)

    async with case.unit_of_work(repository):
        created = await repository.create(SEED[0])

    assert await repository.get_by_id(created.id) == created


async def test_rollback_discards_writes(case: BackendCase, repository: Any) -> None:
    require(case, Creatable, Readable)
    if not case.transactions:
        pytest.skip(f"{case.name} does not roll back writes")

    async def _create_then_abort() -> None:
        async with case.unit_of_work(repository):
            await repository.create(SEED[0])
            raise _Abort

    with pytest.raises(_Abort):
        await _create_then_abort()

    assert await repository.get_by_id(SEED[0].id) is None


async def test_unit_of_work_declares_transactional_as_the_case_does(
    case: BackendCase, repository: Any
) -> None:
    assert type(case.unit_of_work(repository)).transactional is case.transactions


async def test_atomic_transaction_signal_is_open_only_inside_a_transactional_unit_of_work(
    case: BackendCase, repository: Any
) -> None:
    assert in_atomic_transaction() is False

    async with case.unit_of_work(repository):
        assert in_atomic_transaction() is case.transactions

    assert in_atomic_transaction() is False


async def test_reads_inside_the_unit_of_work_see_its_uncommitted_writes(
    case: BackendCase, repository: Any
) -> None:
    require(case, Creatable, Readable)

    async with case.unit_of_work(repository):
        session_before = get_active_session()
        created = await repository.create(SEED[0])
        assert await repository.get_by_id(created.id) == created
        if case.name == "sqlalchemy":
            assert session_before is not None
            assert get_active_session() is session_before


async def _fail_the_commit(case: BackendCase, repository: Any) -> Any:
    async with case.unit_of_work(repository):
        created = await repository.create(SEED[0])
        assert case.fail_commit is not None
        await case.fail_commit(repository)
    return created


async def test_a_failed_sqlalchemy_commit_persists_nothing_and_closes_the_unit_of_work(
    case: BackendCase, repository: Any
) -> None:
    if case.name != "sqlalchemy":
        pytest.skip(f"{case.name}: the commit-failure seam is SQLAlchemy's duplicate key")
    committed = await repository.create(SEED[1])

    with pytest.raises(IntegrityError):
        await _fail_the_commit(case, repository)

    assert _context_state() == (False, None, None)
    assert await repository.get_by_id(SEED[0].id) is None
    assert await repository.get_by_id(SEED[1].id) == committed


async def test_a_failed_mongo_commit_leaves_its_write_visible(
    case: BackendCase, repository: Any
) -> None:
    if case.name != "mongo-transactional":
        pytest.skip(f"{case.name}: the commit-failure seam is the transactional Mongo fake")

    with pytest.raises(ConnectionError):
        await _fail_the_commit(case, repository)

    client = mongo_client_of(repository)
    assert _context_state() == (False, None, None)
    assert client.sessions
    assert all(session.ended for session in client.sessions)
    # characterisation: MongoUnitOfWork does not abort after a failed commit
    # (pymongo marks the transaction itself) and FakeSession.commit_error does
    # not restore the snapshot, so the flushed write stays in the collection.
    # US7 decides whether the unit of work aborts explicitly.
    assert client.aborted == 0
    assert await repository.get_by_id(SEED[0].id) is not None


async def test_cancellation_rolls_back_and_reraises_cancelled_error(
    case: BackendCase, repository: Any
) -> None:
    require(case, Creatable, Readable)
    started = asyncio.Event()
    recorded: list[tuple[bool, object, object]] = []
    created: list[Any] = []

    async def _run() -> None:
        try:
            async with case.unit_of_work(repository):
                created.append(await repository.create(SEED[0]))
                started.set()
                await asyncio.Event().wait()
        except asyncio.CancelledError:
            recorded.append(_context_state())
            raise

    task = asyncio.create_task(_run())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert recorded == [(False, None, None)]
    visible = await repository.get_by_id(SEED[0].id)
    if case.transactions:
        assert visible is None
        return
    # characterisation: autocommit, cancellation cannot discard it.
    assert visible == created[0]


async def test_error_exit_resets_the_session_context(case: BackendCase, repository: Any) -> None:
    require(case, Creatable)
    assert _context_state() == (False, None, None)
    recorded: list[tuple[bool, object, object]] = []

    try:
        async with case.unit_of_work(repository):
            await repository.create(SEED[0])
            raise _Abort
    except _Abort:
        recorded.append(_context_state())
    else:
        raise AssertionError("unit of work suppressed _Abort")

    assert recorded == [(False, None, None)]


async def test_write_before_an_exception_stays_visible_without_transactions(
    case: BackendCase, repository: Any
) -> None:
    require(case, Creatable, Readable)
    if case.transactions:
        pytest.skip(f"{case.name} rolls back writes")

    created = None

    async def _create_then_abort() -> None:
        nonlocal created
        async with case.unit_of_work(repository):
            created = await repository.create(SEED[0])
            raise _Abort

    with pytest.raises(_Abort):
        await _create_then_abort()

    # characterisation: no-op UoW autocommits each write; rollback is a no-op.
    assert await repository.get_by_id(SEED[0].id) == created


async def test_no_op_unit_of_work_exit_never_suppresses_or_claims_rollback(
    case: BackendCase, repository: Any
) -> None:
    require(case, Creatable, Readable)
    if case.transactions:
        pytest.skip(f"{case.name} rolls back writes")

    assert in_atomic_transaction() is False
    uow = case.unit_of_work(repository)
    assert await uow.__aenter__() is uow
    assert in_atomic_transaction() is False
    created = await repository.create(SEED[0])
    assert not await uow.__aexit__(_Abort, _Abort(), None)
    assert in_atomic_transaction() is False
    assert await uow.rollback() is None
    assert await repository.get_by_id(SEED[0].id) == created
    assert await uow.commit() is None
    assert in_atomic_transaction() is False
