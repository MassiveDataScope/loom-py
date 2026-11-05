"""Unit tests for SQLAlchemyUnitOfWork using a mocked SessionManager."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError

from loom.core._async import run_to_completion
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.sqlalchemy.transactional import _mutations
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork, SQLAlchemyUnitOfWorkFactory
from loom.core.transaction import in_atomic_transaction
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory
from loom.core.uow.coordinator import InvocationOutcome, PhysicalStore, RolledBack, Uncertain
from loom.core.uow.scope import current_scope, single_store

from ..conftest import RecordingSessionManager, make_session
from .conftest import SessionPair, make_session_manager


async def _close(uow: SQLAlchemyUnitOfWork) -> None:
    """Close a hand-driven unit of work the documented way: through ``__aexit__``."""
    await uow.__aexit__(RuntimeError, RuntimeError("test cleanup"), None)


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_sqlalchemy_uow_satisfies_protocol() -> None:
    sm = make_session_manager(make_session())
    uow = SQLAlchemyUnitOfWork(sm)
    assert isinstance(uow, UnitOfWork)


def test_sqlalchemy_uow_factory_satisfies_protocol() -> None:
    sm = make_session_manager(make_session())
    factory = SQLAlchemyUnitOfWorkFactory(sm)
    assert isinstance(factory, UnitOfWorkFactory)


# ---------------------------------------------------------------------------
# begin / commit / rollback
# ---------------------------------------------------------------------------


async def test_begin_opens_session() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    assert uow._session is session
    await _close(uow)


async def test_commit_calls_session_commit() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    await uow.commit()
    session.commit.assert_awaited_once()
    await _close(uow)


async def test_rollback_calls_session_rollback() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    await uow.rollback()
    session.rollback.assert_awaited_once()
    await _close(uow)


async def test_begin_before_commit_raises() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    with pytest.raises(RuntimeError, match="before begin"):
        await uow.commit()


async def test_begin_before_rollback_raises() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    with pytest.raises(RuntimeError, match="before begin"):
        await uow.rollback()


async def test_begin_twice_raises() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    await uow.begin()
    with pytest.raises(RuntimeError, match="called twice"):
        await uow.begin()
    await _close(uow)


async def test_a_failure_after_the_session_opened_closes_it_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A4: ``begin`` owns the open session from ``__aenter__`` onwards."""
    session = make_session()
    sm = RecordingSessionManager(session)
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
    monkeypatch.setattr(
        "loom.core.repository.sqlalchemy.uow.set_active_mutations",
        MagicMock(side_effect=RuntimeError("context lost")),
    )

    with pytest.raises(RuntimeError, match="context lost"):
        await uow.begin()

    assert sm.exits == 1
    assert uow.session is None


# ---------------------------------------------------------------------------
# Context manager — the unit of work is the participant of its store
# ---------------------------------------------------------------------------


def _participant_session(sm: Any) -> object:
    scope = current_scope()
    assert scope is not None
    return scope.participants[sm.store].handle


async def test_the_context_manager_makes_its_session_the_participant_of_its_store() -> None:
    session = make_session()
    sm = make_session_manager(session)

    async with SQLAlchemyUnitOfWork(sm):
        assert _participant_session(sm) is session


async def test_the_context_manager_unpublishes_its_scope_on_a_clean_exit() -> None:
    uow = SQLAlchemyUnitOfWork(make_session_manager(make_session()))

    async with uow:
        pass

    assert current_scope() is None


async def test_the_context_manager_unpublishes_its_scope_when_the_block_raises() -> None:
    uow = SQLAlchemyUnitOfWork(make_session_manager(make_session()))

    with pytest.raises(ValueError, match="test error"):
        async with uow:
            raise ValueError("test error")

    assert current_scope() is None


# ---------------------------------------------------------------------------
# Context manager — the atomic-transaction signal (F03)
# ---------------------------------------------------------------------------


async def test_in_atomic_transaction_true_inside_the_unit_of_work() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    assert in_atomic_transaction() is False
    async with uow:
        assert in_atomic_transaction() is True

    assert in_atomic_transaction() is False


async def test_in_atomic_transaction_reset_after_exception() -> None:
    session = make_session()
    sm = make_session_manager(session)
    uow = SQLAlchemyUnitOfWork(sm)

    with pytest.raises(ValueError):
        async with uow:
            raise ValueError("test error")

    assert in_atomic_transaction() is False


# ---------------------------------------------------------------------------
# Context manager — commit / rollback behaviour
# ---------------------------------------------------------------------------


async def test_context_manager_commits_on_success() -> None:
    session = make_session()
    sm = make_session_manager(session)

    async with SQLAlchemyUnitOfWork(sm):
        pass

    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


async def test_context_manager_rolls_back_on_exception() -> None:
    session = make_session()
    sm = make_session_manager(session)

    uow = SQLAlchemyUnitOfWork(sm)
    with pytest.raises(RuntimeError):
        async with uow:
            raise RuntimeError("oops")

    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# Context manager — commit failure and cancellation (US1 s2)
# ---------------------------------------------------------------------------


async def test_commit_failure_rolls_back_then_closes() -> None:
    session = make_session()
    session.commit = AsyncMock(side_effect=RuntimeError("commit failed"))
    sm = RecordingSessionManager(session)
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="commit failed"):
        async with uow:
            pass

    session.rollback.assert_awaited_once()
    assert sm.exits == 1
    assert current_scope() is None


async def test_close_failure_on_the_rollback_path_keeps_the_business_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = make_session()
    sm = RecordingSessionManager(session, exit_error=ConnectionError("pool gone"))
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    with (
        caplog.at_level(logging.ERROR, logger=SQLAlchemyUnitOfWork.__module__),
        pytest.raises(RuntimeError, match="business"),
    ):
        async with uow:
            raise RuntimeError("business")

    session.rollback.assert_awaited_once()
    assert sm.exits == 1
    assert "UoWCloseFailed" in caplog.text
    assert current_scope() is None


async def test_close_failure_after_commit_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The write committed: reporting an error would tell the caller it did not."""
    session = make_session()
    sm = RecordingSessionManager(session, exit_error=ConnectionError("pool gone"))
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR):
        async with uow:
            pass

    session.commit.assert_awaited_once()
    assert "UoWCloseFailed" in caplog.text
    assert current_scope() is None

    session.commit.assert_awaited_once()
    assert current_scope() is None


async def test_a_second_cancellation_waits_for_the_rollback_then_closes() -> None:
    """The coordinator runs the rollback to completion; the scope is unpublished in the task."""
    started = asyncio.Event()
    release = asyncio.Event()
    rolled_back = False

    async def _blocking_rollback() -> None:
        nonlocal rolled_back
        started.set()
        await release.wait()
        rolled_back = True

    session = make_session()
    session.rollback = _blocking_rollback
    sm = RecordingSessionManager(session)
    uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
    entered = asyncio.Event()
    observed: list[bool] = []

    async def run() -> None:
        try:
            async with uow:
                entered.set()
                await asyncio.Event().wait()
        finally:
            observed.append(current_scope() is None)  # the task's own context

    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel()  # cuts the body: the rollback starts and blocks on ``release``
    await started.wait()
    task.cancel()  # arrives while the rollback is in flight
    await asyncio.sleep(0)
    assert not task.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert observed == [True]
    assert rolled_back
    assert sm.exits == 1


class TestRollbackFailure:
    """The connection died before the rollback could run: the caller's error survives."""

    async def test_rollback_failure_still_closes_and_keeps_the_business_error(
        self, broken_rollback: SessionPair, caplog: pytest.LogCaptureFixture
    ) -> None:
        uow = SQLAlchemyUnitOfWork(broken_rollback.manager)  # type: ignore[arg-type]

        with (
            caplog.at_level(logging.ERROR, logger=SQLAlchemyUnitOfWork.__module__),
            pytest.raises(ValueError, match="business"),
        ):
            async with uow:
                raise ValueError("business")

        broken_rollback.session.rollback.assert_awaited_once()
        assert broken_rollback.manager.exits == 1
        assert "UoWRollbackFailed" in caplog.text
        assert current_scope() is None


# ---------------------------------------------------------------------------
# Phased surface (driven by the ordered-commits coordinator)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("step", "error", "outcome"),
    [
        pytest.param(
            "commit", OperationalError("COMMIT", None, OSError()), Uncertain, id="operational"
        ),
        pytest.param(
            "commit", InterfaceError("COMMIT", None, OSError()), Uncertain, id="interface"
        ),
        pytest.param(
            "commit",
            DBAPIError("COMMIT", None, OSError(), connection_invalidated=True),
            Uncertain,
            id="invalidated",
        ),
        pytest.param("commit", TimeoutError(), Uncertain, id="timeout"),
        pytest.param("commit", ConnectionResetError(), Uncertain, id="transport"),
        pytest.param(
            "commit",
            IntegrityError("COMMIT", None, OSError()),
            RolledBack,
            id="deferred-integrity",
        ),
        pytest.param(
            "flush", OperationalError("INSERT", None, OSError()), RolledBack, id="flush-operational"
        ),
    ],
)
async def test_a_failed_commit_is_classified_and_the_caller_gets_the_driver_error(
    step: str, error: Exception, outcome: type[Uncertain | RolledBack]
) -> None:
    session = make_session()
    setattr(session, step, AsyncMock(side_effect=error))
    uow = SQLAlchemyUnitOfWork(RecordingSessionManager(session))  # type: ignore[arg-type]

    with pytest.raises(type(error)) as raised:
        async with single_store(DEFAULT_STORE, uow) as scope:
            pass

    assert raised.value is error
    assert scope.outcome == InvocationOutcome((outcome(DEFAULT_STORE, error),))
    assert session.invalidate.await_count == (outcome is Uncertain)


async def test_a_failing_invalidate_leaves_the_commit_uncertain_with_its_own_error() -> None:
    error = OperationalError("COMMIT", None, OSError())
    session = make_session()
    session.commit = AsyncMock(side_effect=error)
    session.invalidate = AsyncMock(side_effect=OSError("socket gone"))
    uow = SQLAlchemyUnitOfWork(RecordingSessionManager(session))  # type: ignore[arg-type]

    with pytest.raises(OperationalError) as raised:
        async with single_store(DEFAULT_STORE, uow) as scope:
            pass

    assert raised.value is error
    assert scope.outcome == InvocationOutcome((Uncertain(DEFAULT_STORE, error),))


class TestPhased:
    def test_owner_is_the_session_manager(self) -> None:
        sm = make_session_manager(make_session())

        assert SQLAlchemyUnitOfWork(sm).owner is sm

    def test_physical_names_the_url_without_credentials(self) -> None:
        sm = make_session_manager(make_session())
        sm.engine.url = make_url("postgresql+asyncpg://loom:secret@db:5432/app")

        physical = SQLAlchemyUnitOfWork(sm).physical

        assert physical == PhysicalStore(
            backend="sqlalchemy", target="postgresql+asyncpg://db:5432/app"
        )

    async def test_flush_flushes_the_session(self) -> None:
        session = make_session()
        session.flush = AsyncMock()
        uow = SQLAlchemyUnitOfWork(make_session_manager(session))
        await uow.begin()

        await uow.flush()

        session.flush.assert_awaited_once()
        await uow.close()

    async def test_flush_before_begin_is_refused(self) -> None:
        uow = SQLAlchemyUnitOfWork(make_session_manager(make_session()))

        with pytest.raises(RuntimeError, match=r"flush\(\) called before begin"):
            await uow.flush()

    async def test_close_exits_the_session(self) -> None:
        sm = RecordingSessionManager(make_session())
        uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
        await uow.begin()

        await uow.close()

        assert sm.exits == 1

    async def test_close_releases_the_session(self) -> None:
        uow = SQLAlchemyUnitOfWork(make_session_manager(make_session()))
        await uow.begin()

        await uow.close()

        assert uow.session is None

    async def test_close_closes_the_atomic_transaction_signal(self) -> None:
        uow = SQLAlchemyUnitOfWork(make_session_manager(make_session()))
        await uow.begin()

        await uow.close()

        assert in_atomic_transaction() is False

    async def test_close_resets_the_mutation_list(self) -> None:
        uow = SQLAlchemyUnitOfWork(make_session_manager(make_session()))
        await uow.begin()

        await uow.close()

        assert _mutations.get() is None

    @pytest.mark.parametrize(("still_open", "invalidations"), [(True, 1), (False, 0)])
    async def test_close_invalidates_only_a_transaction_its_rollback_left_open(
        self, still_open: bool, invalidations: int
    ) -> None:
        session = make_session()
        session.in_transaction.return_value = still_open
        uow = SQLAlchemyUnitOfWork(make_session_manager(session))
        await uow.begin()
        with pytest.raises(TimeoutError):
            await run_to_completion(uow.rollback(), timeout=0)

        await uow.close()

        assert (session.rollback.await_count, session.invalidate.await_count) == (0, invalidations)

    async def test_a_failure_to_close_still_resets_the_tokens_and_propagates(self) -> None:
        sm = RecordingSessionManager(make_session(), exit_error=ConnectionError("pool gone"))
        uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
        await uow.begin()

        with pytest.raises(ConnectionError, match="pool gone"):
            await uow.close()

        assert (uow.session, in_atomic_transaction()) == (None, False)

    async def test_a_cancelled_close_still_exits_the_session_and_resets_the_tokens(self) -> None:
        gate = asyncio.Event()
        sm = _GatedSessionManager(make_session(), gate)
        uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
        observed: list[bool] = []

        async def close() -> None:
            await uow.begin()
            try:
                await uow.close()
            finally:
                observed.append(uow.session is None)

        task = asyncio.create_task(close())
        await sm.exiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        gate.set()
        await asyncio.sleep(0)

        assert observed == [True]
        assert sm.exits == 1

    async def test_a_close_cancelled_while_invalidating_still_exits_and_resets_the_tokens(
        self,
    ) -> None:
        gate, invalidating = asyncio.Event(), asyncio.Event()

        async def _invalidate() -> None:
            invalidating.set()
            await gate.wait()

        session = make_session()
        session.in_transaction.return_value = True
        session.invalidate = AsyncMock(side_effect=_invalidate)
        sm = RecordingSessionManager(session)
        uow = SQLAlchemyUnitOfWork(sm)  # type: ignore[arg-type]
        observed: list[tuple[bool, bool, bool]] = []

        async def close() -> None:
            await uow.begin()
            try:
                await uow.close()
            finally:
                observed.append(
                    (uow.session is None, in_atomic_transaction(), _mutations.get() is None)
                )

        task = asyncio.create_task(close())
        await invalidating.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        gate.set()
        await asyncio.sleep(0)

        assert observed == [(True, False, True)]
        assert sm.exits == 1


class _GatedSessionManager(RecordingSessionManager):
    """Session manager whose exit blocks on ``gate``; ``exiting`` is set when it starts."""

    def __init__(self, session: MagicMock, gate: asyncio.Event) -> None:
        super().__init__(session)
        self._gate = gate
        self.exiting = asyncio.Event()

    async def __aexit__(self, *exc: object) -> None:
        self.exiting.set()
        await self._gate.wait()
        await super().__aexit__(*exc)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def test_factory_create_returns_new_uow() -> None:
    sm = make_session_manager(make_session())
    factory = SQLAlchemyUnitOfWorkFactory(sm)
    uow1 = factory.create()
    uow2 = factory.create()
    assert uow1 is not uow2


def test_factory_create_returns_uow_with_same_session_manager() -> None:
    sm = make_session_manager(make_session())
    factory = SQLAlchemyUnitOfWorkFactory(sm)
    uow = factory.create()
    assert isinstance(uow, SQLAlchemyUnitOfWork)
    assert uow._session_manager is sm
