"""US1 s1–s2 and SC-002: the executor drives every adapter through one lifecycle."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy import text

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase

from .._lifecycle_doubles import Boom, Log, Metrics, Ok, cancel_mid_flight, context_is_clean
from .conftest import (
    CountingSessionManager,
    LifecycleCase,
    cancel_twice_during_rollback,
    require_transactional,
    sqlite_session_manager,
)

_EXECUTIONS = 1000


class TestOneProtocolPerAdapter:
    """A phased adapter is driven phase by phase, any other through its context manager."""

    async def test_success_enters_and_exits_once_through_the_protocol(
        self, case: LifecycleCase, executor: RuntimeExecutor, metrics: Metrics
    ) -> None:
        result = await executor.execute(Ok(), params={"value": "x"})

        assert result == "x"
        assert case.factory.entered == case.factory.exited == 1
        assert case.factory.off_protocol_calls == []
        assert case.factory.created[0].ended == ["commit"]
        assert case.probe.sessions_opened() == case.probe.sessions_closed()
        assert case.probe.sessions_opened() == case.sessions_expected
        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_DONE]
        done = metrics.only(EventKind.EXEC_DONE)
        assert done.pipeline_ms is not None
        assert done.commit_ms is not None
        assert done.duration_ms is not None
        assert done.duration_ms >= done.pipeline_ms
        assert context_is_clean()

    async def test_failure_exits_once_with_the_error_and_unbinds_everything(
        self, case: LifecycleCase, executor: RuntimeExecutor, metrics: Metrics
    ) -> None:
        action = Boom()
        with pytest.raises(RuntimeError, match="boom"):
            await executor.execute(action, params={"value": "x"})

        assert case.factory.entered == case.factory.exited == 1
        assert case.factory.off_protocol_calls == []
        assert case.factory.created[0].ended == ["rollback"]
        assert case.probe.sessions_opened() == case.probe.sessions_closed()
        assert case.probe.sessions_opened() == case.sessions_expected
        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "business"
        assert context_is_clean()

    async def test_cancellation_exits_once_and_the_task_sees_a_clean_context(
        self, case: LifecycleCase, executor: RuntimeExecutor, metrics: Metrics
    ) -> None:
        await cancel_mid_flight(executor)

        assert case.factory.entered == case.factory.exited == 1
        assert case.factory.off_protocol_calls == []
        assert case.factory.created[0].ended == ["rollback"]
        assert case.probe.sessions_opened() == case.probe.sessions_closed()
        assert case.probe.sessions_opened() == case.sessions_expected
        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "cancelled"


class TestTerminalEventReflectsTheTransaction:
    async def test_commit_failure_is_exec_error_commit_and_the_adapter_closes_once(
        self, case: LifecycleCase, executor: RuntimeExecutor, metrics: Metrics, log: Log
    ) -> None:
        require_transactional(case).fail_commit(RuntimeError("commit refused"))
        action = Ok()

        with pytest.raises(RuntimeError, match="commit refused"):
            await executor.execute(action, params={"value": "x"})

        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        error = metrics.only(EventKind.EXEC_ERROR)
        assert error.error_kind == "commit"
        assert error.commit_ms is not None
        assert case.factory.exited == 1
        assert case.probe.sessions_opened() == case.probe.sessions_closed() == 1
        assert case.probe.rollbacks() == case.rollbacks_after_failed_commit
        assert log.index("uow.exit") < log.index("event.EXEC_ERROR")
        assert context_is_clean()

    async def test_begin_failure_emits_exec_start_then_exec_error_begin(
        self, case: LifecycleCase, executor: RuntimeExecutor, metrics: Metrics
    ) -> None:
        require_transactional(case).fail_begin(ConnectionError("no database"))
        action = Ok()

        with pytest.raises(ConnectionError, match="no database"):
            await executor.execute(action, params={"value": "x"})

        assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "begin"
        assert case.factory.entered == 1
        # The coordinator closes a phased unit of work whose begin failed.
        assert case.factory.exited == (1 if case.phased else 0)
        # A failed begin opens nothing (SQLAlchemy) or ends what it opened (Mongo).
        assert case.probe.sessions_opened() == case.probe.sessions_closed()
        assert context_is_clean()

    async def test_second_cancellation_during_the_rollback_waits_for_it_then_closes(
        self, case: LifecycleCase, executor: RuntimeExecutor, metrics: Metrics
    ) -> None:
        gate = asyncio.Event()
        require_transactional(case).gate_rollback(gate)

        observed = await cancel_twice_during_rollback(executor, gate)

        assert observed == [True]
        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "cancelled"
        assert case.probe.rollbacks() == 1
        assert case.probe.sessions_opened() == case.probe.sessions_closed() == 1
        assert case.factory.exited == 1


class TestNoSessionLeak:
    async def test_a_thousand_executions_close_every_unit_of_work_they_open(
        self, case: LifecycleCase, executor: RuntimeExecutor
    ) -> None:
        for index in range(_EXECUTIONS):
            await executor.execute(Ok(), params={"value": str(index)})

        assert case.factory.entered == case.factory.exited == _EXECUTIONS
        assert case.probe.sessions_opened() == case.probe.sessions_closed()
        assert context_is_clean()


class _SelectOne(UseCase[Any, int]):
    """Runs one statement on the store's active session so a real connection is used."""

    def __init__(self, manager: CountingSessionManager) -> None:
        self._manager = manager

    async def execute(self) -> int:
        session = self._manager.active_session
        assert session is not None
        return (await session.execute(text("SELECT 1"))).scalar_one()


async def test_sc002_sqlite_sessions_opened_equal_sessions_closed_after_a_thousand() -> None:
    async with sqlite_session_manager() as manager:
        factory = SQLAlchemyUnitOfWorkFactory(manager.as_session_manager())
        executor = RuntimeExecutor(
            UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory})
        )

        for _ in range(_EXECUTIONS):
            assert await executor.execute(_SelectOne(manager)) == 1

        assert manager.opened == manager.closed == _EXECUTIONS
        assert context_is_clean()
