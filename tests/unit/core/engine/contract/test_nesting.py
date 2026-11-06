"""US1 s4: channel ownership follows unit-of-work ownership in nested executions."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.errors import UndeclaredStoreWrite
from loom.core.job.context import add_pending_dispatch
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.transactional import transactional
from loom.core.uow.scope import current_scope
from loom.core.use_case.use_case import UseCase

from .._lifecycle_doubles import Broker, Dispatching, Log, Ok, context_is_clean
from .conftest import CountingSessionManager, LifecycleCase, sqlite_session_manager


async def test_inner_joins_the_outer_unit_of_work_and_channel_one_drain(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker, log: Log
) -> None:
    class _Outer(UseCase[Any, str]):
        async def execute(self, value: str) -> str:
            inner = await executor.execute(Dispatching(broker), params={"value": "inner"})
            broker.dispatch("outer")
            return inner

    await executor.execute(_Outer(), params={"value": "x"})

    assert len(case.factory.created) == 1
    assert broker.sent == ["inner", "outer"]
    assert log.entries == [
        "event.EXEC_START",
        "uow.enter",
        "event.EXEC_START",
        "dispatch.queued(inner)",
        "event.EXEC_DONE",
        "dispatch.queued(outer)",
        "uow.exit",
        "event.EXEC_DONE",
        "broker.send(inner) uow=False",
        "broker.send(outer) uow=False",
    ]
    assert context_is_clean()


async def test_a_writing_inner_under_a_read_only_outer_is_refused(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker
) -> None:
    class _ReadOnlyOuter(UseCase[Any, str]):
        read_only = True

        async def execute(self, value: str) -> str:
            return await executor.execute(Dispatching(broker), params={"value": "inner"})

    action = _ReadOnlyOuter()
    with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
        await executor.execute(action, params={"value": "x"})

    assert (case.factory.created, broker.sent) == ([], [])
    assert context_is_clean()


async def test_an_execution_started_from_a_post_commit_action_opens_its_own_lifecycle(
    case: LifecycleCase, executor: RuntimeExecutor, log: Log
) -> None:
    class _FromAction(UseCase[Any, str]):
        async def execute(self, value: str) -> str:
            async def run_again() -> None:
                log(f"action.channel_bound={active_channel() is not None}")
                await executor.execute(Ok(), params={"value": "again"})

            add_pending_dispatch(run_again)
            return value

    await executor.execute(_FromAction(), params={"value": "x"})

    assert len(case.factory.created) == 2
    assert [probe.ended for probe in case.factory.created] == [["commit"], ["commit"]]
    assert log.entries == [
        "event.EXEC_START",
        "uow.enter",
        "uow.exit",
        "event.EXEC_DONE",
        "action.channel_bound=False",
        "event.EXEC_START",
        "uow.enter",
        "uow.exit",
        "event.EXEC_DONE",
    ]
    assert context_is_clean()


# ---------------------------------------------------------------------------
# A4 — an owned ``@transactional`` decorator cancelled in its body
# ---------------------------------------------------------------------------


class _Service:
    """Owner of a ``@transactional`` method with a post-commit hook."""

    def __init__(self, session_manager: CountingSessionManager, log: Log) -> None:
        self.session_manager = session_manager
        self._log = log

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        self._log("hook")

    @transactional
    async def hang(self) -> None:
        self._log(f"body.channel_bound={active_channel() is not None}")
        await asyncio.Event().wait()


async def test_cancellation_inside_an_owned_transactional_body_discards_its_channel() -> None:
    log = Log()
    async with sqlite_session_manager() as manager:
        service = _Service(manager, log)
        observed: list[bool] = []

        async def run() -> None:
            try:
                await service.hang()
            finally:
                observed.append(active_channel() is None and current_scope() is None)

        task = asyncio.create_task(run())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert observed == [True]
        assert log.entries == ["body.channel_bound=True"]
        assert manager.opened == manager.closed == 1
