"""A cancellation after a commit must not strand a queued invalidation.

Before the transaction deferral existed, a bump was durable the instant the
wrapped write returned. Once bumps are queued and drained after the commit,
a cancellation landing between "commit returned" and "the drain finished"
would abort the drain and leave its remaining actions queued on a local
``channel`` variable nobody ever drains again — a committed write that
silently loses its cache invalidation. The channel's priority lane
(``enqueue_priority``, what ``CachedRepository`` queues its own bump
through) runs shielded inside :meth:`PostCommitChannel.drain` so this
cannot occur.

The plain lane (``enqueue``, what job dispatches use) is deliberately left
as cancellable as before: shielding it would make an inline job body — a
full nested use-case execution with no framework-imposed timeout —
uncancellable for as long as it ran. The second test below pins that this
lane's cancellability was not accidentally widened by the priority lane's
shield.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.job.context import add_pending_dispatch
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase

from ._lifecycle_doubles import Log, StubUnitOfWorkFactory


class _SlowPriorityAction(UseCase[Any, str]):
    """Commits, then queues a priority action that blocks on a gate."""

    def __init__(self, gate: asyncio.Event, log: Log) -> None:
        self._gate = gate
        self._log = log

    async def execute(self, value: str) -> str:
        channel = active_channel()
        assert channel is not None

        async def _slow() -> None:
            self._log("priority.started")
            await self._gate.wait()
            self._log("priority.finished")

        channel.enqueue_priority(_slow)
        return value


class _SlowDispatch(UseCase[Any, str]):
    """Commits, then queues a plain dispatch that blocks on a gate."""

    def __init__(self, gate: asyncio.Event, log: Log) -> None:
        self._gate = gate
        self._log = log

    async def execute(self, value: str) -> str:
        async def _slow() -> None:
            self._log("dispatch.started")
            await self._gate.wait()
            self._log("dispatch.finished")

        add_pending_dispatch(_slow)
        return value


async def test_a_cancellation_after_the_commit_does_not_abort_the_shielded_priority_lane() -> None:
    log = Log()
    factory = StubUnitOfWorkFactory(log)
    executor = RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory})
    )
    gate = asyncio.Event()

    task = asyncio.create_task(
        executor.execute(_SlowPriorityAction(gate, log), params={"value": "x"})
    )
    # Let the pipeline run to the commit and the drain start; the priority
    # action blocks on the gate right after logging that it started.
    while "priority.started" not in log.entries:
        await asyncio.sleep(0)
    assert "uow.commit" in log.entries

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The caller gave up, but the shielded priority lane kept running in
    # the background: releasing the gate lets it finish.
    gate.set()
    await asyncio.sleep(0.01)

    assert "priority.finished" in log.entries


async def test_a_cancellation_after_the_commit_still_aborts_a_plain_dispatch_in_flight() -> None:
    """The property the priority lane's shield must not widen: dispatches stay cancellable."""
    log = Log()
    factory = StubUnitOfWorkFactory(log)
    executor = RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory})
    )
    gate = asyncio.Event()

    task = asyncio.create_task(executor.execute(_SlowDispatch(gate, log), params={"value": "x"}))
    while "dispatch.started" not in log.entries:
        await asyncio.sleep(0)
    assert "uow.commit" in log.entries

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Unlike the priority lane, the plain lane really was interrupted: the
    # drain aborted and re-queued the dispatch instead of resuming it, so
    # releasing the gate now reaches nothing still waiting on it.
    gate.set()
    await asyncio.sleep(0.01)

    assert "dispatch.finished" not in log.entries


async def test_a_cancellation_before_any_commit_still_aborts_normally() -> None:
    """No lane, no shield, applies before a unit of work has committed anything."""
    log = Log()
    factory = StubUnitOfWorkFactory(log)
    executor = RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory})
    )

    class _Hang(UseCase[Any, str]):
        async def execute(self, value: str) -> str:
            await asyncio.Event().wait()
            return value

    task = asyncio.create_task(executor.execute(_Hang(), params={"value": "x"}))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert "uow.commit" not in log.entries
    assert "uow.rollback" in log.entries
