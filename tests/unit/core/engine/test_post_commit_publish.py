"""Cancellation and failure contracts for post-commit broker publishes."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.celery.service import CeleryJobService
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitChannel, PostCommitError, active_channel
from loom.core.job.context import add_pending_dispatch, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.use_case.use_case import UseCase

from ._lifecycle_doubles import Log, StubUnitOfWorkFactory


class _EmailJob(Job[None]):
    def execute(self) -> None:
        return None


class _DispatchAfterCommit(UseCase[Any, None]):
    def __init__(
        self, service: CeleryJobService, started: asyncio.Event, release: asyncio.Event
    ) -> None:
        self.service = service
        self.started = started
        self.release = release

    async def execute(self) -> None:
        channel = active_channel()
        assert channel is not None

        async def bump() -> None:
            self.started.set()
            await self.release.wait()

        channel.enqueue_priority(bump)
        self.service.dispatch(_EmailJob)


class _InlineAfterCommit(UseCase[Any, None]):
    def __init__(self, started: asyncio.Event, cancelled: asyncio.Event, calls: list[str]) -> None:
        self.started = started
        self.cancelled = cancelled
        self.calls = calls

    async def execute(self) -> None:
        async def inline() -> None:
            self.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()

        add_pending_dispatch(inline)
        add_pending_dispatch(lambda: self.calls.append("later"))


@pytest.mark.timeout(3)
async def test_cancel_after_commit_still_sends_celery_task_once() -> None:
    app = MagicMock()
    sent = asyncio.Event()
    app.send_task.side_effect = lambda *_, **__: sent.set()
    started, release = asyncio.Event(), asyncio.Event()
    log = Log()
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(log))
    task = asyncio.create_task(
        executor.execute(_DispatchAfterCommit(CeleryJobService(app), started, release))
    )
    await started.wait()
    assert "uow.commit" in log.entries
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    app.send_task.assert_not_called()
    release.set()
    await sent.wait()
    app.send_task.assert_called_once()


@pytest.mark.timeout(3)
@pytest.mark.parametrize("blocked_lane", ["priority", "publish"])
async def test_cancellation_does_not_skip_publishes(blocked_lane: str) -> None:
    channel = PostCommitChannel()
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    async def blocked() -> None:
        started.set()
        await release.wait()
        calls.append(blocked_lane)

    if blocked_lane == "priority":
        channel.enqueue_priority(blocked)
    else:
        channel.enqueue_publish(blocked)
    channel.enqueue_publish(lambda: (calls.append("publish"), finished.set()))
    task = asyncio.create_task(channel.drain(committed=True))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await finished.wait()
    assert calls == [blocked_lane, "publish"]


@pytest.mark.timeout(3)
async def test_plain_action_is_cancelled_and_fallback_requeues_remainder() -> None:
    started, cancelled = asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    async def inline() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    add_pending_dispatch(inline)
    add_pending_dispatch(lambda: calls.append("later"))
    task = asyncio.create_task(flush_pending_dispatches())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await cancelled.wait()
    await flush_pending_dispatches()
    assert calls == ["later"]


@pytest.mark.timeout(3)
async def test_executor_drops_plain_remainder_and_warns(caplog: pytest.LogCaptureFixture) -> None:
    started, cancelled = asyncio.Event(), asyncio.Event()
    calls: list[str] = []
    log = Log()
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(log))
    task = asyncio.create_task(executor.execute(_InlineAfterCommit(started, cancelled, calls)))
    await started.wait()
    assert "uow.commit" in log.entries
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await cancelled.wait()
    assert calls == []
    assert "PostCommitPlainActionsDropped" in caplog.text
    assert "1" in caplog.text


@pytest.mark.timeout(3)
@pytest.mark.parametrize(("requeue", "ran_later"), [(True, ["plain"]), (False, [])])
async def test_a_cancellation_during_the_shield_settles_the_plain_lane(
    caplog: pytest.LogCaptureFixture, requeue: bool, ran_later: list[str]
) -> None:
    channel = PostCommitChannel(requeue_on_cancel=requeue)
    started, release = asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    async def publish() -> None:
        started.set()
        await release.wait()

    channel.enqueue_publish(publish)
    channel.enqueue(lambda: calls.append("plain"))
    task = asyncio.create_task(channel.drain(committed=True))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await channel.drain(committed=True)

    assert calls == ran_later
    assert ("PostCommitPlainActionsDropped" in caplog.text) is not requeue


@pytest.mark.timeout(3)
async def test_publish_failure_after_cancellation_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    channel = PostCommitChannel()
    started, release, logged = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class LogSignal(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "PostCommitShieldedActionFailedAfterCancellation" in record.getMessage():
                logged.set()

    handler = LogSignal()
    logger = logging.getLogger("loom.core.engine.post_commit")
    logger.addHandler(handler)

    async def blocked() -> None:
        started.set()
        await release.wait()

    class FailingPublish:
        task_name = "loom.job.EmailJob"
        task_id = "task-123"

        def __call__(self) -> None:
            raise ConnectionError("broker down")

    channel.enqueue_priority(blocked)
    channel.enqueue_publish(FailingPublish())
    channel.enqueue_publish(lambda: None)
    task = asyncio.create_task(channel.drain(committed=True))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    try:
        release.set()
        await logged.wait()
        assert "broker down" in caplog.text
        assert "loom.job.EmailJob" in caplog.text
        assert "task-123" in caplog.text
    finally:
        logger.removeHandler(handler)


async def test_failures_keep_priority_publish_plain_order() -> None:
    channel = PostCommitChannel()
    errors = [ValueError("priority"), ConnectionError("publish"), RuntimeError("plain")]

    def fail(error: Exception) -> None:
        raise error

    channel.enqueue(lambda: fail(errors[2]))
    channel.enqueue_publish(lambda: fail(errors[1]))
    channel.enqueue_priority(lambda: fail(errors[0]))
    with pytest.raises(PostCommitError) as info:
        await channel.drain(committed=True)
    assert info.value.committed is True
    assert info.value.failures == tuple(errors)


async def test_publish_runs_before_inline_even_when_queued_later() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("inline"))
    channel.enqueue_publish(lambda: calls.append("publish"))

    await channel.drain(committed=True)

    assert calls == ["publish", "inline"]
