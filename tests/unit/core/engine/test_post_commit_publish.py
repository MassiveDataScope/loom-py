"""Cancellation and failure contracts for post-commit broker publishes."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.celery.service import CeleryJobService
from loom.core.engine import post_commit
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitChannel, PostCommitError, active_channel
from loom.core.job.context import add_pending_dispatch, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.use_case.use_case import UseCase

from ._lifecycle_doubles import Log, StubUnitOfWorkFactory

pytestmark = pytest.mark.timeout(3)

_DROPPED = "PostCommitPlainActionsDropped"
_FAILED = "PostCommitActionFailedAfterInterruption"
_NEVER_RAN = "PostCommitShieldedActionNeverRan"
_COMMITTED = "uow.commit"


class _Gate:
    """An async action that parks until released."""

    def __init__(self, calls: list[str] | None = None, name: str = "gate") -> None:
        self.started, self.release = asyncio.Event(), asyncio.Event()
        self._calls, self._name = calls, name

    async def __call__(self) -> None:
        self.started.set()
        await self.release.wait()
        if self._calls is not None:
            self._calls.append(self._name)


class _Hang:
    """An async action that never finishes; records when it is cancelled."""

    def __init__(self) -> None:
        self.started, self.cancelled = asyncio.Event(), asyncio.Event()

    async def __call__(self) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()


class _Send:
    """A broker send carrying the job name and task id a log may name."""

    task_name = "loom.job.EmailJob"
    task_id = "task-123"

    def __init__(self, error: Exception | None = None) -> None:
        self._error = error

    def __call__(self) -> None:
        if self._error is not None:
            raise self._error

    def log_fields(self) -> tuple[dict[str, object]]:
        return ({"job_name": self.task_name, "task_id": self.task_id},)


@pytest.fixture
def logged() -> Iterator[Callable[[str], asyncio.Event]]:
    """Events set once the post-commit logger emits a message containing a marker."""
    waiting: dict[str, asyncio.Event] = {}

    class Signal(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            for marker, event in waiting.items():
                if marker in record.getMessage():
                    event.set()

    handler = Signal()
    logger = logging.getLogger(post_commit.__name__)
    logger.addHandler(handler)
    yield lambda marker: waiting.setdefault(marker, asyncio.Event())
    logger.removeHandler(handler)


def _event(caplog: pytest.LogCaptureFixture, name: str) -> dict[str, Any]:
    """The structured fields of the one post-commit log record named *name*."""
    (fields,) = [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == name
    ]
    return fields


async def _cancelled(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class _DispatchAfterCommit(UseCase[Any, None]):
    def __init__(self, service: CeleryJobService, bump: _Gate) -> None:
        self.service = service
        self.bump = bump

    async def execute(self) -> None:
        channel = active_channel()
        assert channel is not None
        channel.enqueue_priority(self.bump)
        self.service.dispatch(_EmailJob)


class _InlineAfterCommit(UseCase[Any, None]):
    def __init__(self, hang: _Hang, calls: list[str]) -> None:
        self.hang = hang
        self.calls = calls

    async def execute(self) -> None:
        add_pending_dispatch(self.hang)
        add_pending_dispatch(lambda: self.calls.append("later"))


class _EmailJob(Job[None]):
    def execute(self) -> None:
        return None


async def test_a_send_committed_before_the_cancellation_reaches_the_broker_once() -> None:
    app = MagicMock()
    sent = asyncio.Event()
    app.send_task.side_effect = lambda *_, **__: sent.set()
    bump = _Gate()
    log = Log()
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(log))
    task = asyncio.create_task(executor.execute(_DispatchAfterCommit(CeleryJobService(app), bump)))
    await bump.started.wait()
    assert _COMMITTED in log.entries

    await _cancelled(task)
    app.send_task.assert_not_called()
    bump.release.set()
    await sent.wait()

    app.send_task.assert_called_once()
    assert app.send_task.call_args.args[0].endswith("_EmailJob")


@pytest.mark.parametrize("lane", ["enqueue_priority", "enqueue_publish"])
async def test_a_cancellation_skips_no_publish(lane: str) -> None:
    channel = PostCommitChannel()
    calls: list[str] = []
    gate = _Gate(calls, "blocked")
    finished = asyncio.Event()
    getattr(channel, lane)(gate)

    def publish() -> None:
        calls.append("publish")
        finished.set()

    channel.enqueue_publish(publish)
    task = asyncio.create_task(channel.drain(committed=True))
    await gate.started.wait()

    await _cancelled(task)
    gate.release.set()
    await finished.wait()

    assert calls == ["blocked", "publish"]


async def test_the_fallback_channel_requeues_what_a_cancelled_flush_left() -> None:
    hang = _Hang()
    calls: list[str] = []
    add_pending_dispatch(hang)
    add_pending_dispatch(lambda: calls.append("later"))
    task = asyncio.create_task(flush_pending_dispatches())
    await hang.started.wait()

    await _cancelled(task)
    await hang.cancelled.wait()
    await flush_pending_dispatches()

    assert calls == ["later"]


async def test_the_executor_drops_what_a_cancellation_left_and_says_how_much(
    caplog: pytest.LogCaptureFixture,
) -> None:
    hang = _Hang()
    calls: list[str] = []
    log = Log()
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(log))
    task = asyncio.create_task(executor.execute(_InlineAfterCommit(hang, calls)))
    await hang.started.wait()
    assert _COMMITTED in log.entries

    await _cancelled(task)
    await hang.cancelled.wait()

    assert calls == []
    assert {"event": _DROPPED, "count": 1}.items() <= _event(caplog, _DROPPED).items()


@pytest.mark.parametrize(
    ("requeue", "ran_later", "warned"),
    [
        pytest.param(True, ["plain"], False, id="requeued"),
        pytest.param(False, [], True, id="dropped"),
    ],
)
async def test_a_cancellation_during_the_shield_settles_the_plain_lane(
    caplog: pytest.LogCaptureFixture, requeue: bool, ran_later: list[str], warned: bool
) -> None:
    channel = PostCommitChannel(requeue_on_cancel=requeue)
    gate = _Gate()
    calls: list[str] = []
    channel.enqueue_publish(gate)
    channel.enqueue(lambda: calls.append("plain"))
    task = asyncio.create_task(channel.drain(committed=True))
    await gate.started.wait()

    await _cancelled(task)
    gate.release.set()
    await channel.drain(committed=True)

    assert calls == ran_later
    assert (_DROPPED in caplog.text) is warned


async def test_a_publish_that_fails_after_the_cancellation_is_logged_by_job_and_task(
    caplog: pytest.LogCaptureFixture, logged: Callable[[str], asyncio.Event]
) -> None:
    channel = PostCommitChannel()
    gate = _Gate()
    failure_logged = logged(_FAILED)
    channel.enqueue_priority(gate)
    channel.enqueue_publish(_Send(ConnectionError("broker down")))
    task = asyncio.create_task(channel.drain(committed=True))
    await gate.started.wait()

    await _cancelled(task)
    gate.release.set()
    await failure_logged.wait()

    fields = _event(caplog, _FAILED)
    assert (fields["job_name"], fields["task_id"]) == (_Send.task_name, _Send.task_id)
    assert "broker down" in fields["error"]


async def test_sends_a_dead_shield_never_ran_are_logged(
    caplog: pytest.LogCaptureFixture, logged: Callable[[str], asyncio.Event]
) -> None:
    channel = PostCommitChannel()
    hang = _Hang()
    never_ran = logged(_NEVER_RAN)
    channel.enqueue_priority(hang)
    channel.enqueue_publish(_Send())
    task = asyncio.create_task(channel.drain(committed=True))
    await hang.started.wait()
    await _cancelled(task)

    for shielded in list(post_commit._shielded_tasks):  # what closing the loop does
        shielded.cancel()
    await never_ran.wait()

    assert _event(caplog, _NEVER_RAN)["task_id"] == _Send.task_id


async def test_without_a_cancellation_one_error_carries_every_failure_by_lane() -> None:
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


async def test_a_publish_runs_before_an_inline_job_queued_earlier() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("inline"))
    channel.enqueue_publish(lambda: calls.append("publish"))

    await channel.drain(committed=True)

    assert calls == ["publish", "inline"]


class _Batch:
    """A grouped publish: counts its runs and the items it carried."""

    def __init__(self, runs: list[list[str]]) -> None:
        self.items: list[str] = []
        self._runs = runs

    def __call__(self) -> None:
        self._runs.append(self.items)


def _grouped(channel: PostCommitChannel, runs: list[list[str]], item: str) -> None:
    channel.publish_group("broker", lambda: _Batch(runs)).items.append(item)


async def test_a_publish_group_is_queued_once_and_gathers_until_the_drain() -> None:
    channel = PostCommitChannel()
    runs: list[list[str]] = []
    _grouped(channel, runs, "a")
    _grouped(channel, runs, "b")
    await channel.drain(committed=True)
    _grouped(channel, runs, "c")
    await channel.drain(committed=True)

    assert runs == [["a", "b"], ["c"]]


async def test_a_discarded_publish_group_is_not_reused() -> None:
    channel = PostCommitChannel()
    runs: list[list[str]] = []
    _grouped(channel, runs, "rolled back")
    channel.discard()
    _grouped(channel, runs, "committed")
    await channel.drain(committed=True)

    assert runs == [["committed"]]
