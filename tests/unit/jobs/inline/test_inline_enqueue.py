"""The inline transport enqueues every dispatch on its timer, as a queue would, and runs it later.

A job runs once the call that dispatched it has returned, never inside it, and
``wait_until_idle`` returns once no job is running or waiting.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import ClassVar

import pytest

from loom.core.engine.events import EventKind
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.policy import Countdown
from loom.core.job.service import JobService
from loom.jobs.inline import AsyncioJobTimer
from loom.testing.jobs import ManualTimer, RecordingJobTransport
from tests.helpers.jobs import InlineServiceFixture, JobKernel, RecordingExecutionObserver

pytestmark = pytest.mark.timeout(10)


class AcmeError(Exception):
    pass


class RecordedJob(Job[str]):
    """Records each run in :attr:`trail`."""

    trail: ClassVar[list[str]] = []

    async def execute(self) -> str:
        self.trail.append("ran")
        return "done"


class ChainedJob(Job[None]):
    """Dispatches the next link of a chain of three, recording when each link enters and exits."""

    trail: ClassVar[list[str]] = []
    jobs: ClassVar[JobService]

    async def execute(self, link: int) -> None:
        self.trail.append(f"enter {link}")
        if link < 2:
            await self.jobs.dispatch_now(ChainedJob, params={"link": link + 1})
        self.trail.append(f"exit {link}")


@pytest.fixture(autouse=True)
def _forget_trails() -> Iterator[None]:
    RecordedJob.trail.clear()
    ChainedJob.trail.clear()
    yield
    clear_pending_dispatches()


def _inline_service(
    timer: AsyncioJobTimer | ManualTimer | None = None,
    observer: RecordingExecutionObserver | None = None,
) -> InlineServiceFixture:
    kernel = JobKernel()
    for job_type in (RecordedJob, ChainedJob):
        kernel.factory.register(job_type)
    service = InlineServiceFixture(
        kernel.factory,
        kernel.executor,
        timer=AsyncioJobTimer() if timer is None else timer,
        execution_observer=observer,
    )
    ChainedJob.jobs = service
    return service


async def _dispatch_then_fail(service: JobService) -> None:
    await service.dispatch_now(RecordedJob)
    raise AcmeError("the caller failed after sending")


class TestEnqueue:
    async def test_dispatch_now_returns_before_the_job_runs(self) -> None:
        service = _inline_service()

        await service.dispatch_now(RecordedJob)

        assert RecordedJob.trail == []

    async def test_a_job_sent_now_runs_once_the_service_waits_until_idle(self) -> None:
        service = _inline_service()
        handle = await service.dispatch_now(RecordedJob)

        await service.wait_until_idle()

        assert (RecordedJob.trail, handle.wait()) == (["ran"], "done")

    async def test_a_chain_of_jobs_runs_one_after_another_without_nesting(self) -> None:
        service = _inline_service()
        await service.dispatch_now(ChainedJob, params={"link": 0})

        await service.wait_until_idle()

        assert ChainedJob.trail == [
            "enter 0",
            "exit 0",
            "enter 1",
            "exit 1",
            "enter 2",
            "exit 2",
        ]

    async def test_a_caller_failing_after_sending_does_not_stop_the_job(self) -> None:
        service = _inline_service()
        with pytest.raises(AcmeError):
            await _dispatch_then_fail(service)

        await service.wait_until_idle()

        assert RecordedJob.trail == ["ran"]

    async def test_the_post_commit_flush_enqueues_the_job_too(self) -> None:
        service = _inline_service()
        service.dispatch_after_commit(RecordedJob)

        await flush_pending_dispatches()

        assert RecordedJob.trail == []

    async def test_a_job_flushed_after_the_commit_runs_once_the_service_waits_until_idle(
        self,
    ) -> None:
        service = _inline_service()
        service.dispatch_after_commit(RecordedJob)
        await flush_pending_dispatches()

        await service.wait_until_idle()

        assert RecordedJob.trail == ["ran"]

    async def test_observers_still_see_the_job_start_and_succeed(self) -> None:
        observer = RecordingExecutionObserver()
        service = _inline_service(observer=observer)
        await service.dispatch_now(RecordedJob)

        await service.wait_until_idle()

        assert observer.kinds == [EventKind.JOB_STARTED, EventKind.JOB_SUCCEEDED]

    async def test_observers_still_see_a_job_lost_at_shutdown_as_abandoned(self) -> None:
        observer = RecordingExecutionObserver()
        timer = AsyncioJobTimer(execution_observer=observer)
        service = _inline_service(timer, observer)
        await service.dispatch_now(RecordedJob)

        await timer.close()

        assert (RecordedJob.trail, observer.kinds) == ([], [EventKind.JOB_ABANDONED])


class TestAsyncioTimerIdle:
    async def test_it_returns_at_once_when_nothing_is_pending(self) -> None:
        await asyncio.wait_for(AsyncioJobTimer().wait_until_idle(), 1)

    async def test_it_waits_for_a_step_still_waiting_for_its_countdown(self) -> None:
        timer = AsyncioJobTimer()
        timer.schedule(Countdown(900), _never_runs, job="AcmeJob", task_id="t")
        waiting = asyncio.ensure_future(timer.wait_until_idle())
        for _ in range(3):
            await asyncio.sleep(0)

        assert not waiting.done()
        await timer.close()
        await asyncio.wait_for(waiting, 1)


async def _never_runs() -> None:
    raise AssertionError("a step waiting 900 s ran")


class TestManualTimerIdle:
    async def test_it_runs_a_job_sent_with_no_countdown(self) -> None:
        timer = ManualTimer()
        service = _inline_service(timer)
        await service.dispatch_now(RecordedJob)

        await service.wait_until_idle()

        assert (RecordedJob.trail, timer.pending) == (["ran"], 0)

    async def test_it_runs_a_chain_sent_with_no_countdown(self) -> None:
        service = _inline_service(ManualTimer())
        await service.dispatch_now(ChainedJob, params={"link": 0})

        await service.wait_until_idle()

        assert ChainedJob.trail[-1] == "exit 2"

    async def test_it_leaves_a_countdown_for_the_clock(self) -> None:
        timer = ManualTimer()
        service = _inline_service(timer)
        await service.dispatch_now(RecordedJob, countdown=60)

        await service.wait_until_idle()

        assert (RecordedJob.trail, timer.pending) == ([], 1)


class TestBrokerIdle:
    async def test_a_service_whose_jobs_run_elsewhere_returns_at_once(self) -> None:
        service = JobService(RecordingJobTransport())
        await service.dispatch_now(RecordedJob)

        await asyncio.wait_for(service.wait_until_idle(), 1)
