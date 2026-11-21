"""The inline transport enqueues every dispatch on its timer, as a queue would, and runs it later.

A job runs once the call that dispatched it has returned, never inside it, and
``wait_until_idle`` returns once no job is running or waiting.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from loom.core.engine.events import EventKind
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.service import JobService
from loom.jobs.inline import AsyncioJobTimer
from loom.testing.jobs import ManualTimer, RecordingJobTransport
from tests.helpers.jobs import InlineServiceFixture, JobActivity, RecordingExecutionObserver
from tests.unit.jobs.inline._jobs import AcmeError, ChainedJob, RecordedJob
from tests.unit.jobs.inline.conftest import InlineServiceBuilder

pytestmark = pytest.mark.timeout(10)

CHAIN_RUN_ONE_AFTER_ANOTHER = ["enter 0", "exit 0", "enter 1", "exit 1", "enter 2", "exit 2"]


class TestEnqueue:
    async def test_dispatch_now_returns_before_the_job_runs(
        self, inline_service: InlineServiceFixture, activity: JobActivity
    ) -> None:
        await inline_service.dispatch_now(RecordedJob)

        assert activity.entries == []

    async def test_a_job_sent_now_runs_once_the_service_waits_until_idle(
        self, inline_service: InlineServiceFixture, activity: JobActivity
    ) -> None:
        handle = await inline_service.dispatch_now(RecordedJob)

        await inline_service.wait_until_idle()

        assert (activity.entries, handle.wait()) == (["ran"], "done")

    async def test_a_chain_of_jobs_runs_one_after_another_without_nesting(
        self, inline_service: InlineServiceFixture, activity: JobActivity
    ) -> None:
        await inline_service.dispatch_now(ChainedJob, params={"link": 0})

        await inline_service.wait_until_idle()

        assert activity.entries == CHAIN_RUN_ONE_AFTER_ANOTHER

    async def test_a_caller_failing_after_sending_does_not_stop_the_job(
        self,
        inline_service: InlineServiceFixture,
        caller_failing_after_sending: Callable[[], Awaitable[None]],
        activity: JobActivity,
    ) -> None:
        with pytest.raises(AcmeError):
            await caller_failing_after_sending()

        await inline_service.wait_until_idle()

        assert activity.entries == ["ran"]

    async def test_the_post_commit_flush_enqueues_the_job_too(
        self, inline_service: InlineServiceFixture, activity: JobActivity
    ) -> None:
        inline_service.dispatch_after_commit(RecordedJob)

        await flush_pending_dispatches()

        assert activity.entries == []

    async def test_a_job_flushed_after_the_commit_runs_once_the_service_waits_until_idle(
        self, inline_service: InlineServiceFixture, activity: JobActivity
    ) -> None:
        inline_service.dispatch_after_commit(RecordedJob)
        await flush_pending_dispatches()

        await inline_service.wait_until_idle()

        assert activity.entries == ["ran"]

    async def test_observers_still_see_the_job_start_and_succeed(
        self, inline_service_on: InlineServiceBuilder
    ) -> None:
        observer = RecordingExecutionObserver()
        service = inline_service_on(observer=observer)
        await service.dispatch_now(RecordedJob)

        await service.wait_until_idle()

        assert observer.kinds == [EventKind.JOB_STARTED, EventKind.JOB_SUCCEEDED]

    async def test_observers_still_see_a_job_lost_at_shutdown_as_abandoned(
        self, inline_service_on: InlineServiceBuilder, activity: JobActivity
    ) -> None:
        observer = RecordingExecutionObserver()
        timer = AsyncioJobTimer(execution_observer=observer)
        service = inline_service_on(timer, observer)
        await service.dispatch_now(RecordedJob)

        await timer.close()

        assert (activity.entries, observer.kinds) == ([], [EventKind.JOB_ABANDONED])


class TestAsyncioTimerIdle:
    async def test_it_returns_at_once_when_nothing_is_pending(self) -> None:
        idle = await asyncio.wait_for(AsyncioJobTimer().wait_until_idle(), 1)

        assert idle is None

    async def test_it_stays_busy_while_a_step_waits_for_its_countdown(
        self, waiting_for_idle: asyncio.Task[None]
    ) -> None:
        finished, _ = await asyncio.wait({waiting_for_idle}, timeout=0.05)

        assert finished == set()

    async def test_closing_the_timer_releases_a_wait_for_a_step_still_counting_down(
        self, timer_holding_a_step: AsyncioJobTimer, waiting_for_idle: asyncio.Task[None]
    ) -> None:
        await timer_holding_a_step.close()

        finished, _ = await asyncio.wait({waiting_for_idle}, timeout=1)

        assert finished == {waiting_for_idle}


class TestManualTimerIdle:
    async def test_it_runs_a_job_sent_with_no_countdown(
        self, inline_service_on: InlineServiceBuilder, activity: JobActivity
    ) -> None:
        timer = ManualTimer()
        service = inline_service_on(timer)
        await service.dispatch_now(RecordedJob)

        await service.wait_until_idle()

        assert (activity.entries, timer.pending) == (["ran"], 0)

    async def test_it_runs_a_chain_sent_with_no_countdown(
        self, inline_service_on: InlineServiceBuilder, activity: JobActivity
    ) -> None:
        service = inline_service_on(ManualTimer())
        await service.dispatch_now(ChainedJob, params={"link": 0})

        await service.wait_until_idle()

        assert activity.entries == CHAIN_RUN_ONE_AFTER_ANOTHER

    async def test_it_leaves_a_countdown_for_the_clock(
        self, inline_service_on: InlineServiceBuilder, activity: JobActivity
    ) -> None:
        timer = ManualTimer()
        service = inline_service_on(timer)
        await service.dispatch_now(RecordedJob, countdown=60)

        await service.wait_until_idle()

        assert (activity.entries, timer.pending) == ([], 1)


class TestBrokerIdle:
    async def test_a_service_whose_jobs_run_elsewhere_returns_at_once(self) -> None:
        service = JobService(RecordingJobTransport())
        await service.dispatch_now(RecordedJob)

        idle = await asyncio.wait_for(service.wait_until_idle(), 1)

        assert idle is None
