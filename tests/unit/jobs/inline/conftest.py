"""Inline job test fixtures: the pending dispatch context the core job tests isolate."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from loom.core.job.policy import Countdown
from loom.core.job.timer import JobTimer
from loom.jobs.inline import AsyncioJobTimer
from tests.helpers.jobs import (
    InlineServiceFixture,
    JobActivity,
    JobKernel,
    RecordingExecutionObserver,
)
from tests.unit.core.job.conftest import _reset_pending_dispatches as _reset_pending_dispatches
from tests.unit.jobs.inline._jobs import AcmeError, ChainedJob, RecordedJob

InlineServiceBuilder = Callable[..., InlineServiceFixture]


@pytest.fixture
def inline_service_on(activity: JobActivity) -> InlineServiceBuilder:
    """Build an inline service running the recorded and chained jobs, on a timer and observer."""

    def build(
        timer: JobTimer | None = None, observer: RecordingExecutionObserver | None = None
    ) -> InlineServiceFixture:
        kernel = JobKernel()
        kernel.container.register_instance(JobActivity, activity)
        return kernel.serve_inline(
            RecordedJob,
            ChainedJob,
            timer=AsyncioJobTimer() if timer is None else timer,
            execution_observer=observer,
        )

    return build


@pytest.fixture
def inline_service(inline_service_on: InlineServiceBuilder) -> InlineServiceFixture:
    """An inline service on its own asyncio timer."""
    return inline_service_on()


@pytest.fixture
def caller_failing_after_sending(
    inline_service: InlineServiceFixture,
) -> Callable[[], Awaitable[None]]:
    """A caller that sends a recorded job and then fails."""

    async def send_then_fail() -> None:
        await inline_service.dispatch_now(RecordedJob)
        raise AcmeError("the caller failed after sending")

    return send_then_fail


@pytest.fixture
def step_never_run() -> Callable[[], Awaitable[None]]:
    """A step that fails the test if its timer runs it."""

    async def fail_if_run() -> None:
        raise AssertionError("a step waiting 900 s ran")

    return fail_if_run


@pytest.fixture
async def timer_holding_a_step(
    step_never_run: Callable[[], Awaitable[None]],
) -> AsyncIterator[AsyncioJobTimer]:
    """An asyncio timer with one step waiting for its countdown, closed at teardown."""
    timer = AsyncioJobTimer()
    timer.schedule(Countdown(900), step_never_run, job="AcmeJob", task_id="t")
    yield timer
    await timer.close()


@pytest.fixture
async def waiting_for_idle(
    timer_holding_a_step: AsyncioJobTimer,
) -> AsyncIterator[asyncio.Task[None]]:
    """A wait for the timer to be idle, already started and still pending."""
    waiting = asyncio.ensure_future(timer_holding_a_step.wait_until_idle())
    await asyncio.sleep(0)
    yield waiting
    await timer_holding_a_step.close()
    await waiting
