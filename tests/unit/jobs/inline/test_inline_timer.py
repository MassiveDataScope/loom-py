"""The inline job service waits on its timer: countdowns, postponements, and no timer.

Which attempt a failure waits for, and how a postponement counts, is the inline
contract in ``tests/contract/jobs``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, ClassVar

import pytest

from loom.core.engine.events import AbandonReason, EventKind
from loom.core.errors import JobFailedError, JobTimeoutError
from loom.core.job.callback import JobCallback
from loom.core.job.config import JobShutdown, ShutdownMode
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import JobPostponement
from loom.core.job.policy import Backoff, Countdown
from loom.core.job.timer import JobDelayRefusedError, Step
from loom.core.logger.failure import exception_type
from loom.core.use_case import Delivery
from loom.jobs.inline import AsyncioJobTimer
from loom.testing.jobs import ManualTimer
from tests.helpers.jobs import InlineServiceFixture, JobKernel, RecordingExecutionObserver
from tests.helpers.loggers import logged_events

pytestmark = pytest.mark.timeout(10)

STEADY = Backoff(first_s=4, factor=2, max_s=100, jitter=False)
SPINS = 5
ABANDONED = "Job abandoned"


class AcmeError(Exception):
    pass


class Counted(Job[str]):
    """A job that records the attempt and postponements of each of its runs."""

    __retry__ = STEADY
    runs: ClassVar[list[tuple[int, int]]] = []

    def count(self, delivery: JobDelivery | None) -> JobDelivery:
        assert delivery is not None
        self.runs.append((delivery.attempt, delivery.postponements))
        return delivery


class SucceedingJob(Counted):
    def execute(self, delivery: JobDelivery | None = Delivery()) -> str:
        self.count(delivery)
        return "done"


class FailingJob(Counted):
    def execute(self, delivery: JobDelivery | None = Delivery()) -> str:
        self.count(delivery)
        raise AcmeError(f"attempt {self.runs[-1][0]}")


class PostponingJob(Counted):
    def execute(self, delay: int, delivery: JobDelivery | None = Delivery()) -> str:
        self.count(delivery)
        raise JobPostponement("warming up", delay)


class SpinningJob(Counted):
    """Asks to run again at once for its first five runs."""

    def execute(self, delivery: JobDelivery | None = Delivery()) -> str:
        spent = self.count(delivery).postponements
        if spent < SPINS:
            raise JobPostponement("spin", 0)
        return "done"


class Notify(JobCallback):
    failures: ClassVar[list[str]] = []

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
        self.failures.append(exc_type)


class Acme:
    """An inline service over the real kernel, with a manual timer or none."""

    def __init__(self, *, timed: bool = True) -> None:
        kernel = JobKernel()
        for job_type in (SucceedingJob, FailingJob, PostponingJob, SpinningJob, Notify):
            kernel.factory.register(job_type)
        self.timer = ManualTimer()
        self.service = InlineServiceFixture(
            kernel.factory, kernel.executor, timer=self.timer if timed else None
        )

    @property
    def runs(self) -> list[tuple[int, int]]:
        return list(Counted.runs)


@pytest.fixture(autouse=True)
def _forget_runs() -> Iterator[None]:
    Counted.runs.clear()
    Notify.failures.clear()
    yield
    clear_pending_dispatches()


class TestCountdown:
    async def test_a_countdown_waits_for_the_timer_after_the_flush(self) -> None:
        acme = Acme()
        handle = acme.service.dispatch_after_commit(SucceedingJob, countdown=60)

        await flush_pending_dispatches()
        await acme.timer.advance(59)
        assert acme.runs == []
        await acme.timer.advance(1)

        assert acme.runs == [(1, 0)]
        assert handle.wait() == "done"

    async def test_dispatch_now_with_a_countdown_waits_for_the_timer(self) -> None:
        acme = Acme()

        handle = await acme.service.dispatch_now(SucceedingJob, countdown=60)
        assert acme.runs == []
        await acme.timer.advance(60)

        assert acme.runs == [(1, 0)]
        assert handle.wait() == "done"

    async def test_no_countdown_runs_once_the_timer_is_idle_without_moving_the_clock(self) -> None:
        acme = Acme()
        await acme.service.dispatch_now(SucceedingJob)

        await acme.timer.wait_until_idle()

        assert acme.runs == [(1, 0)]
        assert acme.timer.pending == 0


class TestRetries:
    async def test_exhausted_attempts_abandon_and_call_on_failure_once(self) -> None:
        acme = Acme()

        handle = await acme.service.dispatch_now(FailingJob, on_failure=Notify)
        await acme.timer.advance(4)
        await acme.timer.advance(8)

        assert acme.runs == [(1, 0), (2, 0), (3, 0)]
        assert acme.timer.pending == 0
        assert Notify.failures == [exception_type(AcmeError)]
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert str(failed.value.cause) == "attempt 3"

    async def test_a_job_postponement_without_delay_goes_through_the_timer_not_the_stack(
        self,
    ) -> None:
        acme = Acme()
        await acme.service.dispatch_now(SpinningJob)

        await acme.timer.advance(0)

        assert len(acme.runs) == SPINS + 1
        assert acme.timer.pending == 0

    async def test_a_waiting_job_leaves_its_handle_unsettled(self) -> None:
        acme = Acme()

        handle = await acme.service.dispatch_now(FailingJob)

        assert acme.timer.pending == 1
        with pytest.raises(JobTimeoutError):
            handle.wait(timeout=0.01)


class TestWithoutATimer:
    def test_dispatch_after_commit_refuses_a_countdown(self) -> None:
        with pytest.raises(JobDelayRefusedError, match="60s"):
            Acme(timed=False).service.dispatch_after_commit(SucceedingJob, countdown=60)

    async def test_dispatch_now_refuses_a_countdown(self) -> None:
        with pytest.raises(JobDelayRefusedError, match="60s"):
            await Acme(timed=False).service.dispatch_now(SucceedingJob, countdown=60)

    async def test_a_zero_countdown_still_runs(self) -> None:
        acme = Acme(timed=False)
        handle = await acme.service.dispatch_now(SucceedingJob, countdown=0)

        await acme.service.wait_until_idle()

        assert handle.wait() == "done"

    @pytest.mark.parametrize("delay", [30, 0])
    async def test_a_job_postponement_is_abandoned_as_one_that_cannot_wait(
        self, delay: int
    ) -> None:
        acme = Acme(timed=False)
        handle = await acme.service.dispatch_now(PostponingJob, params={"delay": delay})

        await acme.service.wait_until_idle()

        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert isinstance(failed.value.cause, JobPostponement)
        assert acme.runs == [(1, 0)]

    async def test_a_failure_runs_once_and_keeps_its_own_error(self) -> None:
        acme = Acme(timed=False)
        handle = await acme.service.dispatch_now(FailingJob, on_failure=Notify)

        await acme.service.wait_until_idle()

        assert acme.runs == [(1, 0)]
        assert Notify.failures == [exception_type(AcmeError)]
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert type(failed.value.cause) is AcmeError


class TestAsyncioTimer:
    async def test_a_step_runs_once_its_delay_is_over(self) -> None:
        ran = asyncio.Event()
        timer = AsyncioJobTimer()

        async def step() -> None:
            ran.set()

        timer.schedule(Countdown(0), step, job="AcmeJob", task_id="t")
        await asyncio.wait_for(ran.wait(), 1)
        await timer.close()

    async def test_a_step_does_not_run_before_its_delay_is_over(self) -> None:
        ran = asyncio.Event()
        timer = AsyncioJobTimer()

        async def step() -> None:
            ran.set()

        timer.schedule(Countdown(900), step, job="AcmeJob", task_id="t")
        for _ in range(3):
            await asyncio.sleep(0)
        await timer.close()

        assert not ran.is_set()

    async def test_a_step_that_has_run_leaves_nothing_to_lose(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ran = asyncio.Event()
        timer = AsyncioJobTimer()

        async def step() -> None:
            ran.set()

        timer.schedule(Countdown(0), step, job="AcmeJob", task_id="t")
        await asyncio.wait_for(ran.wait(), 1)
        await asyncio.sleep(0)
        await timer.close()

        assert ABANDONED not in caplog.text

    async def test_close_cancels_the_pending_steps_and_logs_each_lost_job_as_an_abandonment(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ran: list[int] = []
        timer = AsyncioJobTimer()

        async def step() -> None:
            ran.append(1)

        for n in range(3):
            timer.schedule(Countdown(900), step, job="AcmeJob", task_id=f"t{n}")
        await timer.close()

        assert ran == []
        assert [
            (entry["level"], entry["loom.job"], entry["loom.task_id"], entry["loom.reason"])
            for entry in _abandonments(caplog)
        ] == [("error", "AcmeJob", f"t{n}", "shutdown") for n in range(3)]

    async def test_close_with_nothing_pending_logs_nothing(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        await AsyncioJobTimer().close()

        assert ABANDONED not in caplog.text

    async def test_each_timer_owns_its_tasks(self) -> None:
        first, second = AsyncioJobTimer(), AsyncioJobTimer()
        ran: list[str] = []

        async def step() -> None:
            ran.append("second")

        first.schedule(Countdown(900), step, job="AcmeJob", task_id="a")
        second.schedule(Countdown(0), step, job="AcmeJob", task_id="b")
        await first.close()
        await asyncio.sleep(0.01)
        await second.close()

        assert ran == ["second"]


def _abandonments(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    return [entry for entry in logged_events(caplog) if entry["event"] == ABANDONED]


def _lost_jobs(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [f"{entry['loom.job']}:{entry['loom.task_id']}" for entry in _abandonments(caplog)]


class TestAsyncioTimerShutdown:
    async def test_cancel_is_the_default_and_drops_what_is_pending(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        timer = AsyncioJobTimer()
        ran: list[str] = []

        async def step() -> None:
            ran.append("ran")

        timer.schedule(Countdown(900), step, job="AcmeJob", task_id="t")
        await timer.close()

        assert (ran, _lost_jobs(caplog)) == ([], ["AcmeJob:t"])

    async def test_wait_runs_the_pending_steps_and_the_steps_they_schedule(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        timer = AsyncioJobTimer(JobShutdown(mode=ShutdownMode.WAIT))
        ran: list[str] = []
        started = asyncio.Event()

        async def callback() -> None:
            ran.append("callback")

        async def retry() -> None:
            ran.append("retry")
            timer.schedule(Countdown(0), callback, job="AcmeJob", task_id="callback")

        async def first() -> None:
            started.set()
            await asyncio.sleep(0.005)
            ran.append("first")
            timer.schedule(Countdown(0), retry, job="AcmeJob", task_id="retry")

        timer.schedule(Countdown(0), first, job="AcmeJob", task_id="first")
        await started.wait()
        await timer.close()

        assert ran == ["first", "retry", "callback"]
        assert _lost_jobs(caplog) == []

    async def test_wait_with_nothing_pending_returns_at_once(self) -> None:
        await asyncio.wait_for(AsyncioJobTimer(JobShutdown(mode=ShutdownMode.WAIT)).close(), 1)

    async def test_wait_cancels_and_logs_what_is_pending_when_the_timeout_expires(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        timer = AsyncioJobTimer(JobShutdown(mode=ShutdownMode.WAIT, timeout_s=0.01))
        ran: list[str] = []

        def step(name: str) -> Step:
            async def run() -> None:
                ran.append(name)

            return run

        timer.schedule(Countdown(0), step("soon"), job="AcmeJob", task_id="soon")
        timer.schedule(Countdown(900), step("late"), job="AcmeJob", task_id="late")
        await timer.close()

        assert (ran, _lost_jobs(caplog)) == (["soon"], ["AcmeJob:late"])

    async def test_wait_without_a_timeout_waits_for_a_step_still_running(self) -> None:
        timer = AsyncioJobTimer(JobShutdown(mode=ShutdownMode.WAIT))
        release = asyncio.Event()
        finished = asyncio.Event()

        async def step() -> None:
            await release.wait()
            finished.set()

        timer.schedule(Countdown(0), step, job="AcmeJob", task_id="t")
        closing = asyncio.ensure_future(timer.close())
        await asyncio.sleep(0.005)
        assert not closing.done()
        release.set()
        await asyncio.wait_for(closing, 1)

        assert finished.is_set()


def _observed_timer(
    shutdown: JobShutdown | None = None,
) -> tuple[AsyncioJobTimer, RecordingExecutionObserver]:
    observer = RecordingExecutionObserver()
    timer = AsyncioJobTimer(
        shutdown or JobShutdown(mode=ShutdownMode.CANCEL), execution_observer=observer
    )
    return timer, observer


def _reported(
    observer: RecordingExecutionObserver,
) -> list[tuple[EventKind, str, AbandonReason | None]]:
    return [(event.kind, event.use_case_name, event.reason) for event in observer.events]


async def _never() -> None:
    return None


class TestAsyncioTimerAbandonments:
    async def test_cancel_reports_each_pending_job_as_abandoned_by_shutdown(self) -> None:
        timer, observer = _observed_timer()

        timer.schedule(Countdown(900), _never, job="AcmeJob", task_id="a")
        timer.schedule(Countdown(900), _never, job="OtherJob", task_id="b")
        await timer.close()

        assert _reported(observer) == [
            (EventKind.JOB_ABANDONED, "AcmeJob", AbandonReason.SHUTDOWN),
            (EventKind.JOB_ABANDONED, "OtherJob", AbandonReason.SHUTDOWN),
        ]

    async def test_a_wait_timeout_reports_only_the_jobs_left(self) -> None:
        timer, observer = _observed_timer(JobShutdown(mode=ShutdownMode.WAIT, timeout_s=0.01))

        timer.schedule(Countdown(0), _never, job="AcmeJob", task_id="soon")
        timer.schedule(Countdown(900), _never, job="LateJob", task_id="late")
        await timer.close()

        assert _reported(observer) == [(EventKind.JOB_ABANDONED, "LateJob", AbandonReason.SHUTDOWN)]

    async def test_a_wait_that_completes_reports_nothing(self) -> None:
        timer, observer = _observed_timer(JobShutdown(mode=ShutdownMode.WAIT))

        timer.schedule(Countdown(0), _never, job="AcmeJob", task_id="t")
        await timer.close()

        assert observer.events == []

    async def test_a_step_that_raises_outside_the_settle_path_is_reported_as_crashed(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        timer, observer = _observed_timer()
        crashed = asyncio.Event()

        async def step() -> None:
            crashed.set()
            raise AcmeError("notify down")

        timer.schedule(Countdown(0), step, job="AcmeJob", task_id="t")
        await asyncio.wait_for(crashed.wait(), 1)
        await asyncio.sleep(0)
        await timer.close()

        (event,) = observer.events
        assert (event.kind, event.reason, type(event.error)) == (
            EventKind.JOB_ABANDONED,
            AbandonReason.STEP_CRASHED,
            AcmeError,
        )
        (logged,) = _abandonments(caplog)
        assert (logged["level"], logged["loom.reason"]) == ("error", "step_crashed")
