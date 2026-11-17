"""The inline job service waits on its timer: countdowns, deferrals, backoff, and no timer."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.errors import JobFailedError, JobTimeoutError
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.outcome import JobRetry
from loom.core.job.policy import Backoff, Countdown
from loom.core.job.service import InlineJobService
from loom.core.job.timer import AsyncioTimer, JobDelayRefused
from loom.testing.jobs import ManualTimer
from tests.helpers.loggers import RecordingLogger

pytestmark = pytest.mark.timeout(10)

STEADY = Backoff(first_s=4, factor=2, max_s=100, jitter=False)


class AcmeError(Exception):
    pass


class AcmeJob(Job[str]):
    __retry__ = STEADY

    async def execute(self) -> str:
        return "done"


class Acme:
    """A service over a scripted executor and a manual timer."""

    def __init__(self, *script: object, timed: bool = True) -> None:
        self.timer = ManualTimer()
        self.executor = MagicMock(run=AsyncMock(side_effect=list(script) or ["done"]))
        self.service = InlineJobService(
            MagicMock(), self.executor, timer=self.timer if timed else None
        )

    @property
    def runs(self) -> list[tuple[int, int]]:
        return [
            (call.kwargs["delivery"].attempt, call.kwargs["delivery"].deferrals)
            for call in self.executor.run.await_args_list
        ]


@pytest.fixture(autouse=True)
def _no_leftover_dispatches() -> Iterator[None]:
    yield
    clear_pending_dispatches()


class TestCountdown:
    async def test_a_countdown_waits_for_the_timer_after_the_flush(self) -> None:
        acme = Acme()
        handle = acme.service.dispatch_after_commit(AcmeJob, countdown=60)

        await flush_pending_dispatches()
        await acme.timer.advance(59)
        assert acme.runs == []
        await acme.timer.advance(1)

        assert acme.runs == [(1, 0)]
        assert handle.wait() == "done"

    async def test_dispatch_now_with_a_countdown_waits_for_the_timer(self) -> None:
        acme = Acme()

        handle = await acme.service.dispatch_now(AcmeJob, countdown=60)
        assert acme.runs == []
        await acme.timer.advance(60)

        assert acme.runs == [(1, 0)]
        assert handle.wait() == "done"

    async def test_no_countdown_runs_without_a_timer_hop(self) -> None:
        acme = Acme()

        await acme.service.dispatch_now(AcmeJob)

        assert acme.runs == [(1, 0)]
        assert acme.timer.pending == 0


class TestRetries:
    async def test_a_job_retry_runs_again_after_its_delay_with_one_more_deferral(self) -> None:
        acme = Acme(JobRetry("warming up", 30), "done")

        handle = await acme.service.dispatch_now(AcmeJob)
        assert acme.runs == [(1, 0)]
        await acme.timer.advance(29)
        assert acme.runs == [(1, 0)]
        await acme.timer.advance(1)

        assert acme.runs == [(1, 0), (1, 1)]
        assert handle.wait() == "done"

    async def test_a_failure_waits_its_backoff_and_runs_as_the_next_attempt(self) -> None:
        acme = Acme(AcmeError("one"), AcmeError("two"), "done")

        handle = await acme.service.dispatch_now(AcmeJob)
        await acme.timer.advance(3)
        assert acme.runs == [(1, 0)]
        await acme.timer.advance(1)
        assert acme.runs == [(1, 0), (2, 0)]
        await acme.timer.advance(8)

        assert acme.runs == [(1, 0), (2, 0), (3, 0)]
        assert handle.wait() == "done"

    async def test_exhausted_attempts_reject_and_call_on_failure_once(self) -> None:
        failures: list[str] = []

        class Notify:
            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
                failures.append(exc_type)

        acme = Acme(AcmeError("one"), AcmeError("two"), AcmeError("three"))
        acme.service._factory.build = MagicMock(  # type: ignore[attr-defined]
            side_effect=lambda cls: Notify() if cls is Notify else MagicMock()
        )

        handle = await acme.service.dispatch_now(AcmeJob, on_failure=Notify)  # type: ignore[arg-type]
        await acme.timer.advance(4)
        await acme.timer.advance(8)

        assert acme.runs == [(1, 0), (2, 0), (3, 0)]
        assert acme.timer.pending == 0
        assert failures == ["AcmeError"]
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert str(failed.value.cause) == "three"

    async def test_a_job_retry_without_delay_goes_through_the_timer_not_the_stack(self) -> None:
        acme = Acme(*[JobRetry("spin", 0)] * 5, "done")

        await acme.service.dispatch_now(AcmeJob)

        assert acme.runs == [(1, 0)]
        assert acme.timer.pending == 1
        await acme.timer.advance(0)
        assert len(acme.runs) == 6
        assert acme.timer.pending == 0

    async def test_a_waiting_job_leaves_its_handle_unsettled(self) -> None:
        acme = Acme(AcmeError("one"), "done")

        handle = await acme.service.dispatch_now(AcmeJob)

        assert acme.timer.pending == 1
        with pytest.raises(JobTimeoutError):
            handle.wait(timeout=0.01)


class DefaultJob(Job[str]):
    async def execute(self) -> str:
        return "done"


class TestWithoutATimer:
    @pytest.fixture
    def acme(self) -> Acme:
        return Acme(AcmeError("boom"), timed=False)

    def test_dispatch_after_commit_refuses_a_countdown(self, acme: Acme) -> None:
        with pytest.raises(JobDelayRefused, match="60s"):
            acme.service.dispatch_after_commit(AcmeJob, countdown=60)

    async def test_dispatch_now_refuses_a_countdown(self, acme: Acme) -> None:
        with pytest.raises(JobDelayRefused, match="60s"):
            await acme.service.dispatch_now(AcmeJob, countdown=60)

    async def test_a_zero_countdown_still_runs(self) -> None:
        acme = Acme("done", timed=False)

        handle = await acme.service.dispatch_now(AcmeJob, countdown=0)

        assert handle.wait() == "done"

    async def test_a_job_retry_is_rejected_as_one_that_cannot_wait(self) -> None:
        signal = JobRetry("warming up", 30)
        acme = Acme(signal, timed=False)

        handle = await acme.service.dispatch_now(AcmeJob)

        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert failed.value.cause is signal
        assert acme.runs == [(1, 0)]

    async def test_a_zero_delay_job_retry_is_rejected_too(self) -> None:
        signal = JobRetry("warming up", 0)
        acme = Acme(signal, timed=False)

        handle = await acme.service.dispatch_now(AcmeJob)

        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert failed.value.cause is signal
        assert acme.runs == [(1, 0)]

    async def test_a_failure_on_the_default_policy_runs_once_and_keeps_its_own_error(
        self,
    ) -> None:
        notified: list[str] = []

        class Notify:
            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
                notified.append(exc_type)

        acme = Acme(AcmeError("db down"), timed=False)
        acme.service._factory.build = MagicMock(  # type: ignore[attr-defined]
            side_effect=lambda cls: Notify() if cls is Notify else MagicMock()
        )

        handle = await acme.service.dispatch_now(DefaultJob, on_failure=Notify)  # type: ignore[arg-type]

        assert acme.runs == [(1, 0)]
        assert notified == ["AcmeError"]
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert type(failed.value.cause) is AcmeError


class TestAsyncioTimer:
    async def test_a_step_that_has_run_leaves_nothing_to_lose(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        log = RecordingLogger()
        monkeypatch.setattr("loom.core.job.timer._log", log)
        yield_once = asyncio.sleep
        ran = asyncio.Event()

        async def instant(seconds: float) -> None:
            await yield_once(0)

        async def step() -> None:
            ran.set()

        monkeypatch.setattr(asyncio, "sleep", instant)
        timer = AsyncioTimer()

        timer.schedule(Countdown(60), step, job="AcmeJob", task_id="t")
        await asyncio.wait_for(ran.wait(), 1)
        await yield_once(0)
        await yield_once(0)
        await timer.close()

        assert log.calls == []

    async def test_a_step_runs_after_its_delay(self, monkeypatch: pytest.MonkeyPatch) -> None:
        slept: list[float] = []
        ran = asyncio.Event()

        async def instant(seconds: float) -> None:
            slept.append(seconds)

        async def step() -> None:
            ran.set()

        monkeypatch.setattr(asyncio, "sleep", instant)
        timer = AsyncioTimer()

        timer.schedule(Countdown(60), step, job="AcmeJob", task_id="t")
        await asyncio.wait_for(ran.wait(), 1)

        assert slept == [60]
        await timer.close()

    async def test_close_cancels_the_pending_steps_and_logs_the_lost_jobs_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        log = RecordingLogger()
        monkeypatch.setattr("loom.core.job.timer._log", log)
        ran: list[int] = []
        timer = AsyncioTimer()

        async def step() -> None:
            ran.append(1)

        for _ in range(3):
            timer.schedule(Countdown(900), step, job="AcmeJob", task_id=f"t{len(ran)}")
            ran.append(0)
        ran.clear()
        pending = tuple(timer._tasks)
        await timer.close()

        assert ran == []
        assert all(task.cancelled() for task in pending)
        assert log.calls == [
            ("Delayed jobs lost at shutdown", {"jobs": ["AcmeJob:t0", "AcmeJob:t1", "AcmeJob:t2"]})
        ]

    async def test_close_with_nothing_pending_logs_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        log = RecordingLogger()
        monkeypatch.setattr("loom.core.job.timer._log", log)

        await AsyncioTimer().close()

        assert log.calls == []

    async def test_each_timer_owns_its_tasks(self) -> None:
        first, second = AsyncioTimer(), AsyncioTimer()

        async def step() -> None:
            return None

        first.schedule(Countdown(900), step, job="AcmeJob", task_id="a")
        second.schedule(Countdown(900), step, job="AcmeJob", task_id="b")
        (kept,) = second._tasks
        await first.close()

        assert not kept.done()
        await second.close()
        assert kept.cancelled()
