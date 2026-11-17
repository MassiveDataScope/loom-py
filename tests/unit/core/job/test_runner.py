from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.core.engine.events import EventKind, RuntimeEvent, Status
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope
from loom.core.job.job import Job
from loom.core.job.outcome import (
    Deferred,
    Failed,
    JobOutcome,
    JobRejection,
    JobRetry,
    Rejected,
    Succeeded,
)
from loom.core.job.policy import Backoff, RetryPolicy
from loom.core.job.runner import JobRunner

POLICY = RetryPolicy(Backoff(first_s=4, factor=2, max_s=100, jitter=False), max_attempts=3)


class AcmeJob(Job[None]):
    async def execute(self) -> None:
        return None


class Metrics:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    def on_event(self, event: RuntimeEvent) -> None:
        self.events.append(event)


class UnretryableError(Exception):
    retryable = False


def _delivery(
    *, attempt: int = 1, deferrals: int = 0, timeout_s: float | None = None
) -> JobDelivery:
    return JobDelivery(
        job_type=AcmeJob,
        envelope=JobEnvelope(job="AcmeJob", trace_id="trace-1"),
        task_id="task-1",
        attempt=attempt,
        deferrals=deferrals,
        policy=POLICY,
        timeout_s=timeout_s,
    )


def _runner(behaviour: Any, metrics: Metrics | None = None) -> JobRunner:
    async def invocation(delivery: JobDelivery) -> Any:
        return await behaviour(delivery)

    return JobRunner(invocation, metrics)


def _returning(value: Any) -> Any:
    async def behave(delivery: JobDelivery) -> Any:
        return value

    return behave


def _raising(error: BaseException) -> Any:
    async def behave(delivery: JobDelivery) -> Any:
        raise error

    return behave


class TestJobRunnerOutcome:
    async def test_a_result_succeeds(self) -> None:
        assert await _runner(_returning(7)).run(_delivery()) == Succeeded(7)

    async def test_a_failure_with_attempts_left_fails_with_the_backoff_delay(self) -> None:
        error = ValueError("boom")
        outcome = await _runner(_raising(error)).run(_delivery(attempt=2))
        assert isinstance(outcome, Failed)
        assert outcome.error is error
        assert outcome.delay.seconds == 8

    async def test_the_last_attempt_rejects_with_the_class_of_the_error(self) -> None:
        error = ValueError("boom")
        assert await _runner(_raising(error)).run(_delivery(attempt=3)) == Rejected(
            "ValueError", error
        )

    async def test_an_error_that_cannot_succeed_again_rejects_at_the_first_attempt(self) -> None:
        error = UnretryableError("never")
        assert await _runner(_raising(error)).run(_delivery()) == Rejected(
            "UnretryableError", error
        )

    async def test_a_job_retry_defers_with_its_delay(self) -> None:
        outcome = await _runner(_raising(JobRetry("later", 30))).run(_delivery(attempt=3))
        assert outcome == Deferred(outcome.delay, "later")
        assert isinstance(outcome, Deferred)
        assert outcome.delay.seconds == 30

    async def test_a_job_retry_without_a_delay_takes_the_backoff_step_of_the_next_deferral(
        self,
    ) -> None:
        outcome = await _runner(_raising(JobRetry("later"))).run(_delivery(deferrals=2))
        assert isinstance(outcome, Deferred)
        assert outcome.delay.seconds == 16

    async def test_a_job_retry_never_exhausts_the_attempts(self) -> None:
        outcome = await _runner(_raising(JobRetry("later", 1))).run(
            _delivery(attempt=3, deferrals=9)
        )
        assert isinstance(outcome, Deferred)

    async def test_deferrals_do_not_spend_attempts_of_a_later_failure(self) -> None:
        outcome = await _runner(_raising(ValueError("boom"))).run(_delivery(attempt=1, deferrals=9))
        assert isinstance(outcome, Failed)

    async def test_a_job_rejection_rejects_with_its_reason(self) -> None:
        signal = JobRejection("no such tenant")
        assert await _runner(_raising(signal)).run(_delivery()) == Rejected(
            "no such tenant", signal
        )

    async def test_a_run_past_its_timeout_fails(self) -> None:
        async def hang(delivery: JobDelivery) -> None:
            await asyncio.sleep(5)

        outcome = await _runner(hang).run(_delivery(timeout_s=0.01))
        assert isinstance(outcome, Failed)
        assert isinstance(outcome.error, TimeoutError)

    async def test_a_cancellation_is_not_an_outcome(self) -> None:
        with pytest.raises(asyncio.CancelledError):
            await _runner(_raising(asyncio.CancelledError())).run(_delivery())


class TestJobRunnerDelivery:
    async def test_the_job_reads_its_delivery_while_it_runs(self) -> None:
        seen: list[JobDelivery | None] = []

        async def behave(delivery: JobDelivery) -> None:
            seen.append(JobDelivery.current())

        delivery = _delivery(attempt=2, deferrals=1)
        await _runner(behave).run(delivery)

        assert seen == [delivery]
        assert JobDelivery.current() is None

    async def test_the_delivery_is_released_when_the_job_fails(self) -> None:
        await _runner(_raising(ValueError("boom"))).run(_delivery())
        assert JobDelivery.current() is None


class TestJobRunnerEvents:
    @pytest.mark.parametrize(
        ("behaviour", "kind", "status"),
        [
            (_returning(1), EventKind.JOB_SUCCEEDED, Status.SUCCESS),
            (_raising(JobRetry("later", 1)), EventKind.JOB_DEFERRED, Status.DEFERRED),
            (_raising(ValueError("boom")), EventKind.JOB_FAILED, Status.FAILURE),
            (_raising(JobRejection("no")), EventKind.JOB_REJECTED, Status.REJECTED),
        ],
        ids=["succeeded", "deferred", "failed", "rejected"],
    )
    async def test_emits_started_then_the_event_of_the_outcome(
        self, behaviour: Any, kind: EventKind, status: Status
    ) -> None:
        metrics = Metrics()
        await _runner(behaviour, metrics).run(_delivery())

        started, settled = metrics.events
        assert started.kind is EventKind.JOB_STARTED
        assert (settled.kind, settled.status) == (kind, status)
        assert settled.use_case_name == "AcmeJob"
        assert settled.trace_id == "trace-1"
        assert settled.duration_ms is not None

    async def test_a_failed_event_carries_the_error(self) -> None:
        metrics = Metrics()
        error = ValueError("boom")
        await _runner(_raising(error), metrics).run(_delivery())
        assert metrics.events[-1].error is error

    async def test_a_runner_without_metrics_runs(self) -> None:
        outcome: JobOutcome = await _runner(_returning(1)).run(_delivery())
        assert outcome == Succeeded(1)
