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
from loom.core.job.policy import Backoff, Redelivery, RetryPolicy
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


def _runner(
    behaviour: Any, metrics: Metrics | None = None, redelivery: Redelivery = Redelivery.DELAYED
) -> JobRunner:
    async def invocation(delivery: JobDelivery) -> Any:
        return await behaviour(delivery)

    return JobRunner(invocation, redelivery=redelivery, metrics=metrics)


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
    async def test_the_invocation_receives_the_delivery_it_was_run_with(self) -> None:
        seen: list[JobDelivery] = []

        async def behave(delivery: JobDelivery) -> None:
            seen.append(delivery)

        delivery = _delivery(attempt=2, deferrals=1)
        await _runner(behave).run(delivery)

        assert seen == [delivery]


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


class TestJobRunnerRedelivery:
    async def test_a_transport_that_cannot_wait_rejects_a_job_retry_naming_why(self) -> None:
        signal = JobRetry("later", 30)
        for redelivery in (Redelivery.NONE, Redelivery.SAME):
            outcome = await _runner(_raising(signal), redelivery=redelivery).run(_delivery())
            assert outcome == Rejected("cannot wait: later", signal)

    async def test_a_transport_that_retries_nothing_rejects_a_failure_at_the_first_attempt(
        self,
    ) -> None:
        error = ValueError("boom")
        outcome = await _runner(_raising(error), redelivery=Redelivery.NONE).run(_delivery())
        assert outcome == Rejected("ValueError", error)

    async def test_a_transport_that_brings_the_same_message_back_retries_a_failure(self) -> None:
        outcome = await _runner(_raising(ValueError("boom")), redelivery=Redelivery.SAME).run(
            _delivery(attempt=2)
        )
        assert isinstance(outcome, Failed)
        assert outcome.delay.seconds == 8

    async def test_a_transport_that_brings_the_same_message_back_still_ends_on_the_last_attempt(
        self,
    ) -> None:
        outcome = await _runner(_raising(ValueError("boom")), redelivery=Redelivery.SAME).run(
            _delivery(attempt=3)
        )
        assert isinstance(outcome, Rejected)

    @pytest.mark.parametrize("redelivery", list(Redelivery))
    async def test_a_job_rejection_rejects_whatever_the_transport_redelivers(
        self, redelivery: Redelivery
    ) -> None:
        signal = JobRejection("no such tenant")
        outcome = await _runner(_raising(signal), redelivery=redelivery).run(_delivery())
        assert outcome == Rejected("no such tenant", signal)

    async def test_a_rejected_job_retry_emits_the_rejection_not_a_deferral(self) -> None:
        metrics = Metrics()
        await _runner(_raising(JobRetry("later", 30)), metrics, redelivery=Redelivery.NONE).run(
            _delivery()
        )

        assert [event.kind for event in metrics.events] == [
            EventKind.JOB_STARTED,
            EventKind.JOB_REJECTED,
        ]


class TestJobRunnerUnrunnable:
    def test_a_message_that_cannot_run_fails_while_attempts_are_left(self) -> None:
        error = ValueError("garbled")
        outcome = _runner(_returning(1)).unrunnable(error, 1, POLICY)
        assert isinstance(outcome, Failed)
        assert outcome.error is error
        assert outcome.delay.seconds == 4

    def test_a_message_that_cannot_run_is_rejected_on_the_last_attempt(self) -> None:
        error = ValueError("garbled")
        assert _runner(_returning(1)).unrunnable(error, 3, POLICY) == Rejected("ValueError", error)

    def test_a_transport_that_retries_nothing_rejects_it_at_once(self) -> None:
        error = ValueError("garbled")
        runner = _runner(_returning(1), redelivery=Redelivery.NONE)
        assert runner.unrunnable(error, 1, POLICY) == Rejected("ValueError", error)

    def test_it_emits_the_event_of_the_outcome_named_by_the_error_class(self) -> None:
        metrics = Metrics()
        error = UnretryableError("never")
        _runner(_returning(1), metrics).unrunnable(error, 1, POLICY)

        [event] = metrics.events
        assert (event.kind, event.status, event.use_case_name, event.error) == (
            EventKind.JOB_REJECTED,
            Status.REJECTED,
            "UnretryableError",
            error,
        )


class TestRedelivery:
    @pytest.mark.parametrize(
        ("redelivery", "defers", "retries"),
        [
            (Redelivery.DELAYED, True, True),
            (Redelivery.SAME, False, True),
            (Redelivery.NONE, False, False),
        ],
    )
    def test_what_each_transport_can_do(
        self, redelivery: Redelivery, defers: bool, retries: bool
    ) -> None:
        assert (redelivery.defers, redelivery.retries) == (defers, retries)
