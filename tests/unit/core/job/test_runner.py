from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.core.engine.events import EventKind, RuntimeEvent, Status
from loom.core.errors import SystemError
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import (
    Abandoned,
    Failed,
    JobAbandonment,
    JobOutcome,
    JobPostponement,
    Postponed,
    Succeeded,
)
from loom.core.job.policy import Backoff, Redelivery, RetryPolicy
from loom.core.job.runner import JobRunner
from loom.testing import JobTestData

POLICY = RetryPolicy(Backoff(first_s=4, factor=2, max_s=100, jitter=False), max_attempts=3)


class AcmeJob(Job[None]):
    async def execute(self) -> None:
        return None


class Metrics:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    def on_event(self, event: RuntimeEvent) -> None:
        self.events.append(event)


class UnretryableError(SystemError):
    is_retryable = False


_DATA = JobTestData().with_job_type(AcmeJob).with_policy(POLICY).with_trace_id("trace-1")


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
        assert await _runner(_returning(7)).run(_DATA.delivery()) == Succeeded(7)

    async def test_a_failure_with_attempts_left_fails_with_the_backoff_delay(self) -> None:
        error = ValueError("boom")
        outcome = await _runner(_raising(error)).run(_DATA.delivery(attempt=2))
        assert isinstance(outcome, Failed)
        assert outcome.error is error
        assert outcome.delay.seconds == 8

    async def test_the_last_attempt_abandons_with_the_class_of_the_error(self) -> None:
        error = ValueError("boom")
        assert await _runner(_raising(error)).run(_DATA.delivery(attempt=3)) == Abandoned(
            "ValueError", error
        )

    async def test_an_error_that_cannot_succeed_again_abandons_at_the_first_attempt(self) -> None:
        error = UnretryableError("never")
        assert await _runner(_raising(error)).run(_DATA.delivery()) == Abandoned(
            "UnretryableError", error
        )

    async def test_a_job_postponement_postpones_with_its_delay(self) -> None:
        outcome = await _runner(_raising(JobPostponement("later", 30))).run(
            _DATA.delivery(attempt=3)
        )
        assert outcome == Postponed(outcome.delay, "later")
        assert isinstance(outcome, Postponed)
        assert outcome.delay.seconds == 30

    async def test_a_job_postponement_without_a_delay_takes_the_next_backoff_step(
        self,
    ) -> None:
        outcome = await _runner(_raising(JobPostponement("later"))).run(
            _DATA.delivery(postponements=2)
        )
        assert isinstance(outcome, Postponed)
        assert outcome.delay.seconds == 16

    async def test_a_job_postponement_never_exhausts_the_attempts(self) -> None:
        outcome = await _runner(_raising(JobPostponement("later", 1))).run(
            _DATA.delivery(attempt=3, postponements=9)
        )
        assert isinstance(outcome, Postponed)

    async def test_postponements_do_not_spend_attempts_of_a_later_failure(self) -> None:
        outcome = await _runner(_raising(ValueError("boom"))).run(
            _DATA.delivery(attempt=1, postponements=9)
        )
        assert isinstance(outcome, Failed)

    async def test_a_job_abandonment_abandons_with_its_reason(self) -> None:
        signal = JobAbandonment("no such tenant")
        assert await _runner(_raising(signal)).run(_DATA.delivery()) == Abandoned(
            "no such tenant", signal
        )

    async def test_a_run_past_its_timeout_fails(self) -> None:
        async def hang(delivery: JobDelivery) -> None:
            await asyncio.sleep(5)

        outcome = await _runner(hang).run(_DATA.delivery(timeout_s=0.01))
        assert isinstance(outcome, Failed)
        assert isinstance(outcome.error, TimeoutError)

    async def test_a_cancellation_is_not_an_outcome(self) -> None:
        with pytest.raises(asyncio.CancelledError):
            await _runner(_raising(asyncio.CancelledError())).run(_DATA.delivery())


class TestJobRunnerDelivery:
    async def test_the_invocation_receives_the_delivery_it_was_run_with(self) -> None:
        seen: list[JobDelivery] = []

        async def behave(delivery: JobDelivery) -> None:
            seen.append(delivery)

        delivery = _DATA.delivery(attempt=2, postponements=1)
        await _runner(behave).run(delivery)

        assert seen == [delivery]


class TestJobRunnerEvents:
    @pytest.mark.parametrize(
        ("behaviour", "kind", "status"),
        [
            (_returning(1), EventKind.JOB_SUCCEEDED, Status.SUCCESS),
            (_raising(JobPostponement("later", 1)), EventKind.JOB_POSTPONED, Status.POSTPONED),
            (_raising(ValueError("boom")), EventKind.JOB_FAILED, Status.FAILURE),
            (_raising(JobAbandonment("no")), EventKind.JOB_ABANDONED, Status.ABANDONED),
        ],
        ids=["succeeded", "postponed", "failed", "abandoned"],
    )
    async def test_emits_started_then_the_event_of_the_outcome(
        self, behaviour: Any, kind: EventKind, status: Status
    ) -> None:
        metrics = Metrics()
        await _runner(behaviour, metrics).run(_DATA.delivery())

        started, settled = metrics.events
        assert started.kind is EventKind.JOB_STARTED
        assert (settled.kind, settled.status) == (kind, status)
        assert settled.use_case_name == "AcmeJob"
        assert settled.trace_id == "trace-1"
        assert settled.duration_ms is not None

    async def test_a_failed_event_carries_the_error(self) -> None:
        metrics = Metrics()
        error = ValueError("boom")
        await _runner(_raising(error), metrics).run(_DATA.delivery())
        assert metrics.events[-1].error is error

    async def test_a_runner_without_metrics_runs(self) -> None:
        outcome: JobOutcome = await _runner(_returning(1)).run(_DATA.delivery())
        assert outcome == Succeeded(1)


class TestJobRunnerRedelivery:
    async def test_a_transport_that_cannot_wait_abandons_a_job_postponement_naming_why(
        self,
    ) -> None:
        signal = JobPostponement("later", 30)
        for redelivery in (Redelivery.NONE, Redelivery.SAME):
            outcome = await _runner(_raising(signal), redelivery=redelivery).run(_DATA.delivery())
            assert outcome == Abandoned("cannot wait: later", signal)

    async def test_a_transport_that_retries_nothing_abandons_a_failure_at_the_first_attempt(
        self,
    ) -> None:
        error = ValueError("boom")
        outcome = await _runner(_raising(error), redelivery=Redelivery.NONE).run(_DATA.delivery())
        assert outcome == Abandoned("ValueError", error)

    async def test_a_transport_that_brings_the_same_message_back_retries_a_failure(self) -> None:
        outcome = await _runner(_raising(ValueError("boom")), redelivery=Redelivery.SAME).run(
            _DATA.delivery(attempt=2)
        )
        assert isinstance(outcome, Failed)
        assert outcome.delay.seconds == 8

    async def test_a_transport_that_brings_the_same_message_back_still_ends_on_the_last_attempt(
        self,
    ) -> None:
        outcome = await _runner(_raising(ValueError("boom")), redelivery=Redelivery.SAME).run(
            _DATA.delivery(attempt=3)
        )
        assert isinstance(outcome, Abandoned)

    @pytest.mark.parametrize("redelivery", list(Redelivery))
    async def test_a_job_abandonment_abandons_whatever_the_transport_redelivers(
        self, redelivery: Redelivery
    ) -> None:
        signal = JobAbandonment("no such tenant")
        outcome = await _runner(_raising(signal), redelivery=redelivery).run(_DATA.delivery())
        assert outcome == Abandoned("no such tenant", signal)

    async def test_an_abandoned_job_postponement_emits_the_abandonment_not_a_postponement(
        self,
    ) -> None:
        metrics = Metrics()
        await _runner(
            _raising(JobPostponement("later", 30)), metrics, redelivery=Redelivery.NONE
        ).run(_DATA.delivery())

        assert [event.kind for event in metrics.events] == [
            EventKind.JOB_STARTED,
            EventKind.JOB_ABANDONED,
        ]


class TestJobRunnerDecideUnrunnable:
    def test_a_message_that_cannot_run_fails_while_attempts_are_left(self) -> None:
        error = ValueError("garbled")
        outcome = _runner(_returning(1)).decide_unrunnable(error, 1, POLICY)
        assert isinstance(outcome, Failed)
        assert outcome.error is error
        assert outcome.delay.seconds == 4

    def test_a_message_that_cannot_run_is_abandoned_on_the_last_attempt(self) -> None:
        error = ValueError("garbled")
        assert _runner(_returning(1)).decide_unrunnable(error, 3, POLICY) == Abandoned(
            "ValueError", error
        )

    def test_a_transport_that_retries_nothing_abandons_it_at_once(self) -> None:
        error = ValueError("garbled")
        runner = _runner(_returning(1), redelivery=Redelivery.NONE)
        assert runner.decide_unrunnable(error, 1, POLICY) == Abandoned("ValueError", error)

    def test_it_emits_the_event_of_the_outcome_named_by_the_error_class(self) -> None:
        metrics = Metrics()
        error = UnretryableError("never")
        _runner(_returning(1), metrics).decide_unrunnable(error, 1, POLICY)

        [event] = metrics.events
        assert (event.kind, event.status, event.use_case_name, event.error) == (
            EventKind.JOB_ABANDONED,
            Status.ABANDONED,
            "UnretryableError",
            error,
        )


class TestRedelivery:
    @pytest.mark.parametrize(
        ("redelivery", "postpones", "retries"),
        [
            (Redelivery.DELAYED, True, True),
            (Redelivery.SAME, False, True),
            (Redelivery.NONE, False, False),
        ],
    )
    def test_what_each_transport_can_do(
        self, redelivery: Redelivery, postpones: bool, retries: bool
    ) -> None:
        assert (redelivery.can_postpone, redelivery.can_retry) == (postpones, retries)
