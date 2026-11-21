"""One job, one signal, the same settlement on every backend."""

from __future__ import annotations

from typing import ClassVar

from loom.core.errors import SystemError
from loom.core.job.delivery import JobDelivery
from loom.core.job.errors import JobAbandonment, JobPostponement
from loom.core.job.job import Job
from loom.core.job.policy import Backoff
from loom.core.use_case import Delivery
from tests.contract.jobs.conftest import Driver, Settlement

STEADY = Backoff(first_s=4, factor=2, max_s=100, jitter=False)


class AcmeError(Exception):
    pass


class UnretryableAcmeError(SystemError):
    is_retryable = False


class SucceedingJob(Job[str]):
    async def execute(self) -> str:
        return "done"


class FailingJob(Job[None]):
    __retry__ = STEADY

    async def execute(self) -> None:
        raise AcmeError("boom")


class UnretryableJob(Job[None]):
    async def execute(self) -> None:
        raise UnretryableAcmeError("never")


class RetryingJob(Job[None]):
    __retry__ = STEADY

    async def execute(self) -> None:
        raise JobPostponement(reason="tenant warming up", delay_s=30)


class StepRetryingJob(Job[None]):
    __retry__ = STEADY

    async def execute(self) -> None:
        raise JobPostponement(reason="tenant warming up")


class AbandoningJob(Job[None]):
    async def execute(self) -> None:
        raise JobAbandonment(reason="tenant deleted")


class RecordingJob(Job[None]):
    seen: ClassVar[list[tuple[int, int]]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements))


class TestJobOutcomeContract:
    async def test_success(self, driver: Driver) -> None:
        assert await driver.deliver(SucceedingJob) == Settlement("succeeded", result="done")

    async def test_a_failure_with_attempts_left_waits_the_policy_backoff(
        self, driver: Driver
    ) -> None:
        settled = await driver.deliver(FailingJob, attempt=2)
        assert settled == Settlement("failed", delay_s=8, reason="AcmeError")

    async def test_the_last_attempt_is_abandoned_with_the_error_class(self, driver: Driver) -> None:
        settled = await driver.deliver(FailingJob, attempt=3)
        assert settled == Settlement("abandoned", reason="AcmeError")

    async def test_an_error_that_cannot_succeed_again_is_abandoned_at_once(
        self, driver: Driver
    ) -> None:
        settled = await driver.deliver(UnretryableJob)
        assert settled == Settlement("abandoned", reason="UnretryableAcmeError")

    async def test_a_job_postponement_postpones_by_its_own_delay(self, driver: Driver) -> None:
        settled = await driver.deliver(RetryingJob)
        assert settled == Settlement("postponed", delay_s=30, reason="tenant warming up")

    async def test_a_job_postponement_without_a_delay_postpones_by_the_next_backoff_step(
        self, driver: Driver
    ) -> None:
        settled = await driver.deliver(StepRetryingJob, postponements=2)
        assert settled == Settlement("postponed", delay_s=16, reason="tenant warming up")

    async def test_a_job_postponement_is_postponed_even_on_the_last_attempt(
        self, driver: Driver
    ) -> None:
        settled = await driver.deliver(RetryingJob, attempt=3, postponements=7)
        assert settled.kind == "postponed"

    async def test_postponements_do_not_use_up_the_attempts_of_a_later_failure(
        self, driver: Driver
    ) -> None:
        settled = await driver.deliver(FailingJob, attempt=1, postponements=9)
        assert settled.kind == "failed"

    async def test_a_job_abandonment_is_abandoned_with_its_reason(self, driver: Driver) -> None:
        settled = await driver.deliver(AbandoningJob)
        assert settled == Settlement("abandoned", reason="tenant deleted")

    async def test_the_job_sees_its_attempt_and_its_postponements(self, driver: Driver) -> None:
        RecordingJob.seen.clear()
        await driver.deliver(RecordingJob, attempt=2, postponements=3)
        assert RecordingJob.seen == [(2, 3)]
