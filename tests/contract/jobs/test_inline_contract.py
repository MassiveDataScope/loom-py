"""The jobs of the outcome contract, run by the real inline service on a manual timer."""

from __future__ import annotations

from typing import ClassVar

import pytest

from loom.core.errors import JobFailedError
from loom.core.job.delivery import JobDelivery
from loom.core.job.errors import JobAbandonment, JobPostponement
from loom.core.job.job import Job
from loom.core.use_case import Delivery
from tests.contract.jobs.conftest import Inline
from tests.contract.jobs.test_job_outcome_contract import (
    STEADY,
    AbandoningJob,
    AcmeError,
    FailingJob,
    RetryingJob,
    SucceedingJob,
    UnretryableAcmeError,
    UnretryableJob,
)


class SeenFailingJob(Job[None]):
    __retry__ = STEADY
    seen: ClassVar[list[tuple[int, int]]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements))
        raise AcmeError("boom")


class SeenRetryingJob(Job[None]):
    __retry__ = STEADY
    seen: ClassVar[list[tuple[int, int]]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements))
        raise JobPostponement(reason="tenant warming up")


class SeenOwnDelayJob(Job[None]):
    __retry__ = STEADY
    seen: ClassVar[list[tuple[int, int]]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements))
        raise JobPostponement(reason="tenant warming up", delay_s=30)


class PostponingThenFailingJob(Job[None]):
    __retry__ = STEADY
    seen: ClassVar[list[tuple[int, int]]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements))
        if delivery.postponements < 9:
            raise JobPostponement(reason="tenant warming up", delay_s=1)
        raise AcmeError("boom")


class FailingThenRetryingJob(Job[None]):
    __retry__ = STEADY
    seen: ClassVar[list[tuple[int, int]]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements))
        if delivery.attempt < 3:
            raise AcmeError("boom")
        raise JobPostponement(reason="tenant warming up", delay_s=5)


@pytest.fixture(autouse=True)
def _clear_seen() -> None:
    for job in (
        SeenFailingJob,
        SeenRetryingJob,
        SeenOwnDelayJob,
        PostponingThenFailingJob,
        FailingThenRetryingJob,
    ):
        job.seen.clear()


class TestInlineContract:
    async def test_success(self, inline: Inline) -> None:
        handle = await inline.service.dispatch_now(SucceedingJob)
        await inline.service.wait_until_idle()

        assert handle.wait() == "done"
        assert inline.timer.pending == 0

    async def test_a_failure_with_attempts_left_waits_the_policy_backoff(
        self, inline: Inline
    ) -> None:
        await inline.service.dispatch_now(SeenFailingJob)
        await inline.timer.advance(3)
        assert SeenFailingJob.seen == [(1, 0)]
        await inline.timer.advance(1)
        assert SeenFailingJob.seen == [(1, 0), (2, 0)]
        await inline.timer.advance(7)
        assert SeenFailingJob.seen == [(1, 0), (2, 0)]
        await inline.timer.advance(1)

        assert SeenFailingJob.seen == [(1, 0), (2, 0), (3, 0)]

    async def test_the_last_attempt_is_abandoned_with_the_error(self, inline: Inline) -> None:
        handle = await inline.service.dispatch_now(FailingJob)
        await inline.timer.advance(4 + 8)

        assert inline.timer.pending == 0
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert type(failed.value.cause) is AcmeError

    async def test_an_error_that_cannot_succeed_again_is_abandoned_at_once(
        self, inline: Inline
    ) -> None:
        handle = await inline.service.dispatch_now(UnretryableJob)
        await inline.service.wait_until_idle()

        assert inline.timer.pending == 0
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert type(failed.value.cause) is UnretryableAcmeError

    async def test_a_job_postponement_postpones_by_its_own_delay(self, inline: Inline) -> None:
        await inline.service.dispatch_now(SeenOwnDelayJob)
        await inline.timer.advance(29)
        assert SeenOwnDelayJob.seen == [(1, 0)]
        await inline.timer.advance(1)

        assert SeenOwnDelayJob.seen == [(1, 0), (1, 1)]

    async def test_a_job_postponement_without_a_delay_postpones_by_the_next_backoff_step(
        self, inline: Inline
    ) -> None:
        await inline.service.dispatch_now(SeenRetryingJob)
        await inline.timer.advance(3)
        assert SeenRetryingJob.seen == [(1, 0)]
        await inline.timer.advance(1)
        assert SeenRetryingJob.seen == [(1, 0), (1, 1)]
        await inline.timer.advance(7)
        assert SeenRetryingJob.seen == [(1, 0), (1, 1)]
        await inline.timer.advance(1)

        assert SeenRetryingJob.seen == [(1, 0), (1, 1), (1, 2)]

    async def test_a_job_postponement_never_ends_the_job_however_often_it_postpones(
        self, inline: Inline
    ) -> None:
        await inline.service.dispatch_now(RetryingJob)
        for _ in range(10):
            await inline.timer.advance(30)

        assert inline.timer.pending == 1

    async def test_postponements_do_not_use_up_the_attempts_of_a_later_failure(
        self, inline: Inline
    ) -> None:
        await inline.service.dispatch_now(PostponingThenFailingJob)
        await inline.timer.advance(9)
        await inline.timer.advance(4)

        assert PostponingThenFailingJob.seen[-2:] == [(1, 9), (2, 9)]
        assert inline.timer.pending == 1

    async def test_a_job_abandonment_is_abandoned_with_its_reason(self, inline: Inline) -> None:
        handle = await inline.service.dispatch_now(AbandoningJob)
        await inline.service.wait_until_idle()

        assert inline.timer.pending == 0
        with pytest.raises(JobFailedError) as failed:
            handle.wait()
        assert isinstance(failed.value.cause, JobAbandonment)
        assert failed.value.cause.reason == "tenant deleted"

    async def test_a_job_postponement_is_postponed_even_on_the_last_attempt(
        self, inline: Inline
    ) -> None:
        await inline.service.dispatch_now(FailingThenRetryingJob)
        await inline.timer.advance(4 + 8)
        assert FailingThenRetryingJob.seen == [(1, 0), (2, 0), (3, 0)]
        assert inline.timer.pending == 1
        await inline.timer.advance(4)
        assert len(FailingThenRetryingJob.seen) == 3
        await inline.timer.advance(1)

        assert FailingThenRetryingJob.seen[-1] == (3, 1)
