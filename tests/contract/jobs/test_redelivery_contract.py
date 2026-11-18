"""What each kind of redelivery does with a job that must run again, on every transport that has it.

``Redelivery.DELAYED`` is the rest of the battery (``test_job_outcome_contract``):
a transport that waits defers a job retry and retries a failure.
"""

from __future__ import annotations

import pytest

from tests.contract.jobs.conftest import UNDELAYED_BACKENDS, Driver, Settlement
from tests.contract.jobs.test_job_outcome_contract import (
    FailingJob,
    RejectingJob,
    RetryingJob,
    StepRetryingJob,
    SucceedingJob,
)


class TestNoRedelivery:
    async def test_success(self, no_redelivery_driver: Driver) -> None:
        settled = await no_redelivery_driver.deliver(SucceedingJob)
        assert settled == Settlement("succeeded", result="done")

    @pytest.mark.parametrize("job", [RetryingJob, StepRetryingJob])
    async def test_a_job_retry_is_rejected_saying_it_cannot_wait(
        self, no_redelivery_driver: Driver, job: type[RetryingJob | StepRetryingJob]
    ) -> None:
        settled = await no_redelivery_driver.deliver(job)
        assert settled == Settlement("rejected", reason="cannot wait: tenant warming up")

    async def test_a_failure_is_rejected_at_the_first_attempt_with_the_error_class(
        self, no_redelivery_driver: Driver
    ) -> None:
        settled = await no_redelivery_driver.deliver(FailingJob, attempt=1)
        assert settled == Settlement("rejected", reason="AcmeError")

    async def test_a_job_rejection_is_rejected_with_its_reason(
        self, no_redelivery_driver: Driver
    ) -> None:
        settled = await no_redelivery_driver.deliver(RejectingJob)
        assert settled == Settlement("rejected", reason="tenant deleted")


@pytest.mark.parametrize("same_message_driver", UNDELAYED_BACKENDS, indirect=True)
class TestSameMessageRedelivery:
    @pytest.mark.parametrize("job", [RetryingJob, StepRetryingJob])
    async def test_a_job_retry_is_rejected_saying_it_cannot_wait(
        self, same_message_driver: Driver, job: type[RetryingJob | StepRetryingJob]
    ) -> None:
        settled = await same_message_driver.deliver(job)
        assert settled == Settlement("rejected", reason="cannot wait: tenant warming up")

    async def test_a_failure_with_attempts_left_waits_the_policy_backoff(
        self, same_message_driver: Driver
    ) -> None:
        settled = await same_message_driver.deliver(FailingJob, attempt=2)
        assert settled == Settlement("failed", delay_s=8, reason="AcmeError")

    async def test_the_last_attempt_is_rejected_with_the_error_class(
        self, same_message_driver: Driver
    ) -> None:
        settled = await same_message_driver.deliver(FailingJob, attempt=3)
        assert settled == Settlement("rejected", reason="AcmeError")
