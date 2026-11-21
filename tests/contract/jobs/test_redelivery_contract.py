"""What each kind of redelivery does with a job that must run again, on every transport that has it.

``Redelivery.DELAYED`` is the rest of the battery (``test_job_outcome_contract``):
a transport that waits postpones a job and retries a failure.
"""

from __future__ import annotations

import pytest

from tests.contract.jobs.conftest import UNDELAYED_BACKENDS, Driver, Settlement
from tests.contract.jobs.test_job_outcome_contract import (
    AbandoningJob,
    FailingJob,
    RetryingJob,
    StepRetryingJob,
    SucceedingJob,
)


class TestNoRedelivery:
    async def test_success(self, no_redelivery_driver: Driver) -> None:
        settled = await no_redelivery_driver.deliver(SucceedingJob)
        assert settled == Settlement("succeeded", result="done")

    @pytest.mark.parametrize("job", [RetryingJob, StepRetryingJob])
    async def test_a_job_postponement_is_abandoned_saying_it_cannot_wait(
        self, no_redelivery_driver: Driver, job: type[RetryingJob | StepRetryingJob]
    ) -> None:
        settled = await no_redelivery_driver.deliver(job)
        assert settled == Settlement("abandoned", reason="cannot wait: tenant warming up")

    async def test_a_failure_is_abandoned_at_the_first_attempt_with_the_error_class(
        self, no_redelivery_driver: Driver
    ) -> None:
        settled = await no_redelivery_driver.deliver(FailingJob, attempt=1)
        assert settled == Settlement("abandoned", reason="AcmeError")

    async def test_a_job_abandonment_is_abandoned_with_its_reason(
        self, no_redelivery_driver: Driver
    ) -> None:
        settled = await no_redelivery_driver.deliver(AbandoningJob)
        assert settled == Settlement("abandoned", reason="tenant deleted")


@pytest.mark.parametrize("same_message_driver", UNDELAYED_BACKENDS, indirect=True)
class TestSameMessageRedelivery:
    @pytest.mark.parametrize("job", [RetryingJob, StepRetryingJob])
    async def test_a_job_postponement_is_abandoned_saying_it_cannot_wait(
        self, same_message_driver: Driver, job: type[RetryingJob | StepRetryingJob]
    ) -> None:
        settled = await same_message_driver.deliver(job)
        assert settled == Settlement("abandoned", reason="cannot wait: tenant warming up")

    async def test_a_failure_with_attempts_left_waits_the_policy_backoff(
        self, same_message_driver: Driver
    ) -> None:
        settled = await same_message_driver.deliver(FailingJob, attempt=2)
        assert settled == Settlement("failed", delay_s=8, reason="AcmeError")

    async def test_the_last_attempt_is_abandoned_with_the_error_class(
        self, same_message_driver: Driver
    ) -> None:
        settled = await same_message_driver.deliver(FailingJob, attempt=3)
        assert settled == Settlement("abandoned", reason="AcmeError")
