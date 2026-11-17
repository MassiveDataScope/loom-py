from __future__ import annotations

import pytest

from loom.core.job.outcome import (
    Deferred,
    Failed,
    JobRejection,
    JobRetry,
    Rejected,
    Succeeded,
)
from loom.core.job.policy import Countdown


class TestOutcomeFailure:
    def test_a_success_has_no_failure(self) -> None:
        assert Succeeded(1).failure() is None

    def test_a_failed_attempt_is_its_error(self) -> None:
        error = ValueError("boom")
        assert Failed(error, Countdown(3)).failure() is error

    def test_a_rejection_is_its_error_when_it_has_one(self) -> None:
        error = ValueError("boom")
        assert Rejected("ValueError", error).failure() is error

    def test_a_rejection_without_an_error_is_a_job_rejection_with_its_reason(self) -> None:
        failure = Rejected("tenant deleted").failure()
        assert isinstance(failure, JobRejection)
        assert str(failure) == "tenant deleted"

    def test_a_deferral_is_a_job_retry_with_its_delay(self) -> None:
        failure = Deferred(Countdown(30), "warming up").failure()
        assert isinstance(failure, JobRetry)
        assert (failure.reason, failure.delay) == ("warming up", Countdown(30))


class TestJobRetry:
    def test_a_delay_above_the_countdown_bound_is_refused(self) -> None:
        with pytest.raises(ValueError, match="0 to 900"):
            JobRetry("later", 901)
