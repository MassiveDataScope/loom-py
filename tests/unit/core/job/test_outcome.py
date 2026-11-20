from __future__ import annotations

import pytest

from loom.core.job.outcome import (
    Abandoned,
    Failed,
    JobAbandonment,
    JobOutcome,
    JobPostponement,
    Postponed,
    Succeeded,
)
from loom.core.job.policy import Countdown


class TestOutcomeAsException:
    def test_a_failed_attempt_is_its_error(self) -> None:
        error = ValueError("boom")
        assert Failed(error, Countdown(3)).as_exception() is error

    def test_an_abandonment_is_its_error_when_it_has_one(self) -> None:
        error = ValueError("boom")
        assert Abandoned("ValueError", error).as_exception() is error

    def test_an_abandonment_without_an_error_is_a_job_abandonment_with_its_reason(self) -> None:
        failure = Abandoned("tenant deleted").as_exception()
        assert isinstance(failure, JobAbandonment)
        assert str(failure) == "tenant deleted"

    @pytest.mark.parametrize(
        "outcome",
        [Succeeded(1), Postponed(Countdown(30), "warming up")],
        ids=["succeeded", "postponed"],
    )
    def test_an_outcome_that_ended_nothing_has_no_exception(self, outcome: JobOutcome) -> None:
        assert not hasattr(outcome, "as_exception")


class TestJobPostponement:
    def test_a_delay_above_the_countdown_bound_is_refused(self) -> None:
        with pytest.raises(ValueError, match="0 to 900"):
            JobPostponement("later", 901)
