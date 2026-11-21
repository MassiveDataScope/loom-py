from __future__ import annotations

import pytest

from loom.core.engine.events import AbandonReason
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
        assert Abandoned(AbandonReason.NOT_RETRYABLE, error).as_exception() is error

    def test_an_abandonment_without_an_error_is_a_job_abandonment_with_its_reason(self) -> None:
        failure = Abandoned(AbandonReason.JOB_ASKED).as_exception()
        assert isinstance(failure, JobAbandonment)
        assert str(failure) == "job_asked"

    @pytest.mark.parametrize(
        ("abandoned", "statement"),
        [
            (
                Abandoned(AbandonReason.JOB_ASKED, JobAbandonment("tenant deleted")),
                "tenant deleted",
            ),
            (
                Abandoned(AbandonReason.CANNOT_WAIT, JobPostponement("warming up")),
                "cannot wait: warming up",
            ),
            (Abandoned(AbandonReason.ATTEMPTS_EXHAUSTED, ValueError("boom")), "ValueError"),
        ],
        ids=["job asked", "cannot wait", "error class"],
    )
    def test_an_abandonment_states_the_words_of_the_job_or_the_class_of_its_error(
        self, abandoned: Abandoned, statement: str
    ) -> None:
        assert abandoned.statement == statement

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
