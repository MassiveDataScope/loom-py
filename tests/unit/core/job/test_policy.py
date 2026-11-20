from __future__ import annotations

import pytest

from loom.core.job.delivery import JobDelivery
from loom.core.job.policy import DEFAULT_RETRY, Backoff, Countdown, RetryPolicy
from loom.testing import make


class TestCountdown:
    @pytest.mark.parametrize("seconds", [0, 1, 900])
    def test_accepts_the_bounds(self, seconds: int) -> None:
        assert Countdown(seconds).seconds == seconds

    @pytest.mark.parametrize("seconds", [-1, 901])
    def test_refuses_outside_the_bounds(self, seconds: int) -> None:
        with pytest.raises(ValueError, match="0 to 900"):
            Countdown(seconds)


class TestBackoff:
    def test_jitters_by_default(self) -> None:
        assert Backoff(first_s=5, factor=2, max_s=300).jitter is True

    @pytest.mark.parametrize(
        ("attempt", "expected"),
        [(1, 5), (2, 10), (3, 20), (7, 300), (500, 300)],
    )
    def test_grows_by_the_factor_up_to_the_cap_without_jitter(
        self, attempt: int, expected: int
    ) -> None:
        backoff = Backoff(first_s=5, factor=2, max_s=300, jitter=False)
        assert backoff.delay(attempt).seconds == expected

    def test_jitter_draws_from_zero_to_the_ceiling(self) -> None:
        backoff = Backoff(first_s=5, factor=2, max_s=300)
        draws = {backoff.delay(3).seconds for _ in range(400)}
        assert max(draws) <= 20
        assert len(draws) > 5

    @pytest.mark.parametrize(
        ("factor", "attempt"),
        [(1e5, 70), (2.0, 5000), (1e300, 3)],
    )
    def test_a_huge_growth_saturates_at_the_cap(self, factor: float, attempt: int) -> None:
        backoff = Backoff(first_s=5, factor=factor, max_s=300, jitter=False)
        assert backoff.delay(attempt).seconds == 300

    def test_a_cap_above_the_countdown_bound_is_refused(self) -> None:
        with pytest.raises(ValueError, match="max_s must be 0 to 900"):
            Backoff(first_s=5, factor=2, max_s=901)

    @pytest.mark.parametrize(
        ("first_s", "factor"),
        [(-1, 2.0), (5, 0.5)],
    )
    def test_refuses_a_negative_start_and_a_shrinking_factor(
        self, first_s: int, factor: float
    ) -> None:
        with pytest.raises(ValueError, match="first_s|factor"):
            Backoff(first_s=first_s, factor=factor, max_s=300)


class TestRetryPolicy:
    @pytest.mark.parametrize("max_attempts", [0, -1])
    def test_refuses_fewer_than_one_attempt(self, max_attempts: int) -> None:
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            RetryPolicy(Backoff(1, 1, 1), max_attempts)

    def test_the_loom_default(self) -> None:
        assert RetryPolicy(Backoff(5, 2, 300, jitter=True), max_attempts=3) == DEFAULT_RETRY

    @pytest.mark.parametrize(
        ("max_attempts", "attempt", "allowed"),
        [(3, 1, True), (3, 2, True), (3, 3, False), (None, 99, True)],
    )
    def test_allows_another_attempt_until_the_limit(
        self, max_attempts: int | None, attempt: int, allowed: bool
    ) -> None:
        policy = RetryPolicy(Backoff(1, 1, 1), max_attempts)
        assert policy.can_retry_after(attempt) is allowed


class TestJobDelivery:
    @pytest.mark.parametrize("attempt", [0, -1])
    def test_refuses_an_attempt_before_the_first(self, attempt: int) -> None:
        with pytest.raises(ValueError, match="attempt must be at least 1"):
            make(JobDelivery, attempt=attempt)

    def test_refuses_negative_postponements(self) -> None:
        with pytest.raises(ValueError, match="postponements cannot be negative"):
            make(JobDelivery, postponements=-1)

    def test_accepts_the_first_attempt_with_no_postponements(self) -> None:
        delivery = make(JobDelivery, attempt=1, postponements=0)

        assert (delivery.attempt, delivery.postponements) == (1, 0)
