"""Countdown is the only owner of the 0..900 s dispatch bound."""

from __future__ import annotations

import pytest

from loom.core.job.policy import Countdown


class TestCountdown:
    @pytest.mark.parametrize("seconds", [0, 1, 900])
    def test_accepts_a_value_within_the_bound(self, seconds: int) -> None:
        assert Countdown(seconds).seconds == seconds

    @pytest.mark.parametrize("seconds", [-1, 901])
    def test_rejects_a_value_outside_the_bound(self, seconds: int) -> None:
        with pytest.raises(ValueError, match="900"):
            Countdown(seconds)
