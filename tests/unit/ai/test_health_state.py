from __future__ import annotations

import pytest

from loom.ai.abc import HealthState


class TestHealthStateWorst:
    @pytest.mark.parametrize(
        ("states", "worst"),
        [
            ((), HealthState.OK),
            ((HealthState.OK, HealthState.OK), HealthState.OK),
            ((HealthState.OK, HealthState.DEGRADED), HealthState.DEGRADED),
            (
                (HealthState.DEGRADED, HealthState.UNAVAILABLE, HealthState.OK),
                HealthState.UNAVAILABLE,
            ),
        ],
        ids=["none", "all-ok", "one-degraded", "one-unavailable"],
    )
    def test_is_the_worst_of_the_states(
        self, states: tuple[HealthState, ...], worst: HealthState
    ) -> None:
        assert HealthState.worst(states) is worst
