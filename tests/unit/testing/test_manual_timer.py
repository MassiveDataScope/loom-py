"""``ManualTimer`` runs a step only when the test moves the clock past its time."""

from __future__ import annotations

import pytest

from loom.core.job.policy import Countdown
from loom.core.job.timer import Step
from loom.testing import ManualTimer

pytestmark = pytest.mark.timeout(10)


class TestManualTimer:
    async def test_a_step_waits_until_the_clock_reaches_its_time(self) -> None:
        timer = ManualTimer()
        ran: list[str] = []

        async def step() -> None:
            ran.append("a")

        timer.schedule(Countdown(60), step, job="AcmeJob", task_id="t")
        await timer.advance(59)
        assert (ran, timer.pending) == ([], 1)
        await timer.advance(1)

        assert (ran, timer.pending) == (["a"], 0)

    async def test_steps_run_in_the_order_of_their_time_then_of_scheduling(self) -> None:
        timer = ManualTimer()
        ran: list[str] = []

        def step(name: str) -> Step:
            async def run() -> None:
                ran.append(name)

            return run

        timer.schedule(Countdown(30), step("late"), job="J", task_id="t")
        timer.schedule(Countdown(10), step("first"), job="J", task_id="t")
        timer.schedule(Countdown(10), step("second"), job="J", task_id="t")
        await timer.advance(60)

        assert ran == ["first", "second", "late"]

    async def test_a_step_scheduled_by_a_step_runs_when_its_time_falls_within_the_move(
        self,
    ) -> None:
        timer = ManualTimer()
        ran: list[int] = []

        async def again() -> None:
            ran.append(2)

        async def first() -> None:
            ran.append(1)
            timer.schedule(Countdown(20), again, job="J", task_id="t")

        timer.schedule(Countdown(10), first, job="J", task_id="t")
        await timer.advance(29)
        assert ran == [1]
        await timer.advance(1)

        assert ran == [1, 2]

    async def test_the_clock_keeps_what_was_advanced(self) -> None:
        timer = ManualTimer()
        ran: list[int] = []

        async def step() -> None:
            ran.append(1)

        await timer.advance(50)
        timer.schedule(Countdown(10), step, job="J", task_id="t")
        await timer.advance(9)
        assert ran == []
        await timer.advance(1)

        assert ran == [1]
