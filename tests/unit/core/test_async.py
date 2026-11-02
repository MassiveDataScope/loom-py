"""``run_to_completion``: a step finishes whatever happens to the task awaiting it."""

from __future__ import annotations

import asyncio

import pytest

from loom.core._async import run_to_completion


async def test_returns_what_the_step_returned() -> None:
    async def step() -> str:
        return "done"

    assert await run_to_completion(step()) == "done"


async def test_raises_what_the_step_raised() -> None:
    async def step() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await run_to_completion(step())


async def test_waits_through_repeated_cancellations_and_raises_the_first() -> None:
    finished: list[bool] = []
    gate = asyncio.Event()

    async def step() -> None:
        await gate.wait()
        finished.append(True)

    caller = asyncio.create_task(run_to_completion(step()))
    await asyncio.sleep(0)
    caller.cancel("first")
    await asyncio.sleep(0)
    caller.cancel("second")
    await asyncio.sleep(0)
    gate.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await caller

    assert finished == [True]
    assert raised.value.args == ("first",)


async def test_a_step_that_ends_cancelled_still_raises_the_callers_cancellation() -> None:
    gate = asyncio.Event()

    async def step() -> None:
        await gate.wait()
        raise asyncio.CancelledError("step's own")

    caller = asyncio.create_task(run_to_completion(step()))
    await asyncio.sleep(0)
    caller.cancel("caller's")
    await asyncio.sleep(0)
    gate.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await caller

    assert raised.value.args == ("caller's",)
