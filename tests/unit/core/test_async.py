"""``run_to_completion``: a step finishes whatever happens to the task awaiting it."""

from __future__ import annotations

import asyncio

import pytest

from loom.core._async import run_to_completion

_LONG = 60.0
"""A timeout no step here reaches."""


async def test_returns_what_the_step_returned() -> None:
    async def step() -> str:
        return "done"

    assert await run_to_completion(step(), timeout=_LONG) == "done"


async def test_raises_what_the_step_raised() -> None:
    async def step() -> None:
        raise ValueError("boom")

    call = step()

    with pytest.raises(ValueError, match="boom"):
        await run_to_completion(call, timeout=_LONG)


async def test_a_step_outlasting_its_timeout_raises_timeout_error() -> None:
    """The step ends on its own a little later, so an unbounded run fails instead of hanging."""
    call = asyncio.sleep(0.5)

    with pytest.raises(TimeoutError):
        await run_to_completion(call, timeout=0.01)


async def test_waits_through_repeated_cancellations_and_raises_the_first() -> None:
    finished: list[bool] = []
    gate = asyncio.Event()

    async def step() -> None:
        await gate.wait()
        finished.append(True)

    caller = asyncio.create_task(run_to_completion(step(), timeout=_LONG))
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

    caller = asyncio.create_task(run_to_completion(step(), timeout=_LONG))
    await asyncio.sleep(0)
    caller.cancel("caller's")
    await asyncio.sleep(0)
    gate.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await caller

    assert raised.value.args == ("caller's",)
