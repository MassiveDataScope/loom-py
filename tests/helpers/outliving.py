"""A task that outlives the execution it was started in: the one harness of the T09a tests."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable

Opens = Callable[[Callable[[], Awaitable[None]]], Awaitable[object]]
"""Runs the body it is given inside an execution, and returns once that execution ended."""


async def outlive(
    opens: Opens,
    late: Callable[[], object],
    *,
    inside: Callable[[], object] = lambda: None,
    seen: list[object] | None = None,
) -> asyncio.Task[object]:
    """Start a task inside the execution *opens* runs, which calls *late* once it ended.

    *inside* runs in that execution first; what it returns is appended to
    *seen*. *late* may return an awaitable, which the task awaits.
    """
    release = asyncio.Event()
    survivors: list[asyncio.Task[object]] = []

    async def after_the_end() -> object:
        await release.wait()
        result = late()
        return await result if inspect.isawaitable(result) else result

    async def body() -> None:
        if seen is not None:
            seen.append(inside())
        survivors.append(asyncio.create_task(after_the_end()))

    await opens(body)
    release.set()
    return survivors[0]


__all__ = ["Opens", "outlive"]
