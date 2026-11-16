"""Running a step to completion, and blocking work on an executor."""

from __future__ import annotations

import asyncio
import contextvars
import functools
from collections.abc import Callable, Coroutine
from concurrent.futures import Executor
from typing import Any, TypeVar

_T = TypeVar("_T")

_held: set[asyncio.Task[Any]] = set()


async def run_in_executor(
    executor: Executor | None, call: Callable[..., _T], /, *args: Any, **kwargs: Any
) -> _T:
    """Run a blocking *call* on *executor* with the caller's context variables visible.

    Args:
        executor: The executor that runs *call*; ``None`` uses the loop's default.
        call: The blocking callable.
        *args: Positional arguments for *call*.
        **kwargs: Keyword arguments for *call*.

    Returns:
        What *call* returns.
    """
    context = contextvars.copy_context()
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        executor, functools.partial(context.run, call, *args, **kwargs)
    )


async def run_to_completion(
    step: Coroutine[Any, Any, _T],
    *,
    timeout: float,  # NOSONAR S7483
) -> _T:
    """Run ``step`` in a task of its own until it finishes or ``timeout`` runs out.

    No cancellation ever abandons ``step``; only ``timeout`` bounds it, so a
    ``step`` stuck on a dead connection cannot leave the awaiting task
    uncancellable.

    Args:
        step: The coroutine that must not be cut short by a cancellation.
        timeout: Seconds ``step`` may run before it is cancelled.

    Returns:
        What ``step`` returned, when the caller was not cancelled.

    Raises:
        asyncio.CancelledError: The first cancellation the caller received,
            once ``step`` has finished; ``step``'s own outcome is discarded.
        TimeoutError: If ``step`` outlasted ``timeout``, when the caller was
            not cancelled.
        Exception: Whatever ``step`` raised, when the caller was not cancelled.
    """
    task = asyncio.create_task(asyncio.wait_for(step, timeout))
    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait([task])
        except asyncio.CancelledError as exc:  # NOSONAR S7497
            cancelled = cancelled or exc
    if cancelled is not None:
        discard_outcome(task)
        raise cancelled
    return task.result()


def held_task(step: Coroutine[Any, Any, _T]) -> asyncio.Task[_T]:
    """Run ``step`` in a task held until it ends: a cancelled shield lets go of it."""
    task = asyncio.create_task(step)
    _held.add(task)
    task.add_done_callback(_held.discard)
    return task


def discard_outcome(task: asyncio.Future[Any]) -> None:
    """Mark a finished task's outcome as retrieved, so it is never reported as lost."""
    if not task.cancelled():
        task.exception()
