"""Running one step to completion whatever happens to the task awaiting it."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

_T = TypeVar("_T")

_held: set[asyncio.Task[Any]] = set()


async def run_to_completion(
    step: Coroutine[Any, Any, _T],
    *,
    timeout: float,  # NOSONAR S7483 - absorbs cancellation, so the bound lives inside
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
        except asyncio.CancelledError as exc:  # NOSONAR S7497 - re-raised once step ends
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
