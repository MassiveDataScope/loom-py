"""Running one step to completion whatever happens to the task awaiting it."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

_T = TypeVar("_T")


async def run_to_completion(step: Coroutine[Any, Any, _T]) -> _T:
    """Run ``step`` in a task of its own until it finishes, however often we are cancelled.

    No cancellation ever abandons ``step``: use it for steps that must finish
    (rollback, close), never for external calls that may hang.

    Args:
        step: The coroutine that must not be cut short.

    Returns:
        What ``step`` returned, when the caller was not cancelled.

    Raises:
        asyncio.CancelledError: The first cancellation the caller received,
            once ``step`` has finished; ``step``'s own outcome is discarded.
        Exception: Whatever ``step`` raised, when the caller was not cancelled.
    """
    task = asyncio.create_task(step)
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


def discard_outcome(task: asyncio.Future[Any]) -> None:
    """Mark a finished task's outcome as retrieved, so it is never reported as lost."""
    if not task.cancelled():
        task.exception()
