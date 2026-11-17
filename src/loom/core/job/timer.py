"""Waiting for a delay: the port inline jobs wait on, and the one that waits in process."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol

from loom.core.job.policy import Countdown
from loom.core.logger import get_logger

_log = get_logger(__name__)

Step = Callable[[], Awaitable[None]]
"""What runs when a delay is over."""


class JobDelayRefused(RuntimeError):
    """Raised when a job is delayed where no timer waits for it.

    Attributes:
        delay: The delay that was refused.
    """

    def __init__(self, delay: Countdown) -> None:
        super().__init__(
            f"cannot wait {delay.seconds}s: this job service has no timer, so a countdown, "
            "a JobRetry or a failure backoff cannot be honoured; use Celery or SQS"
        )
        self.delay = delay


class JobTimer(Protocol):
    """Runs a step once a delay is over; the inline job service waits on it."""

    def schedule(self, delay: Countdown, step: Step) -> None:
        """Run *step* after *delay*; return at once.

        Args:
            delay: How long to wait.
            step: What to run afterwards.
        """
        ...


class AsyncioTimer:
    """A :class:`JobTimer` that waits on the running event loop and keeps its steps in memory.

    It owns the tasks it starts. Closing it cancels every step still pending
    and logs how many jobs were lost, in one line.
    """

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task[None]] = set()

    def schedule(self, delay: Countdown, step: Step) -> None:
        """Run *step* on the running loop after *delay*.

        Args:
            delay: How long to wait.
            step: What to run afterwards.
        """
        task = asyncio.get_running_loop().create_task(self._after(delay, step))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @staticmethod
    async def _after(delay: Countdown, step: Step) -> None:
        await asyncio.sleep(delay.seconds)
        try:
            await step()
        except Exception:
            _log.exception("Delayed job step failed")

    async def close(self) -> None:
        """Cancel every pending step and log the jobs lost with them."""
        lost = tuple(self._tasks)
        for task in lost:
            task.cancel()
        await asyncio.gather(*lost, return_exceptions=True)
        if lost:
            _log.warning("Delayed jobs lost at shutdown", jobs=len(lost))
