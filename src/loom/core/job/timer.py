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
        delay: The countdown that was refused; ``None`` for a ``JobRetry``, whose
            delay the job or its backoff chooses.

    Args:
        delay: The countdown that was refused, if any.
    """

    def __init__(self, delay: Countdown | None = None) -> None:
        waited = "wait" if delay is None else f"wait {delay.seconds}s"
        super().__init__(
            f"cannot {waited}: this job service has no timer, so a countdown "
            "or a JobRetry cannot be honoured; use Celery or SQS"
        )
        self.delay = delay


class JobTimer(Protocol):
    """Runs a step once a delay is over; the inline job service waits on it."""

    def schedule(self, delay: Countdown, step: Step, *, job: str, task_id: str) -> None:
        """Run *step* after *delay*; return at once.

        Args:
            delay: How long to wait.
            step: What to run afterwards.
            job: The qualified name of the job the step runs.
            task_id: The id of the job's dispatch.
        """
        ...


class AsyncioTimer:
    """A :class:`JobTimer` that waits on the running event loop and keeps its steps in memory.

    It owns the tasks it starts. Closing it cancels every step still pending
    and logs the jobs lost, each as ``Job:task_id``, in one line.
    """

    def __init__(self) -> None:
        self._tasks: dict[asyncio.Task[None], str] = {}

    def schedule(self, delay: Countdown, step: Step, *, job: str, task_id: str) -> None:
        """Run *step* on the running loop after *delay*.

        Args:
            delay: How long to wait.
            step: What to run afterwards.
            job: The qualified name of the job the step runs.
            task_id: The id of the job's dispatch.
        """
        task = asyncio.get_running_loop().create_task(self._after(delay, step))
        self._tasks[task] = f"{job}:{task_id}"
        task.add_done_callback(self._tasks.pop)

    @staticmethod
    async def _after(delay: Countdown, step: Step) -> None:
        await asyncio.sleep(delay.seconds)
        try:
            await step()
        except Exception:
            _log.exception("Delayed job step failed")

    async def close(self) -> None:
        """Cancel every pending step and log the jobs lost with them."""
        lost = dict(self._tasks)
        for task in lost:
            task.cancel()
        await asyncio.gather(*lost, return_exceptions=True)
        if lost:
            _log.warning("Delayed jobs lost at shutdown", jobs=list(lost.values()))
