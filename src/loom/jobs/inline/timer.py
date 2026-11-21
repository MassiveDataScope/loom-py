"""The :class:`~loom.core.job.timer.JobTimer` that waits on the running event loop."""

from __future__ import annotations

import asyncio
import logging

from loom.core.job.config import CANCEL_AT_SHUTDOWN, JobShutdown
from loom.core.job.policy import Countdown
from loom.core.job.timer import Step

_log = logging.getLogger(__name__)


class AsyncioJobTimer:
    """A :class:`JobTimer` that waits on the running event loop and keeps its steps in memory.

    It owns the tasks it starts. Closing it follows its *shutdown* policy: under
    ``cancel`` it cancels every step still pending; under ``wait`` it first
    awaits the pending steps, and the steps they schedule, until none is left or
    the timeout is over. The jobs lost are logged, each as ``Job:task_id``, in
    one line.

    Args:
        shutdown: What closing does with the steps still pending.
    """

    def __init__(self, shutdown: JobShutdown = CANCEL_AT_SHUTDOWN) -> None:
        self._shutdown = shutdown
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
        """Let the pending steps run when waiting, then cancel the rest and log the jobs lost."""
        if self._shutdown.is_waiting:
            await self._wait_until_idle()
        lost = dict(self._tasks)
        for task in lost:
            task.cancel()
        await asyncio.gather(*lost, return_exceptions=True)
        if lost:
            _log.warning("Delayed jobs lost at shutdown", extra={"jobs": list(lost.values())})

    async def _wait_until_idle(self) -> None:
        try:
            async with asyncio.timeout(self._shutdown.timeout_s):
                while self._tasks:
                    await asyncio.wait(tuple(self._tasks))
        except TimeoutError:
            return
