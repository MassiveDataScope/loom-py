"""The :class:`~loom.core.job.timer.JobTimer` that waits on the running event loop."""

from __future__ import annotations

import asyncio
import logging

from loom.core.engine.events import AbandonReason
from loom.core.engine.execution_observer import ExecutionObserver, NullExecutionObserver
from loom.core.job.config import CANCEL_AT_SHUTDOWN, JobShutdown
from loom.core.job.policy import Countdown
from loom.core.job.timer import DelayedJob, Step
from loom.core.logger import LogField

_log = logging.getLogger(__name__)


class AsyncioJobTimer:
    """A :class:`JobTimer` that waits on the running event loop and keeps its steps in memory.

    It owns the tasks it starts. Closing it follows its *shutdown* policy: under
    ``cancel`` it cancels every step still pending; under ``wait`` it first
    awaits the pending steps, and the steps they schedule, until none is left or
    the timeout is over. Each job lost, because the process stopped or because
    its step raised outside the settle path, is reported to the execution observer
    as ``JOB_ABANDONED`` with reason ``shutdown`` or ``step_crashed`` and logged as
    one ERROR record.

    Args:
        shutdown: What closing does with the steps still pending.
        execution_observer: Receives the abandonments; none when omitted.
    """

    def __init__(
        self,
        shutdown: JobShutdown = CANCEL_AT_SHUTDOWN,
        *,
        execution_observer: ExecutionObserver | None = None,
    ) -> None:
        self._shutdown = shutdown
        self._tasks: dict[asyncio.Task[None], DelayedJob] = {}
        self._execution_observer: ExecutionObserver = (
            NullExecutionObserver() if execution_observer is None else execution_observer
        )

    def schedule(self, delay: Countdown, step: Step, *, job: str, task_id: str) -> None:
        """Run *step* on the running loop after *delay*.

        Args:
            delay: How long to wait.
            step: What to run afterwards.
            job: The qualified name of the job the step runs.
            task_id: The id of the job's dispatch.
        """
        delayed = DelayedJob(job, task_id)
        task = asyncio.get_running_loop().create_task(self._after(delay, step, delayed))
        self._tasks[task] = delayed
        task.add_done_callback(self._tasks.pop)

    async def _after(self, delay: Countdown, step: Step, delayed: DelayedJob) -> None:
        await asyncio.sleep(delay.seconds)
        try:
            await step()
        except Exception as error:
            self._abandon(delayed, AbandonReason.STEP_CRASHED, error)

    async def close(self) -> None:
        """Let the pending steps run when waiting, then cancel the rest and report them lost."""
        if self._shutdown.is_waiting:
            await self._wait_until_idle()
        lost = dict(self._tasks)
        for task in lost:
            task.cancel()
        await asyncio.gather(*lost, return_exceptions=True)
        for delayed in lost.values():
            self._abandon(delayed, AbandonReason.SHUTDOWN)

    def _abandon(
        self, delayed: DelayedJob, reason: AbandonReason, error: Exception | None = None
    ) -> None:
        self._execution_observer.on_event(delayed.abandoned(reason, error))
        _log.error(
            "Job abandoned",
            extra={
                LogField.JOB: delayed.job,
                LogField.TASK_ID: delayed.task_id,
                LogField.REASON: reason.value,
            },
            exc_info=error,
        )

    async def _wait_until_idle(self) -> None:
        try:
            async with asyncio.timeout(self._shutdown.timeout_s):
                while self._tasks:
                    await asyncio.wait(tuple(self._tasks))
        except TimeoutError:
            return
