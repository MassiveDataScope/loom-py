"""Waiting for a delay: the port inline jobs wait on."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from loom.core.engine.events import AbandonReason, EventKind, ExecutionEvent, Status
from loom.core.job.policy import Countdown

Step = Callable[[], Awaitable[None]]
"""What runs when a delay is over."""


@dataclass(frozen=True, slots=True)
class DelayedJob:
    """A job a timer holds: what it needs to say which one was lost.

    Attributes:
        job: The qualified name of the job.
        task_id: The id of the job's dispatch.
    """

    job: str
    task_id: str

    @property
    def label(self) -> str:
        """The job as ``Job:task_id``."""
        return f"{self.job}:{self.task_id}"

    def abandoned(self, reason: AbandonReason, error: Exception | None = None) -> ExecutionEvent:
        """The ``JOB_ABANDONED`` event that reports this job lost for *reason*."""
        return ExecutionEvent(
            EventKind.JOB_ABANDONED,
            self.job,
            status=Status.ABANDONED,
            error=error,
            reason=reason,
        )


class JobDelayRefusedError(RuntimeError):
    """Raised when a job is delayed where no timer waits for it.

    Attributes:
        delay: The countdown that was refused; ``None`` for a ``JobPostponement``, whose
            delay the job or its backoff chooses.

    Args:
        delay: The countdown that was refused, if any.
    """

    def __init__(self, delay: Countdown | None = None) -> None:
        waited = "wait" if delay is None else f"wait {delay.seconds}s"
        super().__init__(
            f"cannot {waited}: this job service has no timer, so a countdown "
            "or a JobPostponement cannot be honoured; use Celery or SQS"
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

    async def close(self) -> None:
        """Apply the timer's shutdown policy to the steps still pending.

        The application calls it once, when it stops, for the timer its inline
        service waits on, whether composition built it or a test bound it.
        """
        ...
