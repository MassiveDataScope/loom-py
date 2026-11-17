"""One delivery of a job: the message, which attempt it is and the policy that governs it."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from loom.core.job.policy import RetryPolicy

if TYPE_CHECKING:
    from loom.core.job.envelope import JobEnvelope
    from loom.core.job.job import Job

_ACTIVE: ContextVar[JobDelivery | None] = ContextVar("loom_job_delivery", default=None)


@dataclass(frozen=True, slots=True)
class JobDelivery:
    """One delivery of a job message to a worker.

    A job reads the delivery it runs under with :meth:`current`.

    Attributes:
        job_type: The job to run.
        envelope: The message: parameters, payload, trace and caller.
        task_id: The transport's id of the message, or ``None`` when it has none.
        attempt: The attempt number, from 1; deferrals do not advance it.
        deferrals: How many times the job asked to run again.
        policy: The resolved retry policy of the job.
        timeout_s: The seconds the run may take, or ``None`` for no limit.
    """

    job_type: type[Job[Any]]
    envelope: JobEnvelope
    task_id: str | None
    attempt: int
    deferrals: int
    policy: RetryPolicy
    timeout_s: float | None = None

    @staticmethod
    def current() -> JobDelivery | None:
        """The delivery being run in this context, or ``None`` outside a job run."""
        return _ACTIVE.get()

    @contextmanager
    def running(self) -> Iterator[None]:
        """Make this delivery the one :meth:`current` returns, restoring the previous after."""
        token = _ACTIVE.set(self)
        try:
            yield
        finally:
            _ACTIVE.reset(token)
