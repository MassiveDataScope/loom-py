"""One delivery of a job: the message, which attempt it is and the policy that governs it."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

import msgspec

from loom.core.job.envelope import JobEnvelope
from loom.core.job.job import Job
from loom.core.job.policy import MIN_ATTEMPTS, RetryPolicy

MIN_POSTPONEMENTS = 0
"""A delivery that was never postponed has none."""


@dataclass(frozen=True, slots=True)
class JobDelivery:
    """One delivery of a job message to a worker.

    A job declares ``delivery: JobDelivery | None = Delivery()`` on ``execute``
    to read the delivery it runs under; a job run by a caller gets ``None``.

    Attributes:
        job_type: The job to run.
        envelope: The message: parameters, payload, trace and caller.
        task_id: The transport's id of the message, or ``None`` when it has none.
        attempt: The attempt number, from 1; postponements do not advance it.
        postponements: How many times the job asked to run again.
        policy: The resolved retry policy of the job.
        queue: The queue the job is configured for.
        timeout_s: The seconds the run may take, or ``None`` for no limit.

    Raises:
        ValueError: When *attempt* is below 1 or *postponements* is negative.
    """

    job_type: type[Job[Any]]
    envelope: JobEnvelope
    attempt: Annotated[int, msgspec.Meta(ge=MIN_ATTEMPTS)]
    postponements: Annotated[int, msgspec.Meta(ge=MIN_POSTPONEMENTS)]
    policy: RetryPolicy
    queue: str
    timeout_s: float | None = None

    def __post_init__(self) -> None:
        if self.attempt < MIN_ATTEMPTS:
            raise ValueError(f"attempt must be at least {MIN_ATTEMPTS}, not {self.attempt}")
        if self.postponements < MIN_POSTPONEMENTS:
            raise ValueError(f"postponements cannot be negative, not {self.postponements}")

    @property
    def task_id(self) -> str | None:
        """The task id carried by this delivery's envelope, when present."""
        return self.envelope.task_id
