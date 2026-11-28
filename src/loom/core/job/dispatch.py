"""A validated job dispatch shared by every transport."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from loom.core.identity import Delegation, captured_delegation
from loom.core.job.callback import DispatchableCallbacks, JobCallback
from loom.core.job.config import JobOptions
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.job import Job
from loom.core.job.policy import Countdown
from loom.core.tracing import get_trace_id


@dataclass(frozen=True, slots=True)
class JobDispatch:
    """One checked request to send a job, with its caller and wire identity captured."""

    job_type: type[Job[Any]]
    envelope: JobEnvelope
    options: JobOptions
    queue: str
    countdown: Countdown
    priority: int
    on_success: type[JobCallback] | None
    on_failure: type[JobCallback] | None
    delegation: Delegation

    @classmethod
    def build(
        cls,
        job_type: type[Job[Any]],
        *,
        options: JobOptions,
        callbacks: DispatchableCallbacks,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
    ) -> JobDispatch:
        """Resolve options and validate the dispatch before any publish is scheduled.

        Raises:
            ValueError: A callback is unknown, its payload reserves a hook key,
                or the countdown falls outside zero through 900 seconds.
        """
        body = payload or {}
        callbacks.refuse(body, on_success, on_failure)
        delegation = captured_delegation()
        envelope = JobEnvelope(
            job=wire_job_name(job_type.__qualname__),
            payload=body,
            params=params,
            trace_id=get_trace_id(),
            identity=delegation.encoded(),
            task_id=str(uuid.uuid4()),
        )
        return cls(
            job_type=job_type,
            envelope=envelope,
            options=options,
            queue=queue or options.queue,
            countdown=Countdown(options.countdown if countdown is None else countdown),
            priority=options.priority if priority is None else priority,
            on_success=on_success,
            on_failure=on_failure,
            delegation=delegation,
        )
