"""Observe the jobs an application dispatches, build valid job data, and move the inline clock.

:class:`RecordingJobTransport` records each dispatch once the execution that
made it commits; ``app.job_service: loom.testing:RecordingJobTransport``
selects it, and ``JobService.default_transport`` returns it. :class:`JobTestData`
builds valid envelopes, deliveries and dispatches. :class:`ManualTimer` replaces
the timer of the inline transport: ``Override.instance(JobTimer, ManualTimer())``.
"""

from __future__ import annotations

import copy
import functools
import heapq
import itertools
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.identity import Identity
from loom.core.job.callback import ANY_CALLBACKS, DispatchableCallbacks
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish
from loom.core.job.delivery import JobDelivery
from loom.core.job.dispatch import JobDispatch
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.handle import JobHandle
from loom.core.job.job import Job
from loom.core.job.policy import DEFAULT_RETRY, Countdown, RetryPolicy
from loom.core.job.timer import Step

if TYPE_CHECKING:
    from loom.composition import JobTransportContext
    from loom.core.job.callback import JobCallback

ResultT = TypeVar("ResultT")


class NoopJob(Job[None]):
    """A job that does nothing: the default job of :class:`JobTestData`."""

    def execute(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class JobTestData:
    """Shared valid job data for tests of envelopes, deliveries and dispatches.

    Attributes:
        job_type: The job every built object names.
        policy: The retry policy of every built delivery.
        trace_id: The trace every built envelope carries.
    """

    job_type: type[Job[Any]] = NoopJob
    policy: RetryPolicy = DEFAULT_RETRY
    trace_id: str | None = None

    def with_job_type(self, job_type: type[Job[Any]]) -> JobTestData:
        """A copy whose envelope names *job_type*."""
        return replace(self, job_type=job_type)

    def with_policy(self, policy: RetryPolicy) -> JobTestData:
        """A copy whose deliveries use *policy*."""
        return replace(self, policy=policy)

    def with_trace_id(self, trace_id: str | None) -> JobTestData:
        """A copy whose envelopes carry *trace_id*."""
        return replace(self, trace_id=trace_id)

    def envelope(
        self,
        *,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        task_id: str | None = "task-1",
        identity: dict[str, Any] | None = None,
    ) -> JobEnvelope:
        """Build one valid wire envelope using this test data's job and trace."""
        return JobEnvelope(
            job=wire_job_name(self.job_type.__qualname__),
            payload=payload or {},
            params=params,
            task_id=task_id,
            trace_id=self.trace_id,
            identity=identity,
        )

    def delivery(
        self,
        *,
        envelope: JobEnvelope | None = None,
        attempt: int = 1,
        postponements: int = 0,
        timeout_s: float | None = None,
    ) -> JobDelivery:
        """Build one delivery, through ``JobOptions.delivery_for``, with this test data's policy."""
        resolved = JobSettings().of(self.job_type)
        options = replace(resolved, retry=replace(resolved.retry, policy=self.policy))
        return options.delivery_for(
            self.job_type,
            envelope or self.envelope(),
            attempt=attempt,
            postponements=postponements,
            timeout_s=timeout_s,
        )

    def dispatch(
        self,
        *,
        settings: JobSettings | None = None,
        callbacks: DispatchableCallbacks = ANY_CALLBACKS,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
    ) -> JobDispatch:
        """Build a validated dispatch for transport tests."""
        return JobDispatch.build(
            self.job_type,
            options=(settings or JobSettings()).of(self.job_type),
            callbacks=callbacks,
            payload=payload,
            params=params,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )


@dataclass(frozen=True)
class DispatchedJob:
    """One dispatch :class:`RecordingJobTransport` saw committed, as it was at dispatch."""

    job_type: type[Job[Any]]
    payload: dict[str, Any]
    params: dict[str, Any] | None
    queue: str
    countdown: int
    identity: Identity | None
    """The authenticated caller acting at dispatch, ``None`` for none (as the real services)."""


class ManualTimer:
    """A :class:`~loom.core.job.timer.JobTimer` whose clock moves only when a test says so.

    Nothing waits: a scheduled step runs inside :meth:`advance`, once the
    clock reaches its time.
    """

    def __init__(self) -> None:
        self._now = 0
        self._steps: list[tuple[int, int, Step]] = []
        self._order = itertools.count()

    @property
    def pending(self) -> int:
        """How many steps are waiting for the clock."""
        return len(self._steps)

    def schedule(self, delay: Countdown, step: Step, *, job: str, task_id: str) -> None:
        """Hold *step* until the clock has moved *delay* seconds from now.

        Args:
            delay: How long the step waits.
            step: What runs afterwards.
            job: Ignored.
            task_id: Ignored.
        """
        _ = (job, task_id)
        heapq.heappush(self._steps, (self._now + delay.seconds, next(self._order), step))

    async def advance(self, seconds: int) -> None:
        """Move the clock forward and run, in order, every step whose time has come.

        A step another step schedules runs too, when its own time falls
        within the move.

        Args:
            seconds: How far the clock moves.
        """
        target = self._now + seconds
        while self._steps and self._steps[0][0] <= target:
            due, _, step = heapq.heappop(self._steps)
            self._now = due
            await step()
        self._now = target


class RecordingJobTransport:
    """Record validated dispatches at the same time a transport would send them.

    Args:
        context: The composition context, accepted for a custom transport.

    Attributes:
        dispatched: Committed dispatches in order, copied before publication.
    """

    def __init__(self, context: JobTransportContext | None = None) -> None:
        self.dispatched: list[DispatchedJob] = []

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Record a snapshot once the caller's unit of work commits."""
        record, handle = self._record(dispatch)
        add_pending_publish(functools.partial(self.dispatched.append, record))
        return handle

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Record a snapshot before returning the handle."""
        record, handle = self._record(dispatch)
        self.dispatched.append(record)
        return handle

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Accept callbacks as the recording transport has done."""
        return None

    def check_parallel_callbacks(
        self, on_all_success: type[JobCallback] | None, on_any_failure: type[JobCallback] | None
    ) -> None:
        """Accept group callbacks without invoking them, as recording dispatch has done."""
        return None

    def _record(self, dispatch: JobDispatch) -> tuple[DispatchedJob, JobHandle[Any]]:
        record = DispatchedJob(
            job_type=dispatch.job_type,
            payload=copy.deepcopy(dispatch.envelope.payload or {}),
            params=copy.deepcopy(dispatch.envelope.params),
            queue=dispatch.queue,
            countdown=dispatch.countdown.seconds,
            identity=dispatch.delegation.caller,
        )
        handle: JobHandle[Any] = JobHandle(
            job_id=str(dispatch.envelope.task_id),
            queue=dispatch.queue,
            dispatched_at=datetime.now(UTC),
        )
        return record, handle
