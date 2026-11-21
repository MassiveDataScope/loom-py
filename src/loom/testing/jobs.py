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
from loom.core.job.config import CANCEL_AT_SHUTDOWN, JobSettings, JobShutdown
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
        from loom.testing import make

        return make(
            JobEnvelope,
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
        """Build one delivery of this test data's job under its policy."""
        from loom.testing import make

        return make(
            JobDelivery,
            job_type=self.job_type,
            envelope=envelope or self.envelope(),
            attempt=attempt,
            postponements=postponements,
            policy=self.policy,
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
            options=(settings or JobSettings()).options_for(self.job_type),
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
    task_id: str
    payload: dict[str, Any]
    params: dict[str, Any] | None
    queue: str
    countdown: int
    identity: Identity | None
    """The authenticated caller acting at dispatch, ``None`` for none (as the real services)."""
    trace_id: str | None = None


class ManualTimer:
    """A :class:`~loom.core.job.timer.JobTimer` whose clock moves only when a test says so.

    Nothing waits: a scheduled step runs inside :meth:`advance`, once the
    clock reaches its time.

    :meth:`close` rehearses the shutdown of the real timer on the same clock. Under
    ``cancel`` it drops every pending step. Under ``wait`` it runs the pending
    steps in order, moving the clock to each, and the steps they schedule, until
    none is pending or the next one falls beyond ``timeout_s`` seconds from now
    (no timeout runs until idle); the rest is dropped. :attr:`lost` lists what was
    dropped, each as ``Job:task_id``. Composition does not close a timer a test
    bound: the test calls :meth:`close` itself.

    Args:
        shutdown: What :meth:`close` does with the steps still pending.
    """

    def __init__(self, shutdown: JobShutdown = CANCEL_AT_SHUTDOWN) -> None:
        self._shutdown = shutdown
        self._now = 0.0
        self._steps: list[tuple[float, int, Step, str]] = []
        self._order = itertools.count()
        self.lost: list[str] = []

    @property
    def pending(self) -> int:
        """How many steps are waiting for the clock."""
        return len(self._steps)

    def schedule(self, delay: Countdown, step: Step, *, job: str, task_id: str) -> None:
        """Hold *step* until the clock has moved *delay* seconds from now.

        Args:
            delay: How long the step waits.
            step: What runs afterwards.
            job: The qualified name of the job, listed in :attr:`lost` if dropped.
            task_id: The id of the dispatch, listed in :attr:`lost` if dropped.
        """
        entry = (self._now + delay.seconds, next(self._order), step, f"{job}:{task_id}")
        heapq.heappush(self._steps, entry)

    async def advance(self, seconds: int) -> None:
        """Move the clock forward and run, in order, every step whose time has come.

        A step another step schedules runs too, when its own time falls
        within the move.

        Args:
            seconds: How far the clock moves.
        """
        target = self._now + seconds
        await self._run_until(target)
        self._now = target

    async def close(self) -> None:
        """Run the pending steps when waiting, then drop the rest into :attr:`lost`."""
        if self._shutdown.is_waiting:
            timeout = self._shutdown.timeout_s
            await self._run_until(None if timeout is None else self._now + timeout)
        self.lost = [label for *_, label in sorted(self._steps)]
        self._steps.clear()

    async def _run_until(self, limit: float | None) -> None:
        while self._steps and (limit is None or self._steps[0][0] <= limit):
            due, _, step, _ = heapq.heappop(self._steps)
            self._now = due
            await step()


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

    def _record(self, dispatch: JobDispatch) -> tuple[DispatchedJob, JobHandle[Any]]:
        record = DispatchedJob(
            job_type=dispatch.job_type,
            task_id=str(dispatch.envelope.task_id),
            payload=copy.deepcopy(dispatch.envelope.payload or {}),
            params=copy.deepcopy(dispatch.envelope.params),
            queue=dispatch.queue,
            countdown=dispatch.countdown.seconds,
            identity=dispatch.delegation.caller,
            trace_id=dispatch.envelope.trace_id,
        )
        handle: JobHandle[Any] = JobHandle(
            job_id=str(dispatch.envelope.task_id),
            queue=dispatch.queue,
            dispatched_at=datetime.now(UTC),
        )
        return record, handle
