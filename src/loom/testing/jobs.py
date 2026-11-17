"""Observe the jobs an application dispatches.

:class:`RecordingJobService` records each dispatch once the execution that
made it commits; ``app.job_service: loom.testing:RecordingJobService``
selects it.
"""

from __future__ import annotations

import copy
import functools
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.identity import Identity, captured_delegation
from loom.core.job.callback import DispatchableCallbacks
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.policy import Countdown
from loom.core.job.service import InlineJobService

if TYPE_CHECKING:
    from loom.composition import JobBackendContext
    from loom.core.job.callback import JobCallback

ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class DispatchedJob:
    """One dispatch :class:`RecordingJobService` saw committed, as it was at dispatch."""

    job_type: type[Job[Any]]
    payload: dict[str, Any]
    params: dict[str, Any] | None
    queue: str
    countdown: int
    identity: Identity | None
    """The authenticated caller acting at dispatch, ``None`` for none (as the real services)."""


class RecordingJobService:
    """A ``JobService`` recording what would be sent, for tests; nothing is sent.

    A dispatch is recorded on the publish lane, where a real backend sends:
    after the execution commits, never when it rolls back. Outside an
    execution it is recorded by
    :func:`~loom.core.job.context.flush_pending_dispatches`. :meth:`run`
    executes in process, as ``InlineJobService`` does; callbacks are never
    invoked, and one the scan did not find is refused as the other services do
    when built from a context.

    Args:
        context: Given when ``app.job_service`` selects this class; needed by
            :meth:`run` only.

    Attributes:
        dispatched: Every committed dispatch, in order; payload and params are
            copied at dispatch, so a later mutation does not show.
    """

    def __init__(self, context: JobBackendContext | None = None) -> None:
        self.dispatched: list[DispatchedJob] = []
        self._settings = JobSettings() if context is None else context.settings
        self._callbacks = DispatchableCallbacks.any() if context is None else context.callbacks
        self._inline = (
            None
            if context is None
            else InlineJobService(
                context.factory,
                context.executor,
                settings=self._settings,
                callbacks=self._callbacks,
            )
        )

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute *job_type* now, as ``InlineJobService.run`` does.

        Raises:
            RuntimeError: When the service was built without a context.
        """
        if self._inline is None:
            raise RuntimeError("RecordingJobService runs jobs only when built with a context")
        return await self._inline.run(job_type, params=params, payload=payload)

    def dispatch_after_commit(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
    ) -> JobHandle[ResultT]:
        """Record *job_type* once the current execution commits.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
        """
        record, handle = self._record(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        add_pending_publish(functools.partial(self.dispatched.append, record))
        return handle

    async def dispatch_now(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
    ) -> JobHandle[ResultT]:
        """Record *job_type* now, even inside a use case.

        The job can run before the caller's writes commit and is not undone
        if they roll back. Inline runs the job inside the caller's
        transaction. Work that depends on the caller's writes uses
        :meth:`dispatch_after_commit`.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
        """
        record, handle = self._record(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        self.dispatched.append(record)
        return handle

    def _record(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        on_success: type[JobCallback] | None,
        on_failure: type[JobCallback] | None,
        queue: str | None,
        countdown: int | None,
        priority: int | None,
    ) -> tuple[DispatchedJob, JobHandle[ResultT]]:
        """Copy the dispatch and build its handle.

        Args:
            job_type: Concrete Job subclass to record.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type; refused if the scan did not find it.
            on_failure: Callback type; refused if the scan did not find it.
            queue: Override the job's queue for the recorded dispatch.
            countdown: Delay in seconds, 0 through 900 inclusive.
            priority: Ignored.

        Returns:
            The recorded dispatch and a handle for it.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
        """
        _ = priority
        options = self._settings.of(job_type)
        delay = Countdown(options.countdown if countdown is None else countdown)
        payload = payload or {}
        self._callbacks.refuse(payload, on_success, on_failure)
        record = DispatchedJob(
            job_type=job_type,
            payload=copy.deepcopy(payload),
            params=copy.deepcopy(params),
            queue=queue or options.queue,
            countdown=delay.seconds,
            identity=captured_delegation().caller,
        )
        handle: JobHandle[ResultT] = JobHandle(
            job_id=str(uuid.uuid4()), queue=record.queue, dispatched_at=datetime.now(UTC)
        )
        return record, handle

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Record each job as :meth:`dispatch_after_commit` does; callbacks are never invoked."""
        _ = (on_all_success, on_any_failure)
        return JobGroup(
            handles=tuple(
                self.dispatch_after_commit(job_type, payload=payload) for job_type, payload in jobs
            )
        )
