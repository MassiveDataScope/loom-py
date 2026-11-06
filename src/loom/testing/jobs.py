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

from loom.core.identity import Identity, current_identity
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
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
    identity: Identity


class RecordingJobService:
    """A ``JobService`` recording what would be sent, for tests; nothing is sent.

    A dispatch is recorded on the publish lane, where a real backend sends:
    after the execution commits, never when it rolls back. Outside an
    execution it is recorded by
    :func:`~loom.core.job.context.flush_pending_dispatches`. :meth:`run`
    executes in process, as ``InlineJobService`` does; callbacks are accepted
    and never invoked.

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
        self._inline = (
            None
            if context is None
            else InlineJobService(context.factory, context.executor, settings=self._settings)
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

    def dispatch(
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
        eta: datetime | None = None,
    ) -> JobHandle[ResultT]:
        """Record *job_type* once the current execution commits."""
        _ = (on_success, on_failure, countdown, priority, eta)
        record = DispatchedJob(
            job_type=job_type,
            payload=copy.deepcopy(payload or {}),
            params=copy.deepcopy(params),
            queue=queue or self._settings.of(job_type).queue,
            identity=current_identity(),
        )
        add_pending_publish(functools.partial(self.dispatched.append, record))
        return JobHandle(
            job_id=str(uuid.uuid4()), queue=record.queue, dispatched_at=datetime.now(UTC)
        )

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Record each job as :meth:`dispatch` does; callbacks are never invoked."""
        _ = (on_all_success, on_any_failure)
        return JobGroup(
            handles=tuple(self.dispatch(job_type, payload=payload) for job_type, payload in jobs)
        )
