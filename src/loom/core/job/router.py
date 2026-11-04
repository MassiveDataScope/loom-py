"""Route each job to the job service that serves its queue."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.errors.errors import LoomError
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.service import JobService

if TYPE_CHECKING:
    from loom.core.job.callback import JobCallback

ResultT = TypeVar("ResultT")


class MixedJobBackendsError(LoomError):
    """``dispatch_parallel`` with callbacks spans jobs of more than one backend."""

    retryable = False

    def __init__(self) -> None:
        super().__init__(
            "dispatch_parallel callbacks need every job on one job backend; "
            "these jobs route to several",
            code="job_backends_mixed",
        )


class JobRouter:
    """A :class:`~loom.core.job.service.JobService` that routes each job by its queue.

    The effective queue is the ``queue=`` of the call, otherwise the job's
    ``__queue__``; a queue *routes* does not name goes to *default*. Each
    backend keeps its own post-commit semantics, callbacks and handles.
    :meth:`run` goes to *default*: the built-in backends all run in-process.

    Args:
        routes: The backend of each routed queue.
        default: The backend of every other queue.
    """

    def __init__(self, routes: Mapping[str, JobService], *, default: JobService) -> None:
        self._routes = dict(routes)
        self._default = default

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute the job in-process through the default backend."""
        return await self._default.run(job_type, params=params, payload=payload)

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
        """Dispatch through the backend of the effective queue."""
        return self._backend_for(job_type, queue).dispatch(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
            eta=eta,
        )

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Dispatch the jobs through their backends; the group keeps the input order.

        Jobs of one backend go to its own ``dispatch_parallel``.

        Raises:
            MixedJobBackendsError: When callbacks are given and the jobs route
                to more than one backend.
        """
        backends = [self._backend_for(job_type, None) for job_type, _ in jobs]
        if not backends:
            return JobGroup(handles=())
        first = backends[0]
        if all(backend is first for backend in backends):
            return first.dispatch_parallel(
                jobs, on_all_success=on_all_success, on_any_failure=on_any_failure
            )
        if on_all_success is not None or on_any_failure is not None:
            raise MixedJobBackendsError()
        return JobGroup(
            handles=tuple(
                backend.dispatch(job_type, payload=payload)
                for backend, (job_type, payload) in zip(backends, jobs, strict=True)
            )
        )

    def _backend_for(self, job_type: type[Job[Any]], queue: str | None) -> JobService:
        return self._routes.get(queue or job_type.__queue__, self._default)
