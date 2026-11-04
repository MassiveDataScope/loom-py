"""Route each job to the job service that serves its queue."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, TypeVar

from loom.core.config.errors import ConfigError
from loom.core.errors.errors import LoomError
from loom.core.identity import current_identity
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.service import JobService

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.job.callback import JobCallback
    from loom.core.use_case.factory import UseCaseFactory

ResultT = TypeVar("ResultT")

BUILTIN_JOB_BACKENDS: Final = ("inline", "celery", "sqs")
"""The names ``app.job_service`` takes besides a ``module:Class`` reference."""


class DuplicateJobQueue(ConfigError):
    """Two job backends declare the same queue."""

    def __init__(self, queue: str) -> None:
        super().__init__(f"queue {queue!r} is declared by two job backends; keep it in one")
        self.queue = queue


class UnservedJobQueue(ConfigError):
    """A discovered job's queue is served by no backend and there is no default."""

    def __init__(self, job_name: str, queue: str) -> None:
        super().__init__(
            f"job {job_name} goes to queue {queue!r}, which no job backend serves; "
            "declare the queue in a backend section or set app.job_service"
        )
        self.job_name = job_name
        self.queue = queue


class UnroutedJobError(LoomError):
    """A dispatch names a queue no backend serves, and there is no default backend."""

    retryable = False

    def __init__(self, job_name: str, queue: str) -> None:
        super().__init__(
            f"job {job_name} cannot go to queue {queue!r}: no job backend serves it",
            code="job_unrouted",
        )
        self.job_name = job_name
        self.queue = queue


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
    ``__queue__``. A queue no backend declares goes to *default*. Each backend
    keeps its own post-commit semantics, callbacks and handles; :meth:`run`
    executes in-process, as every backend does.

    Args:
        backends: Each backend with the queues it declares.
        default: Backend of the queues no backend declares, or ``None``.
        factory: Builds the job :meth:`run` executes.
        executor: Executes it.

    Raises:
        DuplicateJobQueue: When two backends declare the same queue.
    """

    def __init__(
        self,
        backends: Sequence[tuple[JobService, Sequence[str]]],
        *,
        default: JobService | None,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
    ) -> None:
        routes: dict[str, JobService] = {}
        for backend, queues in backends:
            for queue in queues:
                if routes.setdefault(queue, backend) is not backend:
                    raise DuplicateJobQueue(queue)
        self._routes = MappingProxyType(routes)
        self._default = default
        self._factory = factory
        self._executor = executor

    @property
    def routes(self) -> Mapping[str, JobService]:
        """The backend of each declared queue."""
        return self._routes

    @property
    def default(self) -> JobService | None:
        """The backend of the queues no backend declares."""
        return self._default

    def require_served(self, jobs: Iterable[type[Job[Any]]]) -> None:
        """Refuse a job whose queue no backend serves.

        Raises:
            UnservedJobQueue: For the first such job.
        """
        if self._default is not None:
            return
        for job_type in jobs:
            if job_type.__queue__ not in self._routes:
                raise UnservedJobQueue(job_type.__qualname__, job_type.__queue__)

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute the job in-process, whatever backend its queue routes to."""
        instance = self._factory.build(job_type)
        return await self._executor.execute(
            instance, params=params, payload=payload, identity=current_identity()
        )

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
        """Dispatch through the backend of the effective queue.

        Raises:
            UnroutedJobError: When no backend serves the queue and there is
                no default; nothing is registered for the commit.
        """
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
            UnroutedJobError: When a job's queue has no backend; nothing is
                registered for the commit.
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
        effective = queue or job_type.__queue__
        backend = self._routes.get(effective, self._default)
        if backend is None:
            raise UnroutedJobError(job_type.__qualname__, effective)
        return backend
