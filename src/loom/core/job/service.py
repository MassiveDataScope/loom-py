"""The public job API: in-process runs and dispatch through the transport of a queue."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.job.callback import ANY_CALLBACKS, DispatchableCallbacks, JobCallback
from loom.core.job.config import JobSettings
from loom.core.job.dispatch import JobDispatch
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.transport import JobTransport

if TYPE_CHECKING:
    from loom.core.engine.executor import UseCaseExecutor
    from loom.core.use_case.factory import UseCaseFactory

ResultT = TypeVar("ResultT")


class JobService:
    """Run jobs in process and dispatch them through the transport of their effective queue.

    A dispatch is validated once, before its transport schedules any work.
    Unrouted queues use the default transport.

    Args:
        default: Transport serving queues without an explicit route.
        factory: Builds jobs for an in-process run.
        executor: Runs an in-process job.
        routes: Transport selected by each routed queue name.
        settings: Resolved options of this application's jobs.
        callbacks: Callback classes discovered for this application.
    """

    def __init__(
        self,
        default: JobTransport,
        *,
        factory: UseCaseFactory | None = None,
        executor: UseCaseExecutor | None = None,
        routes: Mapping[str, JobTransport] | None = None,
        settings: JobSettings | None = None,
        callbacks: DispatchableCallbacks = ANY_CALLBACKS,
    ) -> None:
        self._default = default
        self._factory = factory
        self._executor = executor
        self._routes = dict(routes or {})
        self._settings = settings or JobSettings()
        self._callbacks = callbacks

    @property
    def default_transport(self) -> JobTransport:
        """The transport serving the queues without an explicit route."""
        return self._default

    def transport_for(self, queue: str) -> JobTransport:
        """The transport that sends the jobs of *queue*."""
        return self._routes.get(queue, self._default)

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute a job immediately in process and return its result.

        Raises:
            NotImplementedError: The service was built without a factory and executor.
        """
        if self._factory is None or self._executor is None:
            raise NotImplementedError(
                "JobService.run() requires factory and executor. "
                "Pass them at construction or dispatch the job."
            )
        result: ResultT = await self._executor.run(
            job_type, factory=self._factory, params=params, payload=payload
        )
        return result

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
        """Register a job for execution after the current unit of work commits.

        The job is flushed after a successful commit, or at the end of an
        execution without a unit of work. A rollback discards the dispatch.

        Args:
            job_type: Concrete job to dispatch.
            params: Primitive arguments bound to the execute signature.
            payload: Raw input command data and callback context.
            on_success: Callback invoked when the job succeeds.
            on_failure: Callback invoked when the job fails terminally.
            queue: Override the job's resolved queue.
            countdown: Delay in seconds, from zero through 900.
            priority: Transport priority override.

        Returns:
            A handle carrying the dispatch's task id.

        Raises:
            ValueError: A callback is unknown, its payload reserves a hook key,
                or the countdown falls outside zero through 900 seconds.
        """
        dispatch = self._build_dispatch(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        return self.transport_for(dispatch.queue).send_after_commit(dispatch)

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
        """Send a job now, even before the caller's writes commit.

        Args:
            job_type: Concrete job to dispatch.
            params: Primitive arguments bound to the execute signature.
            payload: Raw input command data and callback context.
            on_success: Callback invoked when the job succeeds.
            on_failure: Callback invoked when the job fails terminally.
            queue: Override the job's resolved queue.
            countdown: Delay in seconds, from zero through 900.
            priority: Transport priority override.

        Returns:
            A handle carrying the dispatch's task id.

        Raises:
            ValueError: A callback is unknown, its payload reserves a hook key,
                or the countdown falls outside zero through 900 seconds.
        """
        dispatch = self._build_dispatch(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        return await self.transport_for(dispatch.queue).send(dispatch)

    def dispatch_parallel(self, jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]]) -> JobGroup:
        """Schedule each job after commit and return handles in input order."""
        dispatches = [self._build_dispatch(job, payload=body) for job, body in jobs]
        return JobGroup(
            handles=tuple(
                self.transport_for(dispatch.queue).send_after_commit(dispatch)
                for dispatch in dispatches
            )
        )

    def _build_dispatch(
        self,
        job_type: type[Job[Any]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
    ) -> JobDispatch:
        options = self._settings.options_for(job_type)
        self.transport_for(queue or options.queue).check_callbacks(on_success, on_failure)
        return JobDispatch.build(
            job_type,
            options=options,
            callbacks=self._callbacks,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
