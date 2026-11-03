from __future__ import annotations

import inspect
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

# RuntimeExecutor imported at runtime to avoid circular imports at module load.
# The type annotation is kept in TYPE_CHECKING only.
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from loom.core.identity import ANONYMOUS, Identity, current_identity
from loom.core.job.callback import JobCallback
from loom.core.job.context import add_pending_dispatch
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor

ResultT = TypeVar("ResultT")


# ---------------------------------------------------------------------------
# Callback helpers — module-level, single responsibility each
# ---------------------------------------------------------------------------


async def _invoke_success_callback(
    factory: UseCaseFactory,
    callback_type: type[Any],
    job_id: str,
    result: Any,
    context: dict[str, Any],
) -> None:
    cb = factory.build(callback_type)
    if inspect.iscoroutinefunction(cb.on_success):
        await cb.on_success(job_id=job_id, result=result, **context)
    else:
        cb.on_success(job_id=job_id, result=result, **context)


async def _invoke_failure_callback(
    factory: UseCaseFactory,
    callback_type: type[Any],
    job_id: str,
    exc: BaseException,
    context: dict[str, Any],
) -> None:
    cb = factory.build(callback_type)
    exc_type = type(exc).__qualname__
    exc_msg = str(exc)
    if inspect.iscoroutinefunction(cb.on_failure):
        await cb.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)
    else:
        cb.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)


# ---------------------------------------------------------------------------
# Pending dispatch — replaces closures with a typed, testable object
# ---------------------------------------------------------------------------


@dataclass
class _PendingDispatch:
    """Encapsulates a single inline dispatch ready to run after UoW commit.

    Avoids nested function definitions (closures) by capturing all context
    as explicit dataclass fields.  The ``run`` method is enqueued on the
    post-commit channel and called when the executor drains it.
    """

    job_type: type[Job[Any]]
    task_id: str
    payload: dict[str, Any]
    params: dict[str, Any] | None
    on_success: type[Any] | None
    on_failure: type[Any] | None
    result_holder: list[Any]
    factory: UseCaseFactory
    executor: RuntimeExecutor = field(repr=False)
    identity: Identity = ANONYMOUS

    async def run(self) -> None:
        """Build the job, execute it, store the result, and invoke callbacks."""
        instance = self.factory.build(self.job_type)
        try:
            result: Any = await self.executor.execute(
                instance,
                params=self.params,
                payload=self.payload,
                identity=self.identity,
            )
            self.result_holder.append(result)
            if self.on_success is not None:
                await _invoke_success_callback(
                    self.factory, self.on_success, self.task_id, result, self.payload
                )
        except Exception as exc:
            if self.on_failure is not None:
                await _invoke_failure_callback(
                    self.factory, self.on_failure, self.task_id, exc, self.payload
                )


# ---------------------------------------------------------------------------
# JobService protocol
# ---------------------------------------------------------------------------


class JobService(Protocol):
    """Protocol for dispatching and running Jobs.

    Injected into UseCases and application code as any other service.
    The caller never knows whether it is talking to Celery or to an inline
    runner — both implement this protocol.

    Example::

        class NotifyUsersUseCase(UseCase[User, None]):
            def __init__(self, job_service: JobService) -> None:
                self._jobs = job_service

            async def execute(self, cmd: NotifyCmd = Input()) -> None:
                self._jobs.dispatch(SendEmailJob, payload={"email": cmd.email})
    """

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute a Job immediately and return its result.

        Args:
            job_type: Concrete Job subclass to run.
            params: Primitive params bound to the execute signature.
            payload: Raw dict for ``Input()`` command construction.

        Returns:
            The value returned by ``Job.execute()``.
        """
        ...

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
        """Register a Job for deferred execution after the current UoW commits.

        The job is NOT sent immediately.  It is queued in the current async
        context and flushed by the executor after a successful commit.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked on job success.
            on_failure: Callback type invoked on job failure.
            queue: Override the job's default ``__queue__``.
            countdown: Delay in seconds before execution (Celery only).
            priority: Task priority override (Celery only).
            eta: Absolute datetime for first execution (Celery only).

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` for result retrieval.
        """
        ...

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Dispatch multiple Jobs concurrently and return a group handle.

        Args:
            jobs: Sequence of ``(job_type, payload)`` pairs.
            on_all_success: Callback invoked when all jobs succeed (Celery chord).
            on_any_failure: Callback invoked when any job fails.

        Returns:
            A :class:`~loom.core.job.handle.JobGroup` for collective result retrieval.
        """
        ...


# ---------------------------------------------------------------------------
# InlineJobService — synchronous execution for tests and local mode
# ---------------------------------------------------------------------------


class InlineJobService:
    """Executes Jobs inline without a broker, for tests and local mode.

    Satisfies the :class:`JobService` protocol structurally.  The
    ``dispatch()`` method respects the post-commit semantics: the job is
    enqueued on the execution's post-commit channel and runs when the
    executor drains it, after the UoW has closed (or at the end of an
    execution without one).  Outside an execution the fallback channel
    holds it until :func:`~loom.core.job.context.flush_pending_dispatches`.

    A job runs as :func:`~loom.core.identity.current_identity` at the time of
    the call, which inside an execution is the identity that execution was
    given. With no transport and no execution to set one, that is
    :data:`~loom.core.identity.identity.ANONYMOUS`.

    Args:
        factory: Used to build Job and callback instances from the container.
        executor: Used to drive the compiled ExecutionPlan.

    Example::

        service = InlineJobService(factory, executor)
        handle = service.dispatch(SendEmailJob, payload={"email": "a@b.com"})
        await flush_pending_dispatches()  # outside an execution only
        result = handle.wait()
    """

    def __init__(self, factory: UseCaseFactory, executor: RuntimeExecutor) -> None:
        self._factory = factory
        self._executor = executor

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute the Job immediately and return its result.

        Args:
            job_type: Concrete Job subclass to run.
            params: Primitive params bound to the execute signature.
            payload: Raw dict for ``Input()`` command construction.

        Returns:
            The value returned by ``Job.execute()``.
        """
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
        on_success: type[Any] | None = None,
        on_failure: type[Any] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
        eta: datetime | None = None,
    ) -> JobHandle[ResultT]:
        """Queue the Job for post-commit execution and return a handle.

        ``countdown``, ``priority``, and ``eta`` are accepted but ignored —
        inline execution is always immediate after flush.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked after successful execution.
            on_failure: Callback type invoked after failed execution.
            queue: Overrides the job's ``__queue__`` for the returned handle.
            countdown: Ignored in inline mode.
            priority: Ignored in inline mode.
            eta: Ignored in inline mode.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` whose ``wait()``
            returns the result once the queue is flushed.
        """
        _ = (countdown, priority, eta)  # accepted for API compat; inline ignores scheduling hints
        task_id = str(uuid.uuid4())
        result_holder: list[Any] = []
        pending = _PendingDispatch(
            job_type=job_type,
            task_id=task_id,
            payload=payload or {},
            params=params,
            on_success=on_success,
            on_failure=on_failure,
            result_holder=result_holder,
            factory=self._factory,
            executor=self._executor,
            # Captured now, not at flush time: the dispatch belongs to the
            # caller that requested it, whatever runs the queue afterwards.
            identity=current_identity(),
        )
        add_pending_dispatch(pending.run)
        return JobHandle(
            job_id=task_id,
            queue=queue or job_type.__queue__,
            dispatched_at=datetime.now(UTC),
            _inline_result_holder=result_holder,
        )

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[Any] | None = None,
        on_any_failure: type[Any] | None = None,
    ) -> JobGroup:
        """Dispatch multiple Jobs and return a group handle.

        ``on_all_success`` and ``on_any_failure`` are accepted but ignored —
        chord semantics require a Celery-backed service.

        Args:
            jobs: Sequence of ``(job_type, payload)`` pairs.
            on_all_success: Ignored in inline mode.
            on_any_failure: Ignored in inline mode.

        Returns:
            A :class:`~loom.core.job.handle.JobGroup` with one handle per job.
        """
        _ = (
            on_all_success,
            on_any_failure,
        )  # accepted for API compat; chord semantics require Celery
        handles = tuple(self.dispatch(job_type, payload=payload) for job_type, payload in jobs)
        return JobGroup(handles=handles)
