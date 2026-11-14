from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

# RuntimeExecutor imported at runtime to avoid circular imports at module load.
# The type annotation is kept in TYPE_CHECKING only.
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from loom.core.identity import Identity, delegation, reset_identity, set_identity
from loom.core.job.callback import JobCallback, run_on_failure, run_on_success
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_dispatch
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor

ResultT = TypeVar("ResultT")


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
    captured: tuple[Identity | None, float | None] = (None, None)
    """The dispatcher's caller and deadline (:func:`~loom.core.identity.delegation`)."""

    async def run(self) -> None:
        """Run the job, then its callback, as the dispatcher, within the captured delegation.

        Where its dispatcher acts (drained by the work that dispatched it), it
        runs as is. Elsewhere it resumes the captured delegation, refused
        past its deadline when the job's execution starts; inside work acting
        as anyone else, resuming it raises
        :class:`~loom.core.identity.IdentityFixed`: it fails closed.
        """
        caller, until = self.captured
        # Compared in the captured form: delegation() maps ANONYMOUS to None.
        acting = delegation()[0]
        token = None if acting is caller or acting == caller else set_identity(caller, until=until)
        try:
            result: Any = await self.executor.run(
                self.job_type,
                factory=self.factory,
                params=self.params,
                payload=self.payload,
            )
            self.result_holder.append(result)
            if self.on_success is not None:
                await run_on_success(
                    self.factory, self.on_success, self.task_id, result, self.payload
                )
        except Exception as exc:
            if self.on_failure is not None:
                await run_on_failure(
                    self.factory,
                    self.on_failure,
                    self.task_id,
                    type(exc).__qualname__,
                    str(exc),
                    self.payload,
                )
        finally:
            if token is not None:
                reset_identity(token)


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

    A dispatched job, and its callbacks, act as
    :func:`~loom.core.identity.current_caller` at the time of the dispatch:
    the authenticated caller acting there, else no caller, with the deadline
    of its delegation (:func:`~loom.core.identity.delegation`), exactly as
    the Celery and SQS services capture it. :meth:`run` acts as the identity
    acting, as any execution given none.

    Args:
        factory: Used to build Job and callback instances from the container.
        executor: Used to drive the compiled ExecutionPlan.
        settings: The application's job settings; the ClassVars when omitted.

    Example::

        service = InlineJobService(factory, executor)
        handle = service.dispatch(SendEmailJob, payload={"email": "a@b.com"})
        await flush_pending_dispatches()  # outside an execution only
        result = handle.wait()
    """

    def __init__(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        *,
        settings: JobSettings | None = None,
    ) -> None:
        self._factory = factory
        self._executor = executor
        self._settings = settings or JobSettings()

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
        result: ResultT = await self._executor.run(
            job_type,
            factory=self._factory,
            params=params,
            payload=payload,
        )
        return result

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
            queue: Overrides the job's queue for the returned handle.
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
            captured=delegation(),
        )
        add_pending_dispatch(pending.run)
        return JobHandle(
            job_id=task_id,
            queue=queue or self._settings.of(job_type).queue,
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
