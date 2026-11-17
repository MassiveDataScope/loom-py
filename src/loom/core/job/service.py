from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from loom.core.identity import (
    NO_DELEGATION,
    Acting,
    Delegation,
    captured_delegation,
    reset_identity,
    set_identity,
)
from loom.core.job.callback import (
    ANY_CALLBACKS,
    CallbackOutcome,
    DispatchableCallbacks,
    JobCallback,
    notify,
)
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_dispatch
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.handle import InlineOutcome, JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.outcome import (
    Deferred,
    Failed,
    JobOutcome,
    JobRejection,
    JobRetry,
    Rejected,
    Succeeded,
)
from loom.core.job.runner import JobRunner
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor

ResultT = TypeVar("ResultT")


@dataclass
class _PendingDispatch:
    """One inline dispatch, run by the post-commit channel once the unit of work has closed."""

    delivery: JobDelivery
    runner: JobRunner = field(repr=False)
    on_success: type[Any] | None
    on_failure: type[Any] | None
    outcome: InlineOutcome
    factory: UseCaseFactory
    captured: Delegation = NO_DELEGATION
    """The dispatcher's caller and deadline (:func:`~loom.core.identity.captured_delegation`)."""

    @property
    def task_id(self) -> str:
        """The id of the dispatch, which its handle and callbacks carry."""
        return str(self.delivery.task_id)

    @property
    def payload(self) -> dict[str, Any]:
        """The payload the job runs with."""
        return self.delivery.envelope.payload or {}

    async def run(self) -> None:
        """Run the job, then its callback, as the dispatcher, within the captured delegation."""
        resumed = Acting.current().resumed(self.captured)
        token = None if resumed is None else set_identity(resumed.caller, until=resumed.until)
        try:
            await self._settle(await self.runner.run(self.delivery))
        finally:
            if token is not None:
                reset_identity(token)

    async def _settle(self, outcome: JobOutcome) -> None:
        match outcome:
            case Succeeded(result=result):
                error = await self._succeeded(result)
            case Failed(error=error):
                pass
            case Rejected(reason=reason, error=rejection):
                error = rejection or JobRejection(reason)
            case Deferred(reason=reason):
                error = JobRetry(reason)
        if error is not None:
            await self._failed(error)

    async def _succeeded(self, result: Any) -> Exception | None:
        """Record *result* and call ``on_success``; the callback's error, when it raises."""
        self.outcome.succeed(result)
        if self.on_success is None:
            return None
        try:
            await notify(
                self.factory,
                self.on_success,
                CallbackOutcome.SUCCESS,
                job_id=self.task_id,
                result=result,
                **self.payload,
            )
        except Exception as exc:
            return exc
        return None

    async def _failed(self, error: Exception) -> None:
        self.outcome.fail(error)
        if self.on_failure is not None:
            await notify(
                self.factory,
                self.on_failure,
                CallbackOutcome.FAILURE,
                job_id=self.task_id,
                exc_type=type(error).__qualname__,
                exc_msg=str(error),
                **self.payload,
            )


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

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                or *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``.
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
    of its delegation (:func:`~loom.core.identity.captured_delegation`), exactly as
    the Celery and SQS services capture it. :meth:`run` acts as the identity
    acting, as any execution given none.

    Args:
        factory: Used to build Job and callback instances from the container.
        executor: Used to drive the compiled ExecutionPlan.
        settings: The application's job settings; the ClassVars when omitted.
        callbacks: The callbacks a dispatch may attach; any class when omitted.

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
        callbacks: DispatchableCallbacks = ANY_CALLBACKS,
    ) -> None:
        self._callbacks = callbacks
        self._factory = factory
        self._executor = executor
        self._settings = settings or JobSettings()
        self._runner = JobRunner(self._invoke)

    @property
    def runner(self) -> JobRunner:
        """The runner every dispatched job goes through."""
        return self._runner

    async def _invoke(self, delivery: JobDelivery) -> Any:
        envelope = delivery.envelope
        return await self._executor.run(
            delivery.job_type,
            factory=self._factory,
            params=envelope.params,
            payload=envelope.payload,
        )

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

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                or *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``.
        """
        _ = (countdown, priority, eta)
        payload = payload or {}
        self._callbacks.refuse(payload, on_success, on_failure)
        task_id = str(uuid.uuid4())
        outcome = InlineOutcome()
        options = self._settings.of(job_type)
        envelope = JobEnvelope(
            job=wire_job_name(job_type.__qualname__),
            payload=payload,
            params=params,
            task_id=task_id,
        )
        pending = _PendingDispatch(
            delivery=JobDelivery(
                job_type=job_type,
                envelope=envelope,
                task_id=task_id,
                attempt=1,
                deferrals=0,
                policy=options.retry.policy,
            ),
            runner=self._runner,
            on_success=on_success,
            on_failure=on_failure,
            outcome=outcome,
            factory=self._factory,
            captured=captured_delegation(),
        )
        add_pending_dispatch(pending.run)
        return JobHandle(
            job_id=task_id,
            queue=queue or options.queue,
            dispatched_at=datetime.now(UTC),
            _inline_outcome=outcome,
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
        )
        handles = tuple(self.dispatch(job_type, payload=payload) for job_type, payload in jobs)
        return JobGroup(handles=handles)
