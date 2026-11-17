from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
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
    Rejected,
    Succeeded,
)
from loom.core.job.policy import Countdown
from loom.core.job.runner import JobRunner
from loom.core.job.timer import JobDelayRefused, JobTimer, Step
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor

ResultT = TypeVar("ResultT")


@dataclass
class _PendingDispatch:
    """One inline dispatch, started by the post-commit channel once the unit of work has closed.

    A deferred or failed run starts again on the timer, as a copy of this
    dispatch holding the next delivery, until it succeeds or is rejected.
    """

    delivery: JobDelivery
    runner: JobRunner = field(repr=False)
    timer: JobTimer | None = field(repr=False)
    on_success: type[Any] | None
    on_failure: type[Any] | None
    countdown: Countdown
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

    async def start(self) -> None:
        """Run the job now, or once its countdown is over.

        Raises:
            JobDelayRefused: When the countdown is above zero and no timer waits for it.
        """
        if self.countdown.seconds == 0:
            await self.run()
        else:
            self.wait(self.countdown, self.run)

    def wait(self, delay: Countdown, step: Step) -> None:
        """Run *step* once *delay* is over.

        Args:
            delay: How long to wait.
            step: What to run afterwards.

        Raises:
            JobDelayRefused: When no timer waits for it.
        """
        if self.timer is None:
            raise JobDelayRefused(delay)
        self.timer.schedule(
            delay, step, job=self.delivery.job_type.__qualname__, task_id=self.task_id
        )

    async def run(self) -> None:
        """Run the job, then settle it, as the dispatcher, within the captured delegation."""
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
            case Deferred(delay=delay):
                error = self._again(
                    replace(self.delivery, deferrals=self.delivery.deferrals + 1), delay
                )
            case Failed(delay=delay):
                error = self._again(
                    replace(self.delivery, attempt=self.delivery.attempt + 1), delay
                )
            case Rejected():
                error = outcome.failure()
        if error is not None:
            await self._failed(error)

    def _again(self, delivery: JobDelivery, delay: Countdown) -> JobDelayRefused | None:
        """Hand *delivery* to the timer for after *delay*; the refusal when none waits."""
        try:
            self.wait(delay, replace(self, delivery=delivery).run)
        except JobDelayRefused:
            return JobDelayRefused()
        return None

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
                self._jobs.dispatch_after_commit(SendEmailJob, payload={"email": cmd.email})
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
            countdown: Delay in seconds before execution, 0 through 900
                inclusive. SQS sends it as ``DelaySeconds`` on a standard
                queue and refuses a delay on a FIFO queue. Celery sends it
                as the task countdown. Inline waits on its timer after
                the flush, and refuses it where it has none (Lambda).
            priority: Task priority override. Celery honours it. SQS refuses
                a value other than the job's. Inline ignores it.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` for result retrieval.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
        """
        ...

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
        """Send the job now, even inside a use case.

        The job can run before the caller's writes commit and is not undone
        if they roll back. Inline runs the job inside the caller's
        transaction. Work that depends on the caller's writes uses
        :meth:`dispatch_after_commit`.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked on job success.
            on_failure: Callback type invoked on job failure.
            queue: Override the job's default ``__queue__``.
            countdown: Delay in seconds before execution, 0 through 900
                inclusive. Same per-transport behaviour as
                :meth:`dispatch_after_commit`.
            priority: Task priority override. Same per-transport behaviour
                as :meth:`dispatch_after_commit`.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` for result retrieval.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
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
    ``dispatch_after_commit()`` method respects the post-commit semantics:
    the job is enqueued on the execution's post-commit channel and runs when
    the executor drains it, after the UoW has closed (or at the end of an
    execution without one). A job with a countdown, a deferral or a failed
    attempt with attempts left waits on the timer and runs again with its
    deferrals and attempt carried; a rejection ends it and calls ``on_failure``.
    Outside an execution the fallback channel holds it until
    :func:`~loom.core.job.context.flush_pending_dispatches`.

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
        timer: Waits out a countdown, a :class:`~loom.core.job.outcome.JobRetry`
            and a failure backoff. Without one a countdown is refused with
            :class:`~loom.core.job.timer.JobDelayRefused`, a ``JobRetry`` is
            rejected naming the refused delay, and a failure is rejected at
            its first attempt with its own error.

    Example::

        service = InlineJobService(factory, executor)
        handle = service.dispatch_after_commit(SendEmailJob, payload={"email": "a@b.com"})
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
        timer: JobTimer | None = None,
    ) -> None:
        self._timer = timer
        self._callbacks = callbacks
        self._factory = factory
        self._executor = executor
        base = settings or JobSettings()
        self._settings = base if timer is not None else base.without_waiting()
        self._runner = JobRunner(self._invoke)

    @property
    def settings(self) -> JobSettings:
        """The job settings in force: one attempt for every job when the service has no timer."""
        return self._settings

    async def _invoke(self, delivery: JobDelivery) -> Any:
        envelope = delivery.envelope
        return await self._executor.run(
            delivery.job_type,
            factory=self._factory,
            params=envelope.params,
            payload=envelope.payload,
            delivery=delivery,
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
        """Queue the Job for post-commit execution and return a handle.

        A zero ``countdown`` runs the job at the flush; a larger one waits on
        the timer from the flush. ``priority`` is ignored.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked after successful execution.
            on_failure: Callback type invoked after failed execution.
            queue: Overrides the job's queue for the returned handle.
            countdown: Delay in seconds, 0 through 900 inclusive.
            priority: Ignored in inline mode.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` whose ``wait()``
            returns the result once the queue is flushed and any delay is over.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
            JobDelayRefused: When *countdown* is above zero and the service has no timer.
        """
        pending, handle = self._pending(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        add_pending_dispatch(pending.start)
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
        """Run the Job now, or after its countdown, and return its handle.

        A zero ``countdown`` runs the job before returning, so the handle is
        settled unless the job waits to run again. A larger one waits on the
        timer. ``priority`` is ignored.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked after successful execution.
            on_failure: Callback type invoked after failed execution.
            queue: Overrides the job's queue for the returned handle.
            countdown: Delay in seconds, 0 through 900 inclusive.
            priority: Ignored in inline mode.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` whose ``wait()``
            returns the result of the run.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
            JobDelayRefused: When *countdown* is above zero and the service has no timer.
        """
        pending, handle = self._pending(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        await pending.start()
        return handle

    def _pending(
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
    ) -> tuple[_PendingDispatch, JobHandle[ResultT]]:
        """Build the pending run and its handle.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked after successful execution.
            on_failure: Callback type invoked after failed execution.
            queue: Overrides the job's queue for the returned handle.
            countdown: Delay in seconds, 0 through 900 inclusive.
            priority: Ignored in inline mode.

        Returns:
            The pending run and a handle sharing its outcome.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``,
                or *countdown* is outside 0..900 inclusive.
            JobDelayRefused: When *countdown* is above zero and the service has no timer.
        """
        _ = priority
        options = self._settings.of(job_type)
        delay = Countdown(options.countdown if countdown is None else countdown)
        if delay.seconds and self._timer is None:
            raise JobDelayRefused(delay)
        payload = payload or {}
        self._callbacks.refuse(payload, on_success, on_failure)
        task_id = str(uuid.uuid4())
        outcome = InlineOutcome()
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
            timer=self._timer,
            on_success=on_success,
            on_failure=on_failure,
            countdown=delay,
            outcome=outcome,
            factory=self._factory,
            captured=captured_delegation(),
        )
        handle: JobHandle[ResultT] = JobHandle(
            job_id=task_id,
            queue=queue or options.queue,
            dispatched_at=datetime.now(UTC),
            _inline_outcome=outcome,
        )
        return pending, handle

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
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
        handles = tuple(
            self.dispatch_after_commit(job_type, payload=payload) for job_type, payload in jobs
        )
        return JobGroup(handles=handles)
