"""Celery task factories for Job and Callback execution on the worker side.

Each ``_make_*`` function registers one Celery task against a given
``celery_app`` and returns the task object.  They are called once, when
:meth:`~loom.celery.runtime.CeleryRuntime.mount` registers the tasks, and
never again. Every task is registered with ``shared=False``: it belongs to
its ``celery_app`` alone.

All jobs — both sync and async ``execute()`` — are executed through the
:class:`~loom.core.engine.executor.UseCaseExecutor` via a per-process
:class:`~loom.core.async_bridge.AsyncBridge`.  This gives every job
access to the Unit of Work, injection markers (``Input()``, ``Load()``,
etc.), and the ability to dispatch further jobs post-commit.  The bridge
is initialized once per forked Celery worker process and reused across
tasks.

Async ``on_success`` / ``on_failure`` methods run on the same bridge; sync
ones run in the task thread. Either way the callback is built and called in
an execution acting as the identity its link envelope carries; a link
without one acts as no one. A process whose application failed to open
refuses every task.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from celery import Celery  # type: ignore[import-untyped]
from celery.exceptions import TaskPredicate  # type: ignore[import-untyped]

from loom.celery.constants import callback_task_name
from loom.celery.errors import WorkerNotOpenError
from loom.celery.log_events import CeleryLogField
from loom.celery.service import CallbackLink
from loom.core.async_bridge import AsyncBridge
from loom.core.execution import ExecutionScope
from loom.core.identity import Acting, current_caller
from loom.core.job.callback import CallbackOutcome, notify
from loom.core.job.config import JobSettings
from loom.core.job.envelope import JobEnvelope, job_context, wire_job_name
from loom.core.job.outcome import (
    Abandoned,
    Failed,
    JobOutcome,
    JobPostponement,
    Postponed,
    Succeeded,
)
from loom.core.job.policy import Redelivery
from loom.core.job.run import EnvelopeInvocation
from loom.core.job.runner import JobRunner
from loom.core.logger.failure import Handoff, reference
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.tracing import active_trace_id

if TYPE_CHECKING:
    from loom.core.engine.execution_observer import ExecutionObserver
    from loom.core.engine.executor import UseCaseExecutor
    from loom.core.job.job import Job
    from loom.core.job.service import JobService
    from loom.core.use_case.factory import UseCaseFactory

_log = logging.getLogger(__name__)

REDELIVERY = Redelivery.DELAYED
"""What the broker does with a job that must run again: it waits the countdown it is sent with."""


@dataclass(slots=True)
class _CeleryProcessBridge:
    """Per-process async bridge for Celery worker coroutines.

    Attributes:
        jobs: The application's job service. An eager run ends its own loop, so
            it waits until the inline jobs its task dispatched have run.
    """

    backend: str = "asyncio"
    backend_options: dict[str, Any] = field(default_factory=dict)
    shutdown_timeout_ms: int | None = None
    jobs: JobService | None = None
    _bridge: AsyncBridge | None = None
    _open_failure: Exception | None = None

    def initialize(self) -> None:
        """Create the async bridge once per forked worker process."""
        if self._bridge is not None:
            return
        self._bridge = AsyncBridge(
            backend=self.backend,
            backend_options=self.backend_options,
            shutdown_timeout_ms=self.shutdown_timeout_ms,
        )

    def refuse(self, cause: Exception) -> None:
        """Fail every later :meth:`run` of this process: its application did not open."""
        self._open_failure = cause

    def ensure_open(self) -> None:
        """Refuse a task of a process whose application failed to open.

        Raises:
            WorkerNotOpenError: When :meth:`refuse` recorded a failure to open.
        """
        failure = self._open_failure
        if failure is None:
            return
        raise WorkerNotOpenError(failure) from failure

    def run(
        self,
        coro: Any,
        *,
        timeout: float | None = None,
        eager_fallback: bool,
    ) -> Any:
        """Run *coro* through the worker bridge or fall back to asyncio.

        Raises:
            WorkerNotOpenError: When :meth:`refuse` recorded that this
                process's application failed to open; *coro* is closed unrun.
        """
        try:
            self.ensure_open()
        except WorkerNotOpenError:
            _close_unrun(coro)
            raise
        if self._bridge is not None and self._bridge.is_alive:
            return self._bridge.run(coro, timeout=timeout)

        if eager_fallback:
            return asyncio.run(self._run_eagerly(coro, timeout))

        _close_unrun(coro)
        raise RuntimeError(
            "Celery async bridge is not initialized and eager fallback is disabled. "
            "Use a Celery worker process or enable task_always_eager."
        )

    async def _run_eagerly(self, coro: Any, timeout: float | None) -> Any:
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        finally:
            if self.jobs is not None:
                await self.jobs.wait_until_idle()

    def shutdown(self) -> None:
        """Close the worker bridge if it was initialized."""
        if self._bridge is None:
            return
        self._bridge.shutdown()
        self._bridge = None


class CeleryHandoff(Handoff):
    """Where a Celery worker hands a failure to Celery, its result backend or a callback."""

    RESULT = "celery.result"
    RETRY = "celery.retry"
    FAILURE_CALLBACK = "celery.failure_callback"
    WORKER_INIT = "celery.worker_init"


def _close_unrun(coro: Any) -> None:
    """Close a coroutine that will never run, so it is not reported as never awaited."""
    close = getattr(coro, "close", None)
    if callable(close):
        close()


def _is_eager_request(task_self: Any) -> bool:
    request = getattr(task_self, "request", None)
    is_eager = getattr(request, "is_eager", None)
    if isinstance(is_eager, bool):
        return is_eager

    app = getattr(task_self, "app", None)
    conf = getattr(app, "conf", None)
    return bool(getattr(conf, "task_always_eager", False))


def _make_job_task(
    celery_app: Celery,
    job_type: type[Job[Any]],
    factory: UseCaseFactory,
    executor: UseCaseExecutor,
    process_bridge: _CeleryProcessBridge,
    execution_observer: ExecutionObserver | None = None,
    observability: Observability | None = None,
    *,
    settings: JobSettings,
) -> Any:
    """Register and return a Celery task that executes *job_type* on the worker.

    The returned task is bound (``bind=True``) so it can access retry
    context via ``self``.  Each run goes through a
    :class:`~loom.core.job.runner.JobRunner`, which decides whether it
    succeeded, postpones, fails or is abandoned; the task only maps that outcome
    to Celery: a failure retries after the policy's backoff, a postponement
    retries after its delay without spending an attempt (the task carries the
    count in its ``postponements`` argument), and an abandonment raises its error, which
    the worker stores as the failure and acknowledges (``acks_late``) without a retry.
    Any error that ends the task (one the runner abandoned, a worker that could
    not open its application, a soft time limit) first sends the failure callback
    task named in its ``failure_callback`` argument, with the error's class and
    text; a ``Retry``, ``Reject`` or ``Ignore`` sends nothing. The callback is
    delivered at least once, as a worker that dies between the send and the
    acknowledgement sends it again, and a hard kill sends none.

    Both sync and async ``execute()`` methods are driven by the
    :class:`~loom.core.engine.executor.UseCaseExecutor` via a shared
    :class:`~loom.core.async_bridge.AsyncBridge`, giving every job access
    to the full framework (UoW, injection markers, dispatch).  The executor
    owns the lifecycle: it opens the unit of work, commits and drains the
    dispatches queued during the execution once it has closed.  The identity
    decoded from the envelope is forwarded, so a job declaring ``Caller()``
    fails closed rather than running as an unknown caller.

    A :class:`~loom.core.errors.PostCommitError` whose
    transaction committed is terminal: the task is not retried, since the
    write already stands.  One raised without a commit is retried like any
    other failure.

    Args:
        celery_app: Celery application to register the task on.
        job_type: Concrete :class:`~loom.core.job.job.Job` subclass.
        factory: Used to build the Job instance via DI.
        executor: UseCaseExecutor driving the compiled ExecutionPlan.
        execution_observer: Optional execution observer.  Receives the job events the
            runner emits (``JOB_STARTED``, ``JOB_SUCCEEDED``,
            ``JOB_POSTPONED``, ``JOB_FAILED`` and ``JOB_ABANDONED``).
        settings: The application's job settings, which give the task its
            retry policy and time limit.

    Returns:
        The registered Celery task object.
    """
    options = settings.options_for(job_type)
    timeout = options.timeout
    runtime = observability or Observability.noop()
    run_timeout = float(timeout) if timeout is not None and timeout > 0 else None
    name = job_type.__qualname__
    runner = JobRunner(
        EnvelopeInvocation(factory, executor, runtime),
        redelivery=REDELIVERY,
        execution_observer=execution_observer,
    )

    def _deliver(
        *,
        task_self: Any,
        payload: dict[str, Any] | None,
        params: dict[str, Any] | None,
        identity: dict[str, Any] | None,
        trace_id: str | None,
        postponements: int,
    ) -> JobOutcome:
        """Run one delivery of the job through the runner."""
        task_id = getattr(getattr(task_self, "request", None), "id", None)
        delivery = options.delivery_for(
            job_type,
            JobEnvelope(
                job=name,
                payload=payload,
                params=params,
                trace_id=trace_id,
                identity=identity,
                task_id=task_id,
            ),
            attempt=task_self.request.retries - postponements + 1,
            postponements=postponements,
            timeout_s=run_timeout,
        )
        outcome: JobOutcome = process_bridge.run(
            runner.run(delivery), eager_fallback=_is_eager_request(task_self)
        )
        return outcome

    def _settle(task_self: Any, outcome: JobOutcome, postponements: int) -> Any:
        """Map the outcome to Celery: return the result, retry, or raise the abandonment's error.

        Args:
            task_self: The bound Celery task.
            outcome: What the runner decided for this delivery.
            postponements: Postponements already spent, carried to the next delivery.

        Returns:
            The job's result when it succeeded.

        Raises:
            Retry: When the job failed with attempts left or was postponed; its
                ``exc`` is the failure's reference, which Celery stores.
            FailureReference: In place of the error that ended an abandoned job,
                which the worker stores as the task's failure before
                acknowledging the message.
        """
        match outcome:
            case Succeeded(result=result):
                return result
            case Failed(error=error, delay=delay):
                raise task_self.retry(exc=reference(error), countdown=delay.seconds)
            case Postponed(delay=delay) as postponed:
                kwargs = {**(task_self.request.kwargs or {}), "postponements": postponements + 1}
                raise task_self.retry(
                    exc=reference(JobPostponement(postponed.reason, delay.seconds)),
                    countdown=delay.seconds,
                    kwargs=kwargs,
                )
            case Abandoned() as abandoned:
                raise reference(abandoned.as_exception())

    @celery_app.task(  # type: ignore[untyped-decorator]
        name=wire_job_name(job_type.__qualname__),
        shared=False,
        bind=True,
        acks_late=True,
        acks_on_failure_or_timeout=True,
        reject_on_worker_lost=True,
        max_retries=None,
        soft_time_limit=timeout,
    )
    def _job_task(
        self: Any,
        *,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        trace_id: str | None = None,
        identity: dict[str, Any] | None = None,
        postponements: int = 0,
        failure_callback: str | None = None,
    ) -> Any:
        try:
            with active_trace_id(trace_id or None):
                outcome = _deliver(
                    task_self=self,
                    payload=payload,
                    params=params,
                    identity=identity,
                    trace_id=trace_id,
                    postponements=postponements,
                )
                return _settle(self, outcome, postponements)
        except TaskPredicate:
            raise
        except Exception as error:
            failure = reference(error)
        if failure_callback is not None:
            CallbackLink(self.request.id, payload or {}, trace_id, identity).send_failure(
                celery_app, failure_callback, failure
            )
        raise failure

    return _job_task


def _make_callback_task(
    celery_app: Celery,
    callback_type: type[Any],
    outcome: CallbackOutcome,
    factory: UseCaseFactory,
    process_bridge: _CeleryProcessBridge,
    observability: Observability | None = None,
) -> Any:
    """Register and return the Celery task calling *callback_type*'s hook for *outcome*.

    A success link is mutable: Celery passes the job's return value as the
    first positional argument. A failure callback is sent by the worker on every
    terminal failure of the job task, with the error's ``exc_type`` and ``exc_msg`` as keyword
    arguments, so no result backend is read. The link's ``job_id`` and
    ``context`` arrive as keyword arguments.

    The link's trace and caller are installed where the hook runs: an async
    hook in the coroutine the async bridge runs, through
    :func:`~loom.core.job.callback.notify`; a sync one in the task's
    thread, built and called in an execution acting as that caller.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        outcome: The outcome whose hook the task calls.
        factory: Used to build the callback instance via DI.
        process_bridge: Runs an async hook.
        observability: Opens the callback's span.

    Returns:
        The registered Celery task object.
    """
    runtime = observability or Observability.noop()
    name = callback_type.__qualname__
    hook_is_async = inspect.iscoroutinefunction(outcome.hook(callback_type))

    def details(job_result: tuple[Any, ...], exc_type: str, exc_msg: str) -> dict[str, Any]:
        if outcome is CallbackOutcome.SUCCESS:
            return {"result": job_result[0]}
        Acting.current().refuse_expired()
        return {"exc_type": exc_type, "exc_msg": exc_msg}

    async def notified(
        job_id: str,
        job_result: tuple[Any, ...],
        trace_id: str | None,
        identity: dict[str, Any] | None,
        context: dict[str, Any],
        exc_type: str,
        exc_msg: str,
    ) -> None:
        with (
            job_context(trace_id, identity),
            runtime.span(
                Scope.JOB,
                name,
                trace_id=trace_id,
                id=job_id,
                attributes={CeleryLogField.CALLBACK: outcome.value},
            ),
        ):
            await notify(
                factory,
                callback_type,
                outcome,
                **{**context, "job_id": job_id, **details(job_result, exc_type, exc_msg)},
            )

    @celery_app.task(  # type: ignore[untyped-decorator]
        name=callback_task_name(callback_type, outcome), shared=False, bind=True
    )
    def _callback_task(
        self: Any,
        *job_result: Any,
        job_id: str,
        context: dict[str, Any],
        trace_id: str | None = None,
        identity: dict[str, Any] | None = None,
        exc_type: str = "Unknown",
        exc_msg: str = "",
    ) -> None:
        process_bridge.ensure_open()
        if hook_is_async:
            process_bridge.run(
                notified(job_id, job_result, trace_id, identity, context, exc_type, exc_msg),
                eager_fallback=_is_eager_request(self),
            )
            return
        with (
            job_context(trace_id, identity),
            runtime.span(
                Scope.JOB,
                name,
                trace_id=trace_id,
                id=job_id,
                attributes={CeleryLogField.CALLBACK: outcome.value},
            ),
        ):
            arguments = {
                **context,
                "job_id": job_id,
                **details(job_result, exc_type, exc_msg),
            }
            with ExecutionScope(current_caller(), what=name):
                reply = outcome.hook(factory.build(callback_type))(**arguments)
                if reply is not None and inspect.isawaitable(reply):
                    _close_unrun(reply)
                    raise TypeError(
                        f"{name}.on_{outcome} returned an awaitable from a plain def: "
                        "declare it async def"
                    )

    return _callback_task
