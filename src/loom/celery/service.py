"""CeleryJobService — Celery-backed implementation of the JobService protocol.

Dispatches Jobs to the broker post-commit by enqueuing a sync callable on
the post-commit channel of the current execution
(:mod:`loom.core.engine.post_commit`, through
:func:`~loom.core.job.context.add_pending_publish`).  No broker call is
made inside :meth:`~CeleryJobService.dispatch`; the actual ``send_task``
runs when the executor drains the channel after the Unit of Work has
closed, or at the end of an execution without one.

Usage::

    from loom.celery.service import CeleryJobService

    service = CeleryJobService(celery_app)

    class NotifyUsersUseCase(UseCase[User, None]):
        def __init__(self, jobs: JobService) -> None:
            self._jobs = jobs

        async def execute(self, cmd: NotifyCmd = Input()) -> None:
            self._jobs.dispatch(SendEmailJob, payload={"email": cmd.email})
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

import msgspec
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.constants import callback_task_name
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import NoMetrics
from loom.core.identity import captured_delegation
from loom.core.job.callback import CallbackOutcome, DispatchableCallbacks
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish
from loom.core.job.envelope import wire_job_name
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.tracing import get_trace_id

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.callback import JobCallback
    from loom.core.use_case.factory import UseCaseFactory

ResultT = TypeVar("ResultT")


@dataclass
class _PendingCeleryDispatch:
    """One deferred ``send_task``, run by the post-commit channel once the unit of work closed."""

    celery_app: Celery
    task_name: str
    task_id: str
    payload: dict[str, Any]
    params: dict[str, Any] | None
    trace_id: str | None
    identity: dict[str, Any] | None
    queue: str
    countdown: int
    priority: int
    eta: datetime | None
    soft_time_limit: int | None
    link: Any | None
    link_error: Any | None
    observability_runtime: ObservabilityRuntime

    def __call__(self) -> None:
        """Invoke ``send_task`` synchronously. Called by flush."""
        with self.observability_runtime.span(
            Scope.TRANSPORT,
            self.task_name,
            trace_id=self.trace_id,
            id=self.task_id,
            queue=self.queue,
            countdown=self.countdown,
            priority=self.priority,
        ):
            self.celery_app.send_task(
                self.task_name,
                kwargs={
                    "payload": self.payload,
                    "params": self.params,
                    "trace_id": self.trace_id,
                    "identity": self.identity,
                },
                task_id=self.task_id,
                queue=self.queue,
                countdown=self.countdown,
                priority=self.priority,
                eta=self.eta,
                soft_time_limit=self.soft_time_limit,
                link=self.link,
                link_error=self.link_error,
            )

    def log_fields(self) -> tuple[dict[str, object]]:
        return ({"job_name": self.task_name, "task_id": self.task_id},)


class _CallbackLink(msgspec.Struct, frozen=True):
    """The keyword arguments of a job's callback task, the dispatcher's delegation among them."""

    job_id: str
    context: dict[str, Any]
    trace_id: str | None
    identity: dict[str, Any] | None

    def signature(
        self, celery_app: Celery, callback_type: type[Any], outcome: CallbackOutcome
    ) -> Any:
        """Return the signature calling *callback_type*'s hook for *outcome* with this link.

        A success signature is mutable, so Celery passes the job's return
        value first; a failure signature is immutable.

        Args:
            celery_app: The application the signature belongs to.
            callback_type: The callback class whose task it calls.
            outcome: The outcome the job's ``link`` or ``link_error`` reports.

        Returns:
            The Celery ``Signature``.
        """
        return celery_app.signature(
            callback_task_name(callback_type, outcome),
            kwargs=msgspec.structs.asdict(self),
            immutable=outcome is CallbackOutcome.FAILURE,
        )


class CeleryJobService:
    """Celery-backed implementation of the ``JobService`` protocol.

    All dispatch calls are deferred until the current Unit of Work has
    committed and closed.  Tasks are enqueued on the execution's post-commit
    channel via :func:`~loom.core.job.context.add_pending_publish`; the
    :class:`~loom.core.engine.executor.RuntimeExecutor` drains the channel
    after the close (or at the end of an execution without a Unit of Work)
    and discards it when the execution fails.

    Callbacks are wired as native Celery ``link`` / ``link_error``
    signatures so the broker manages delivery without framework
    intervention after dispatch.

    When *factory* and *executor* are provided, :meth:`run` executes the
    job in-process (same as :class:`~loom.core.job.service.InlineJobService`)
    without touching the broker; without them it raises
    :class:`NotImplementedError`.

    Args:
        celery_app: Configured Celery application instance.
        metrics: Optional metrics adapter.  Receives a ``JOB_DISPATCHED``
            event from the web process each time a job is registered for
            deferred dispatch — before the UoW commits and the broker call
            is made.
        factory: Optional use-case factory.  Required for :meth:`run`.
        executor: Optional runtime executor.  Required for :meth:`run`.
        settings: The application's job settings; the ClassVars when omitted.
        callbacks: The callbacks a dispatch may attach; any class when omitted.

    Example::

        service = CeleryJobService(celery_app, factory=factory, executor=executor)
        handle = service.dispatch(SendEmailJob, payload={"user_id": 1})
    """

    def __init__(
        self,
        celery_app: Celery,
        metrics: MetricsAdapter | None = None,
        factory: UseCaseFactory | None = None,
        executor: RuntimeExecutor | None = None,
        observability_runtime: ObservabilityRuntime | None = None,
        *,
        settings: JobSettings | None = None,
        callbacks: DispatchableCallbacks | None = None,
    ) -> None:
        self._app = celery_app
        self._callbacks = callbacks or DispatchableCallbacks()
        self._settings = settings or JobSettings()
        self._metrics = NoMetrics() if metrics is None else metrics
        self._factory = factory
        self._executor = executor
        self._observability_runtime = observability_runtime or ObservabilityRuntime.noop()

    def close(self) -> None:
        """Close the Celery application owned by this service."""
        self._app.close()

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute a Job in-process and return its result immediately.

        Requires *factory* and *executor* to have been supplied at
        construction time.  This is the case when the service is built by
        :func:`~loom.rest.fastapi.auto.create_app`.

        Use :meth:`dispatch` for fire-and-forget broker delivery.

        Args:
            job_type: Concrete ``Job`` subclass to run.
            params: Primitive params bound to the execute signature.
            payload: Raw dict for ``Input()`` command construction.

        Returns:
            The value returned by ``Job.execute()``.

        Raises:
            NotImplementedError: When factory or executor were not supplied
                at construction.
        """
        if self._factory is None or self._executor is None:
            raise NotImplementedError(
                "CeleryJobService.run() requires factory and executor. "
                "Pass them at construction or use InlineJobService."
            )
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
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
        eta: datetime | None = None,
    ) -> JobHandle[ResultT]:
        """Register a Job for deferred dispatch to the Celery broker.

        The job is NOT sent immediately.  A :class:`_PendingCeleryDispatch`
        is added to the async context's pending queue and executed after
        the current Unit of Work commits.  If the UoW rolls back, the
        pending entry is discarded and no task reaches the broker.

        The current trace ID, the identity acting
        (:func:`~loom.core.identity.current_caller`) and the deadline of its
        delegation are captured at registration time so they are propagated
        to the Celery worker even though the send happens later.  They travel
        inside the envelope, and inside each callback link: no context
        variable crosses a broker, and the callbacks act as the dispatcher.

        Args:
            job_type: Concrete ``Job`` subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Callback type invoked by the broker on task success.
            on_failure: Callback type invoked by the broker on task failure.
            queue: Override the job's queue for this dispatch only.
            countdown: Delay in seconds before first execution.  Defaults
                to the job's countdown.
            priority: Task priority.  Defaults to the job's priority.
            eta: Absolute datetime for first execution.  Mutually exclusive
                with ``countdown`` in Celery semantics.

        Returns:
            A :class:`~loom.core.job.handle.JobHandle` whose ``job_id``
            matches the ``task_id`` that will be passed to ``send_task``.

        Raises:
            ValueError: When a callback is attached that the scan did not find,
                or *payload* holds ``job_id``, ``result``, ``exc_type`` or ``exc_msg``.
        """
        task_id = str(uuid.uuid4())
        trace_id = get_trace_id()
        identity = captured_delegation().encoded()
        payload = payload or {}
        self._callbacks.refuse(payload, on_success, on_failure)
        options = self._settings.of(job_type)
        eff_queue = queue or options.queue
        eff_countdown = countdown if countdown is not None else options.countdown
        eff_priority = priority if priority is not None else options.priority
        callback_link = _CallbackLink(task_id, payload, trace_id, identity)

        pending = _PendingCeleryDispatch(
            celery_app=self._app,
            task_name=wire_job_name(job_type.__qualname__),
            task_id=task_id,
            payload=payload,
            params=params,
            trace_id=trace_id,
            identity=identity,
            queue=eff_queue,
            countdown=eff_countdown,
            priority=eff_priority,
            eta=eta,
            soft_time_limit=options.timeout,
            link=None
            if on_success is None
            else callback_link.signature(self._app, on_success, CallbackOutcome.SUCCESS),
            link_error=None
            if on_failure is None
            else callback_link.signature(self._app, on_failure, CallbackOutcome.FAILURE),
            observability_runtime=self._observability_runtime,
        )
        add_pending_publish(pending)
        self._metrics.on_event(
            RuntimeEvent(
                kind=EventKind.JOB_DISPATCHED,
                use_case_name=job_type.__qualname__,
                trace_id=trace_id,
            )
        )

        return JobHandle(
            job_id=task_id,
            queue=eff_queue,
            dispatched_at=datetime.now(UTC),
        )

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Dispatch multiple Jobs and return a group handle.

        Each job is dispatched independently via :meth:`dispatch` and
        will be executed in parallel by the available workers once the
        Unit of Work commits.

        ``on_all_success`` and ``on_any_failure`` require native Celery
        chord support which is not yet implemented.  Passing either raises
        :class:`NotImplementedError`.  Parallel dispatch without callbacks
        works as expected.

        Args:
            jobs: Sequence of ``(job_type, payload)`` pairs.
            on_all_success: Not yet supported.  Pass ``None``.
            on_any_failure: Not yet supported.  Pass ``None``.

        Returns:
            A :class:`~loom.core.job.handle.JobGroup` with one handle
            per dispatched job.

        Raises:
            NotImplementedError: When *on_all_success* or *on_any_failure*
                is provided.  Celery chord support is reserved for a
                future release.
        """
        if on_all_success is not None or on_any_failure is not None:
            raise NotImplementedError(
                "dispatch_parallel() callbacks (on_all_success / on_any_failure) "
                "require Celery chord support, which is not yet implemented. "
                "Pass None for both callback arguments."
            )
        handles = tuple(
            self.dispatch(job_type, payload=job_payload) for job_type, job_payload in jobs
        )
        return JobGroup(handles=handles)
