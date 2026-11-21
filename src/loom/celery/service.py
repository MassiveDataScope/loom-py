"""CeleryJobTransport: the :class:`~loom.core.job.transport.JobTransport` for a Celery broker.

:meth:`~CeleryJobTransport.send_after_commit` enqueues a sync callable on the
post-commit channel of the current execution (:mod:`loom.core.engine.post_commit`,
through :func:`~loom.core.job.context.add_pending_publish`).  No broker call is
made there; the ``send_task`` runs when the executor drains the channel after the
Unit of Work has closed, or at the end of an execution without one.
:meth:`~CeleryJobTransport.send` publishes at once, off the event loop.

Usage::

    from loom.celery.service import CeleryJobTransport

    service = JobService(CeleryJobTransport(celery_app))

    class NotifyUsersUseCase(UseCase[User, None]):
        def __init__(self, jobs: JobService) -> None:
            self._jobs = jobs

        async def execute(self, cmd: NotifyCmd = Input()) -> None:
            self._jobs.dispatch_after_commit(SendEmailJob, payload={"email": cmd.email})
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

import msgspec
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.constants import callback_task_name
from loom.core.engine.events import EventKind, ExecutionEvent
from loom.core.engine.execution_observer import NullExecutionObserver
from loom.core.job.callback import CallbackOutcome
from loom.core.job.context import add_pending_publish
from loom.core.job.dispatch import JobDispatch
from loom.core.job.handle import JobHandle
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability

if TYPE_CHECKING:
    from loom.core.engine.execution_observer import ExecutionObserver
    from loom.core.job.callback import JobCallback

ResultT = TypeVar("ResultT")
_log = logging.getLogger(__name__)


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
    soft_time_limit: int | None
    link: Any | None
    failure_callback: str | None
    observability: Observability

    def __call__(self) -> None:
        """Invoke ``send_task`` synchronously. Called by flush."""
        with self.observability.span(
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
                    "failure_callback": self.failure_callback,
                },
                task_id=self.task_id,
                queue=self.queue,
                countdown=self.countdown,
                priority=self.priority,
                soft_time_limit=self.soft_time_limit,
                link=self.link,
            )

    def log_fields(self) -> tuple[dict[str, object]]:
        return ({"job_name": self.task_name, "task_id": self.task_id},)


class CallbackLink(msgspec.Struct, frozen=True):
    """The keyword arguments of a job's callback task, the dispatcher's delegation among them."""

    job_id: str
    context: dict[str, Any]
    trace_id: str | None
    identity: dict[str, Any] | None

    def success_signature(self, celery_app: Celery, callback_type: type[Any]) -> Any:
        """Return the signature calling *callback_type*'s ``on_success`` with this link.

        The signature is mutable, so Celery passes the job's return value first.

        Args:
            celery_app: The application the signature belongs to.
            callback_type: The callback class whose task it calls.

        Returns:
            The Celery ``Signature``, for the job's ``link``.
        """
        return celery_app.signature(
            callback_task_name(callback_type, CallbackOutcome.SUCCESS),
            kwargs=msgspec.structs.asdict(self),
        )

    def send_failure(self, celery_app: Celery, task_name: str, error: Exception) -> None:
        """Send the failure callback task *task_name* with this link and the error's class and text.

        The callback runs at least once: a worker that dies between this send
        and acknowledging the job's message sends it again. A send that fails
        is logged and never raised, so the caller still raises the job's own error.

        Args:
            celery_app: The application the callback task belongs to.
            task_name: The name of the callback task (:func:`callback_task_name`).
            error: The error that ended the job, as the worker raised it.
        """
        try:
            celery_app.send_task(
                task_name,
                kwargs={
                    **msgspec.structs.asdict(self),
                    "exc_type": type(error).__qualname__,
                    "exc_msg": str(error),
                },
            )
        except Exception:
            _log.exception(
                "Failure callback not sent", extra={"job_id": self.job_id, "task": task_name}
            )


class CeleryJobTransport:
    """Send validated jobs to a Celery broker on either dispatch channel.

    Args:
        celery_app: Broker application owned by this transport.
        execution_observer: Receives dispatch events.
        observability: Traces each broker send.
    """

    def __init__(
        self,
        celery_app: Celery,
        execution_observer: ExecutionObserver | None = None,
        observability: Observability | None = None,
    ) -> None:
        self._app = celery_app
        self._execution_observer = (
            NullExecutionObserver() if execution_observer is None else execution_observer
        )
        self._observability = observability or Observability.noop()

    def close(self) -> None:
        """Close this transport's Celery application."""
        self._app.close()

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Queue the broker send for the successful post-commit flush."""
        pending, handle = self._build_pending(dispatch)
        add_pending_publish(pending)
        return handle

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Send the task to the broker before returning, without blocking the event loop.

        The blocking ``send_task`` runs in a worker thread with a copy of the
        caller's context. A thread cannot be interrupted: when the awaiting task
        is cancelled, the send that already started still finishes and the job is
        published, though no handle reaches the caller.
        """
        pending, handle = self._build_pending(dispatch)
        await asyncio.to_thread(pending)
        return handle

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Accept callbacks, whose discovery is checked by the service."""
        return None

    def _build_pending(
        self, dispatch: JobDispatch
    ) -> tuple[_PendingCeleryDispatch, JobHandle[Any]]:
        envelope = dispatch.envelope
        task_id = str(envelope.task_id)
        link = CallbackLink(task_id, envelope.payload or {}, envelope.trace_id, envelope.identity)
        pending = _PendingCeleryDispatch(
            celery_app=self._app,
            task_name=envelope.job,
            task_id=task_id,
            payload=envelope.payload or {},
            params=envelope.params,
            trace_id=envelope.trace_id,
            identity=envelope.identity,
            queue=dispatch.queue,
            countdown=dispatch.countdown.seconds,
            priority=dispatch.priority,
            soft_time_limit=dispatch.options.timeout,
            link=None
            if dispatch.on_success is None
            else link.success_signature(self._app, dispatch.on_success),
            failure_callback=None
            if dispatch.on_failure is None
            else callback_task_name(dispatch.on_failure, CallbackOutcome.FAILURE),
            observability=self._observability,
        )
        self._execution_observer.on_event(
            ExecutionEvent(
                kind=EventKind.JOB_DISPATCHED,
                use_case_name=dispatch.job_type.__qualname__,
                trace_id=envelope.trace_id,
            )
        )
        return pending, JobHandle(
            job_id=task_id, queue=dispatch.queue, dispatched_at=datetime.now(UTC)
        )
