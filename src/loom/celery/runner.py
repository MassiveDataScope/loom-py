"""Celery task factories for Job and Callback execution on the worker side.

Each ``_make_*`` function registers one Celery task against a given
``celery_app`` and returns the task object.  They are called once, when
:meth:`~loom.celery.adapter.CeleryAdapter.bind` registers the tasks, and
never again.

Every task is registered with ``shared=False``: it belongs to its
``celery_app`` alone. A shared task joins Celery's process-wide finalizers,
so a second application finalized in the same process would also receive
it, and the first task registered under a name wins — the second
application would serve a task bound to the first one's runtime.

All jobs — both sync and async ``execute()`` — are executed through the
:class:`~loom.core.engine.executor.RuntimeExecutor` via a per-process
:class:`~loom.core.async_bridge.AsyncBridge`.  This gives every job
access to the Unit of Work, injection markers (``Input()``, ``Load()``,
etc.), and the ability to dispatch further jobs post-commit.  The bridge
is initialized once per forked Celery worker process and reused across
tasks.

Callbacks follow the same pattern: async ``on_success`` / ``on_failure``
methods are submitted to the shared :class:`~loom.core.async_bridge.AsyncBridge`;
sync methods are called directly from the task thread. Every task body first
calls ``ensure_open``, so a process whose application failed to open refuses
sync and async work alike.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from celery import Celery  # type: ignore[import-untyped]
from celery.result import AsyncResult  # type: ignore[import-untyped]

from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX
from loom.celery.errors import WorkerNotOpenError
from loom.core.async_bridge import AsyncBridge
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.job.config import JobSettings
from loom.core.job.envelope import JobEnvelope, job_context, wire_job_name
from loom.core.job.outcome import failure_log_fields, is_retryable
from loom.core.job.run import run_job
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job
    from loom.core.use_case.factory import UseCaseFactory

_log = get_logger(__name__)


@dataclass(slots=True)
class _CeleryAsyncRuntime:
    """Per-process async bridge for Celery worker coroutines."""

    backend: str = "asyncio"
    backend_options: dict[str, Any] = field(default_factory=dict)
    shutdown_timeout_ms: int | None = None
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

        Every task body calls it first: a sync callback never reaches
        :meth:`run`, so the gate there alone would let it run.

        Raises:
            WorkerNotOpenError: When :meth:`refuse` recorded a failure to open.
        """
        failure = self._open_failure
        if failure is None:
            return
        # S5707 false positive: refuse() only ever records an Exception.
        raise WorkerNotOpenError(failure) from failure  # NOSONAR S5707

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
            if timeout is None:
                return asyncio.run(coro)

            async def _with_timeout() -> Any:
                return await asyncio.wait_for(coro, timeout=timeout)

            return asyncio.run(_with_timeout())

        _close_unrun(coro)
        raise RuntimeError(
            "Celery async bridge is not initialized and eager fallback is disabled. "
            "Use a Celery worker process or enable task_always_eager."
        )

    def shutdown(self) -> None:
        """Close the worker bridge if it was initialized."""
        if self._bridge is None:
            return
        self._bridge.shutdown()
        self._bridge = None


def _close_unrun(coro: Any) -> None:
    """Close a coroutine that will never run, so it is not reported as never awaited."""
    close = getattr(coro, "close", None)
    if callable(close):
        close()


# ---------------------------------------------------------------------------
# Metrics helpers
# ---------------------------------------------------------------------------


def _emit(metrics: MetricsAdapter | None, event: RuntimeEvent) -> None:
    """Forward *event* to *metrics* when an adapter is configured."""
    if metrics is not None:
        metrics.on_event(event)


def _job_event(
    kind: EventKind,
    use_case_name: str,
    trace_id: str | None,
    **kwargs: Any,
) -> RuntimeEvent:
    """Build a :class:`~loom.core.engine.events.RuntimeEvent` for a job lifecycle step."""
    return RuntimeEvent(kind=kind, use_case_name=use_case_name, trace_id=trace_id, **kwargs)


def _exhausted_event(
    use_case_name: str, trace_id: str | None, started: float, error: Exception
) -> RuntimeEvent:
    """Build the terminal ``JOB_EXHAUSTED`` event for a job that will not run again."""
    return _job_event(
        EventKind.JOB_EXHAUSTED,
        use_case_name,
        trace_id,
        duration_ms=(time.monotonic() - started) * 1000,
        status="exhausted",
        error=error,
    )


def _is_eager_request(task_self: Any) -> bool:
    request = getattr(task_self, "request", None)
    is_eager = getattr(request, "is_eager", None)
    if isinstance(is_eager, bool):
        return is_eager

    app = getattr(task_self, "app", None)
    conf = getattr(app, "conf", None)
    return bool(getattr(conf, "task_always_eager", False))


# ---------------------------------------------------------------------------
# Job runner factory
# ---------------------------------------------------------------------------


def _log_post_commit_failure(exc: Exception, job: str) -> None:
    """Record which post-commit actions failed and whether anything committed."""
    fields = failure_log_fields(exc)
    if fields:
        _log.error("JobPostCommitFailed", job=job, **fields)


def _make_job_task(
    celery_app: Celery,
    job_type: type[Job[Any]],
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    async_runtime: _CeleryAsyncRuntime,
    metrics: MetricsAdapter | None = None,
    observability_runtime: ObservabilityRuntime | None = None,
    backoff: int = 2,
    settings: JobSettings | None = None,
) -> Any:
    """Register and return a Celery task that executes *job_type* on the worker.

    The returned task is bound (``bind=True``) so it can access retry
    context via ``self``.  Exponential back-off uses ``backoff ** retries``
    seconds (default ``2 ** 0 = 1``, ``2 ** 1 = 2``, …).

    Both sync and async ``execute()`` methods are driven by the
    :class:`~loom.core.engine.executor.RuntimeExecutor` via a shared
    :class:`~loom.core.async_bridge.AsyncBridge`, giving every job access
    to the full framework (UoW, injection markers, dispatch).  The executor
    owns the lifecycle: it opens the unit of work, commits and drains the
    dispatches queued during the execution once it has closed.  The identity
    decoded from the envelope is forwarded, so a job declaring ``Caller()``
    fails closed rather than running as an unknown caller.

    A :class:`~loom.core.engine.post_commit.PostCommitError` whose
    transaction committed is terminal: the task is not retried, since the
    write already stands.  One raised without a commit is retried like any
    other failure.

    Args:
        celery_app: Celery application to register the task on.
        job_type: Concrete :class:`~loom.core.job.job.Job` subclass.
        factory: Used to build the Job instance via DI.
        executor: RuntimeExecutor driving the compiled ExecutionPlan.
        metrics: Optional metrics adapter.  Emits ``JOB_STARTED``,
            ``JOB_SUCCEEDED``, ``JOB_RETRYING``, and ``JOB_EXHAUSTED``
            events during task execution in the worker process.
        backoff: Base for exponential retry delay (seconds).  Defaults to 2.
        settings: The application's job settings, which give the task its
            retries and time limit; the ClassVars when omitted.

    Returns:
        The registered Celery task object.
    """
    settings = settings or JobSettings()
    timeout_value = settings.timeout(job_type)
    runtime = observability_runtime or ObservabilityRuntime.noop()
    run_timeout = float(timeout_value) if timeout_value is not None and timeout_value > 0 else None
    name = job_type.__qualname__

    def _execute_job(
        *,
        task_self: Any,
        payload: dict[str, Any] | None,
        params: dict[str, Any] | None,
        identity: dict[str, Any] | None,
        trace_id: str | None,
    ) -> Any:
        """Build the job instance and run it under the job span."""
        # Inside the task's retry handling: a retry may land on a healthy process.
        async_runtime.ensure_open()
        return async_runtime.run(
            run_job(
                JobEnvelope(
                    job=name,
                    payload=payload,
                    params=params,
                    trace_id=trace_id,
                    identity=identity,
                ),
                job_type,
                factory=factory,
                executor=executor,
                observability=runtime,
                message_id=getattr(getattr(task_self, "request", None), "id", None),
            ),
            timeout=run_timeout,
            eager_fallback=_is_eager_request(task_self),
        )

    def _handle_job_failure(
        *, task_self: Any, exc: Exception, trace_id: str | None, started: float
    ) -> NoReturn:
        """Retry the task when the failure allows it, otherwise let it die."""
        _log_post_commit_failure(exc, name)
        if is_retryable(exc) and task_self.request.retries < task_self.max_retries:
            countdown = backoff**task_self.request.retries
            _emit(
                metrics,
                _job_event(EventKind.JOB_RETRYING, name, trace_id, status="retrying", error=exc),
            )
            raise task_self.retry(exc=exc, countdown=countdown) from exc
        _emit(metrics, _exhausted_event(name, trace_id, started, exc))
        raise exc

    @celery_app.task(  # type: ignore[untyped-decorator]
        name=wire_job_name(job_type.__qualname__),
        shared=False,
        bind=True,
        acks_late=True,
        reject_on_worker_lost=True,
        max_retries=settings.retries(job_type),
        soft_time_limit=timeout_value,
    )
    def _job_task(
        self: Any,
        *,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        trace_id: str | None = None,
        identity: dict[str, Any] | None = None,
    ) -> Any:
        with job_context(trace_id, identity):
            _emit(metrics, _job_event(EventKind.JOB_STARTED, name, trace_id))
            t0 = time.monotonic()
            try:
                result = _execute_job(
                    task_self=self,
                    payload=payload,
                    params=params,
                    identity=identity,
                    trace_id=trace_id,
                )
            except Exception as exc:
                _handle_job_failure(task_self=self, exc=exc, trace_id=trace_id, started=t0)
            _emit(
                metrics,
                _job_event(
                    EventKind.JOB_SUCCEEDED,
                    name,
                    trace_id,
                    duration_ms=(time.monotonic() - t0) * 1000,
                    status="success",
                ),
            )
            return result

    return _job_task


# ---------------------------------------------------------------------------
# Callback runner factories
# ---------------------------------------------------------------------------


def _make_callback_task(
    celery_app: Celery,
    callback_type: type[Any],
    factory: UseCaseFactory,
    async_runtime: _CeleryAsyncRuntime,
    observability_runtime: ObservabilityRuntime | None = None,
) -> Any:
    """Register and return a Celery task for the ``on_success`` callback.

    Celery passes the parent job's return value as the first positional
    argument (``result``) because the link signature uses ``immutable=False``.
    The callback receives ``job_id`` and ``context`` via kwargs.

    Async ``on_success`` methods are submitted to the shared async bridge.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        factory: Used to build the callback instance via DI.

    Returns:
        The registered Celery task object.
    """
    _on_success_is_async = inspect.iscoroutinefunction(callback_type.on_success)
    runtime = observability_runtime or ObservabilityRuntime.noop()

    @celery_app.task(  # type: ignore[untyped-decorator]
        name=f"{TASK_CALLBACK_PREFIX}.{callback_type.__qualname__}", shared=False
    )
    def _callback_task(
        result: Any,
        *,
        job_id: str,
        context: dict[str, Any],
        trace_id: str | None = None,
    ) -> None:
        async_runtime.ensure_open()
        with (
            job_context(trace_id, None),
            runtime.span(
                Scope.JOB,
                callback_type.__qualname__,
                trace_id=trace_id,
                id=job_id,
                callback="success",
            ),
        ):
            cb = factory.build(callback_type)
            if _on_success_is_async:
                async_runtime.run(
                    cb.on_success(job_id=job_id, result=result, **context),
                    eager_fallback=bool(getattr(celery_app.conf, "task_always_eager", False)),
                )
            else:
                cb.on_success(job_id=job_id, result=result, **context)

    return _callback_task


def _resolve_error_info(job_id: str, app: Celery) -> tuple[str, str]:
    """Look up the exception that caused a job to fail via the result backend.

    Falls back to empty strings if the backend does not hold the result or
    the stored value is not an exception (e.g. backend not configured).

    Args:
        job_id: Celery task UUID of the *failed* job.
        app: The worker application holding the result backend.

    Returns:
        A ``(exc_type, exc_msg)`` tuple ready to forward to ``on_failure``.
    """
    exc = AsyncResult(job_id, app=app).result
    if isinstance(exc, Exception):
        return type(exc).__qualname__, str(exc)
    return "Unknown", ""


def _make_callback_error_task(
    celery_app: Celery,
    callback_type: type[Any],
    factory: UseCaseFactory,
    async_runtime: _CeleryAsyncRuntime,
    observability_runtime: ObservabilityRuntime | None = None,
) -> Any:
    """Register and return a Celery task for the ``on_failure`` callback.

    The link was registered with ``immutable=True`` so Celery does not
    prepend additional positional arguments.  Exception details are
    retrieved from the result backend via :func:`_resolve_error_info`; a
    configured backend is therefore required for full ``exc_type`` /
    ``exc_msg`` propagation.

    Async ``on_failure`` methods are submitted to the shared async bridge.

    Args:
        celery_app: Celery application to register the task on.
        callback_type: Concrete :class:`~loom.core.job.callback.JobCallback`
            subclass.
        factory: Used to build the callback instance via DI.

    Returns:
        The registered Celery task object.
    """
    _on_failure_is_async = inspect.iscoroutinefunction(callback_type.on_failure)
    runtime = observability_runtime or ObservabilityRuntime.noop()

    @celery_app.task(  # type: ignore[untyped-decorator]
        name=f"{TASK_CALLBACK_ERROR_PREFIX}.{callback_type.__qualname__}", shared=False
    )
    def _callback_error_task(
        *,
        job_id: str,
        context: dict[str, Any],
        trace_id: str | None = None,
    ) -> None:
        async_runtime.ensure_open()
        with (
            job_context(trace_id, None),
            runtime.span(
                Scope.JOB,
                callback_type.__qualname__,
                trace_id=trace_id,
                id=job_id,
                callback="failure",
            ),
        ):
            exc_type, exc_msg = _resolve_error_info(job_id, celery_app)
            cb = factory.build(callback_type)
            if _on_failure_is_async:
                async_runtime.run(
                    cb.on_failure(
                        job_id=job_id,
                        exc_type=exc_type,
                        exc_msg=exc_msg,
                        **context,
                    ),
                    eager_fallback=bool(getattr(celery_app.conf, "task_always_eager", False)),
                )
            else:
                cb.on_failure(job_id=job_id, exc_type=exc_type, exc_msg=exc_msg, **context)

    return _callback_error_task
