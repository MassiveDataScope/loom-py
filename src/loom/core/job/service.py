"""Public job dispatch and the inline transport."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from loom.core.errors import MixedJobTransportsError
from loom.core.identity import Acting, reset_identity, set_identity
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
from loom.core.job.dispatch import JobDispatch
from loom.core.job.handle import InlineOutcome, JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.outcome import Abandoned, Failed, JobOutcome, Postponed, Succeeded
from loom.core.job.policy import Countdown, Redelivery
from loom.core.job.runner import JobRunner
from loom.core.job.timer import JobDelayRefused, JobTimer
from loom.core.use_case.factory import UseCaseFactory

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter

ResultT = TypeVar("ResultT")


class JobTransport(Protocol):
    """Send a validated dispatch on the immediate or post-commit channel."""

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Schedule the send for after the current unit of work commits."""
        ...

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Send now, even when the caller's unit of work remains open."""
        ...

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Refuse callbacks that the transport cannot carry before discovery checks."""
        ...

    def check_parallel_callbacks(
        self, on_all_success: type[JobCallback] | None, on_any_failure: type[JobCallback] | None
    ) -> None:
        """Refuse group callbacks the transport cannot deliver."""
        ...


class JobService:
    """Run jobs in process and dispatch them through the transport of their effective queue.

    A dispatch is validated once, before its transport schedules any work.
    Unrouted queues use the default transport.

    Args:
        factory: Builds jobs for an in-process run.
        executor: Runs an in-process job.
        default: Transport serving queues without an explicit route.
        routes: Transport selected by each routed queue name.
        settings: Resolved options of this application's jobs.
        callbacks: Callback classes discovered for this application.
    """

    def __init__(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        default: JobTransport,
        *,
        routes: Mapping[str, JobTransport] | None = None,
        settings: JobSettings | None = None,
        callbacks: DispatchableCallbacks = ANY_CALLBACKS,
    ) -> None:
        self._factory = factory
        self._executor = executor
        self._default = default
        self._routes = dict(routes or {})
        self._settings = settings or JobSettings()
        self._callbacks = callbacks

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        """Execute a job immediately in process and return its result.

        Raises:
            NotImplementedError: The service was built without an in-process executor.
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
            job_type, params=params, payload=payload, on_success=on_success,
            on_failure=on_failure, queue=queue, countdown=countdown, priority=priority,
        )
        return self._transport_for(dispatch.queue).send_after_commit(dispatch)

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
            job_type, params=params, payload=payload, on_success=on_success,
            on_failure=on_failure, queue=queue, countdown=countdown, priority=priority,
        )
        return await self._transport_for(dispatch.queue).send(dispatch)

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        """Schedule each job after commit and return handles in input order.

        Raises:
            MixedJobTransportsError: Group callbacks span more than one transport.
            NotImplementedError: A transport cannot deliver group callbacks.
        """
        dispatches = [self._build_dispatch(job, payload=body) for job, body in jobs]
        transports = [self._transport_for(dispatch.queue) for dispatch in dispatches]
        first = transports[0] if transports else self._default
        same_transport = all(transport is first for transport in transports)
        if (on_all_success is not None or on_any_failure is not None) and not same_transport:
            raise MixedJobTransportsError()
        if same_transport:
            first.check_parallel_callbacks(on_all_success, on_any_failure)
        return JobGroup(
            handles=tuple(
                transport.send_after_commit(dispatch)
                for transport, dispatch in zip(transports, dispatches, strict=True)
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
        options = self._settings.of(job_type)
        self._transport_for(queue or options.queue).check_callbacks(on_success, on_failure)
        return JobDispatch.build(
            job_type, options=options, callbacks=self._callbacks,
            params=params, payload=payload, on_success=on_success,
            on_failure=on_failure, queue=queue, countdown=countdown, priority=priority,
        )

    def _transport_for(self, queue: str) -> JobTransport:
        return self._routes.get(queue, self._default)


@dataclass(frozen=True, slots=True)
class _PendingDispatch:
    """The data of one inline run held until its post-commit flush or timer step."""

    dispatch: JobDispatch
    delivery: JobDelivery
    outcome: InlineOutcome


class InlineJobTransport:
    """Run dispatched jobs in process, optionally waiting on an injected timer.

    Args:
        factory: Builds job and callback instances.
        executor: Runs compiled execution plans.
        timer: Schedules countdowns, postponements and retries; without it
            any initial delay is refused and later waiting is abandoned.
        metrics: Receives the runner's job events.
    """

    def __init__(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        *,
        timer: JobTimer | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._factory = factory
        self._executor = executor
        self._timer = timer
        self._redelivery = Redelivery.DELAYED if timer is not None else Redelivery.NONE
        self._runner = JobRunner(self._invoke, redelivery=self._redelivery, metrics=metrics)

    @property
    def redelivery(self) -> Redelivery:
        """The inline runner's ability to wait and run again."""
        return self._redelivery

    async def _invoke(self, delivery: JobDelivery) -> Any:
        envelope = delivery.envelope
        return await self._executor.run(
            delivery.job_type, factory=self._factory, params=envelope.params,
            payload=envelope.payload, delivery=delivery,
        )

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Schedule the inline run on the post-commit channel."""
        pending, handle = self._build_pending(dispatch)
        add_pending_dispatch(lambda: self._start(pending))
        return handle

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Start the inline run before returning its handle."""
        pending, handle = self._build_pending(dispatch)
        await self._start(pending)
        return handle

    def check_parallel_callbacks(
        self, on_all_success: type[JobCallback] | None, on_any_failure: type[JobCallback] | None
    ) -> None:
        """Accept group callbacks without invoking them, as inline dispatch has done."""
        return None

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Accept callbacks, whose discovery is checked by the service."""
        return None

    def _build_pending(self, dispatch: JobDispatch) -> tuple[_PendingDispatch, JobHandle[Any]]:
        if dispatch.countdown.seconds and self._timer is None:
            raise JobDelayRefused(dispatch.countdown)
        outcome = InlineOutcome()
        pending = _PendingDispatch(
            dispatch=dispatch,
            delivery=dispatch.options.delivery_for(dispatch.job_type, dispatch.envelope),
            outcome=outcome,
        )
        return pending, JobHandle(
            job_id=str(dispatch.envelope.task_id), queue=dispatch.queue,
            dispatched_at=datetime.now(UTC), _inline_outcome=outcome,
        )

    async def _start(self, pending: _PendingDispatch) -> None:
        if pending.dispatch.countdown.seconds:
            self._schedule(pending, pending.dispatch.countdown)
            return
        await self._run(pending)

    def _schedule(self, pending: _PendingDispatch, delay: Countdown) -> None:
        if self._timer is None:
            raise JobDelayRefused(delay)
        async def step() -> None:
            await self._run(pending)
        self._timer.schedule(
            delay, step, job=pending.dispatch.job_type.__qualname__,
            task_id=str(pending.dispatch.envelope.task_id),
        )

    async def _run(self, pending: _PendingDispatch) -> None:
        resumed = Acting.current().resumed(pending.dispatch.delegation)
        token = None if resumed is None else set_identity(resumed.caller, until=resumed.until)
        try:
            await self._settle(pending, await self._runner.run(pending.delivery))
        finally:
            if token is not None:
                reset_identity(token)

    async def _settle(self, pending: _PendingDispatch, outcome: JobOutcome) -> None:
        match outcome:
            case Succeeded(result=result):
                error = await self._record_success(pending, result)
            case Postponed(delay=delay):
                self._schedule(
                    replace(pending, delivery=replace(
                        pending.delivery, postponements=pending.delivery.postponements + 1
                    )), delay,
                )
                error = None
            case Failed(delay=delay):
                self._schedule(
                    replace(pending, delivery=replace(
                        pending.delivery, attempt=pending.delivery.attempt + 1
                    )), delay,
                )
                error = None
            case Abandoned():
                error = outcome.as_exception()
        if error is not None:
            await self._record_failure(pending, error)

    async def _record_success(self, pending: _PendingDispatch, result: Any) -> Exception | None:
        pending.outcome.succeed(result)
        callback = pending.dispatch.on_success
        if callback is None:
            return None
        try:
            await notify(
                self._factory, callback, CallbackOutcome.SUCCESS,
                job_id=pending.dispatch.envelope.task_id, result=result,
                **(pending.dispatch.envelope.payload or {}),
            )
        except Exception as exc:
            return exc
        return None

    async def _record_failure(self, pending: _PendingDispatch, error: Exception) -> None:
        pending.outcome.fail(error)
        callback = pending.dispatch.on_failure
        if callback is not None:
            await notify(
                self._factory, callback, CallbackOutcome.FAILURE,
                job_id=pending.dispatch.envelope.task_id,
                exc_type=type(error).__qualname__, exc_msg=str(error),
                **(pending.dispatch.envelope.payload or {}),
            )
