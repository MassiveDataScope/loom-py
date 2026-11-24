"""The transport that runs dispatched jobs in the sending process."""

from __future__ import annotations

import functools
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from loom.core.errors.summary import ErrorSummary
from loom.core.job.callback import CallbackOutcome, JobCallback, notify
from loom.core.job.context import add_pending_dispatch
from loom.core.job.delivery import JobDelivery
from loom.core.job.dispatch import JobDispatch
from loom.core.job.envelope import resumed_delegation
from loom.core.job.handle import InlineOutcome, JobHandle
from loom.core.job.outcome import Abandoned, Failed, JobOutcome, Postponed, Succeeded
from loom.core.job.policy import Countdown, Redelivery
from loom.core.job.runner import JobRunner
from loom.core.job.timer import JobDelayRefusedError, JobTimer

if TYPE_CHECKING:
    from loom.core.engine.execution_observer import ExecutionObserver
    from loom.core.engine.executor import UseCaseExecutor
    from loom.core.use_case.factory import UseCaseFactory


@dataclass(frozen=True, slots=True)
class _PendingDispatch:
    """The data of one inline run held until its post-commit flush or timer step."""

    dispatch: JobDispatch
    delivery: JobDelivery
    outcome: InlineOutcome


class InlineJobTransport:
    """Enqueue dispatched jobs on a timer and run them in process, as a queue consumer would.

    Every dispatch, sent now or after the commit, with a countdown or none, is
    handed to the timer and runs once the sending call has returned, whatever
    the sender then does. A job a running job dispatches is enqueued the same
    way, so a chain of jobs never nests. The timer runs each step in a context
    of its own, so no invocation scope, post-commit channel or log field of the
    sender reaches the run; it acts as the dispatch's delegation and in its
    trace. :meth:`wait_until_idle` returns once the timer holds nothing.

    Args:
        factory: Builds job and callback instances.
        executor: Runs compiled execution plans.
        timer: The one owner of scheduling: every run, retry and postponement
            waits on it.
        can_wait_in_process: Whether a delay may wait in the memory of the
            process. Without it a countdown is refused, a postponement is
            abandoned and a failure is not retried; a job with no delay still
            runs on the timer.
        execution_observer: Receives the runner's job events.
    """

    def __init__(
        self,
        factory: UseCaseFactory,
        executor: UseCaseExecutor,
        *,
        timer: JobTimer,
        can_wait_in_process: bool = True,
        execution_observer: ExecutionObserver | None = None,
    ) -> None:
        self._factory = factory
        self._executor = executor
        self._timer = timer
        self._redelivery = Redelivery.DELAYED if can_wait_in_process else Redelivery.NONE
        self._runner = JobRunner(
            self._invoke, redelivery=self._redelivery, execution_observer=execution_observer
        )

    @property
    def redelivery(self) -> Redelivery:
        """The inline runner's ability to wait and run again."""
        return self._redelivery

    async def _invoke(self, delivery: JobDelivery) -> Any:
        envelope = delivery.envelope
        return await self._executor.run(
            delivery.job_type,
            factory=self._factory,
            params=envelope.params,
            payload=envelope.payload,
            delivery=delivery,
        )

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Enqueue the run on the timer once the caller's unit of work commits."""
        pending, handle = self._build_pending(dispatch)
        add_pending_dispatch(functools.partial(self._schedule, pending, dispatch.countdown))
        return handle

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Enqueue the run on the timer now; it starts after this call returns."""
        pending, handle = self._build_pending(dispatch)
        self._schedule(pending, dispatch.countdown)
        return handle

    async def wait_until_idle(self) -> None:
        """Return once no job of this transport is running or waiting on its timer."""
        await self._timer.wait_until_idle()

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Accept callbacks, whose discovery is checked by the service."""
        return None

    def _build_pending(self, dispatch: JobDispatch) -> tuple[_PendingDispatch, JobHandle[Any]]:
        if dispatch.countdown.seconds and not self._redelivery.can_postpone:
            raise JobDelayRefusedError(dispatch.countdown)
        outcome = InlineOutcome()
        pending = _PendingDispatch(
            dispatch=dispatch,
            delivery=dispatch.options.delivery_for(dispatch.job_type, dispatch.envelope),
            outcome=outcome,
        )
        return pending, JobHandle(
            job_id=str(dispatch.envelope.task_id),
            queue=dispatch.queue,
            dispatched_at=datetime.now(UTC),
            _inline_outcome=outcome,
        )

    def _schedule(self, pending: _PendingDispatch, delay: Countdown) -> None:
        async def step() -> None:
            await self._run(pending)

        self._timer.schedule(
            delay,
            step,
            job=pending.dispatch.job_type.__qualname__,
            task_id=str(pending.dispatch.envelope.task_id),
        )

    async def _run(self, pending: _PendingDispatch) -> None:
        dispatch = pending.dispatch
        with resumed_delegation(dispatch.delegation, dispatch.envelope.trace_id):
            await self._settle(pending, await self._runner.run(pending.delivery))

    async def _settle(self, pending: _PendingDispatch, outcome: JobOutcome) -> None:
        match outcome:
            case Succeeded(result=result):
                error = await self._record_success(pending, result)
            case Postponed(delay=delay):
                self._schedule(
                    replace(
                        pending,
                        delivery=replace(
                            pending.delivery, postponements=pending.delivery.postponements + 1
                        ),
                    ),
                    delay,
                )
                error = None
            case Failed(delay=delay):
                self._schedule(
                    replace(
                        pending,
                        delivery=replace(pending.delivery, attempt=pending.delivery.attempt + 1),
                    ),
                    delay,
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
                self._factory,
                callback,
                CallbackOutcome.SUCCESS,
                job_id=pending.dispatch.envelope.task_id,
                result=result,
                **(pending.dispatch.envelope.payload or {}),
            )
        except Exception as exc:
            return exc
        return None

    async def _record_failure(self, pending: _PendingDispatch, error: Exception) -> None:
        pending.outcome.fail(error)
        callback = pending.dispatch.on_failure
        if callback is not None:
            summary = ErrorSummary.of(error)
            await notify(
                self._factory,
                callback,
                CallbackOutcome.FAILURE,
                job_id=pending.dispatch.envelope.task_id,
                exc_type=summary.type,
                exc_code=summary.code,
                **(pending.dispatch.envelope.payload or {}),
            )
