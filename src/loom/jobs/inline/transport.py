"""The transport that runs dispatched jobs in the sending process."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from loom.core.identity import Acting, reset_identity, set_identity
from loom.core.job.callback import CallbackOutcome, JobCallback, notify
from loom.core.job.context import add_pending_dispatch
from loom.core.job.delivery import JobDelivery
from loom.core.job.dispatch import JobDispatch
from loom.core.job.handle import InlineOutcome, JobHandle
from loom.core.job.outcome import Abandoned, Failed, JobOutcome, Postponed, Succeeded
from loom.core.job.policy import Countdown, Redelivery
from loom.core.job.runner import JobRunner
from loom.core.job.timer import JobDelayRefusedError, JobTimer

if TYPE_CHECKING:
    from loom.core.engine.executor import UseCaseExecutor
    from loom.core.engine.metrics import MetricsRecorder
    from loom.core.use_case.factory import UseCaseFactory


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
        executor: UseCaseExecutor,
        *,
        timer: JobTimer | None = None,
        metrics: MetricsRecorder | None = None,
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
            delivery.job_type,
            factory=self._factory,
            params=envelope.params,
            payload=envelope.payload,
            delivery=delivery,
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

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Accept callbacks, whose discovery is checked by the service."""
        return None

    def _build_pending(self, dispatch: JobDispatch) -> tuple[_PendingDispatch, JobHandle[Any]]:
        if dispatch.countdown.seconds and self._timer is None:
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

    async def _start(self, pending: _PendingDispatch) -> None:
        if pending.dispatch.countdown.seconds:
            self._schedule(pending, pending.dispatch.countdown)
            return
        await self._run(pending)

    def _schedule(self, pending: _PendingDispatch, delay: Countdown) -> None:
        if self._timer is None:
            raise JobDelayRefusedError(delay)

        async def step() -> None:
            await self._run(pending)

        self._timer.schedule(
            delay,
            step,
            job=pending.dispatch.job_type.__qualname__,
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
            await notify(
                self._factory,
                callback,
                CallbackOutcome.FAILURE,
                job_id=pending.dispatch.envelope.task_id,
                exc_type=type(error).__qualname__,
                exc_msg=str(error),
                **(pending.dispatch.envelope.payload or {}),
            )
