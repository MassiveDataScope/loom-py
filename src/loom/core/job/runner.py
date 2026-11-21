"""The one place that settles what a job's run came to, and reports it."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from loom.core.engine.events import AbandonReason, EventKind, ExecutionEvent, Status
from loom.core.engine.execution_observer import ExecutionObserver, NullExecutionObserver
from loom.core.errors import LoomError
from loom.core.job.delivery import JobDelivery
from loom.core.job.errors import JobAbandonment, JobPostponement
from loom.core.job.log_events import JobLogEvent, JobLogField
from loom.core.job.outcome import Abandoned, Failed, JobOutcome, Postponed, Succeeded
from loom.core.job.policy import Redelivery, RetryPolicy
from loom.core.logger import LogField, log_context

_log = logging.getLogger(__name__)

Invocation = Callable[[JobDelivery], Awaitable[Any]]
"""Runs the job of a delivery and returns its result, or raises what it raised."""


@dataclass(frozen=True, slots=True)
class _Whereabouts:
    """Which message a decision record is about; a job and a queue only when it was readable."""

    job: str | None
    queue: str | None
    attempt: int
    postponements: int
    task_id: str | None

    @classmethod
    def of_delivery(cls, delivery: JobDelivery) -> _Whereabouts:
        return cls(
            job=delivery.job_type.__qualname__,
            queue=delivery.queue,
            attempt=delivery.attempt,
            postponements=delivery.postponements,
            task_id=delivery.task_id,
        )

    @classmethod
    def of_unrunnable(cls, attempt: int, message_id: str | None) -> _Whereabouts:
        return cls(job=None, queue=None, attempt=attempt, postponements=0, task_id=message_id)

    def run_fields(self) -> dict[str, object]:
        """The fields every record of the run carries, leaving out what is not known."""
        known = {
            LogField.JOB: self.job,
            LogField.TASK_ID: self.task_id,
            LogField.ATTEMPT: self.attempt,
        }
        return {key: value for key, value in known.items() if value is not None}

    def log_decision(self, outcome: JobOutcome) -> None:
        """Write the runner's own decision on a failed run: one record, the error as its exception.

        ``job.delivery_retried`` (WARNING) when another attempt follows, and
        ``job.delivery_abandoned`` (ERROR) when the message will not run again.
        The record factory turns the error into its type and code, and writes
        its stack only when no scope has written it.
        """
        match outcome:
            case Failed(error=error, delay=delay):
                _log.warning(
                    JobLogEvent.DELIVERY_RETRIED,
                    exc_info=error,
                    extra={**self.run_fields(), JobLogField.RETRY_DELAY_S: delay.seconds},
                )
            case Abandoned(reason=reason):
                _log.error(
                    JobLogEvent.DELIVERY_ABANDONED,
                    exc_info=outcome.as_exception(),
                    extra={
                        **self.run_fields(),
                        LogField.REASON: reason.value,
                        LogField.MESSAGING_DESTINATION_NAME: self.queue,
                        JobLogField.POSTPONEMENTS: self.postponements,
                    },
                )


class JobRunner:
    """Runs a delivery and decides whether it succeeded, postpones, fails or is abandoned.

    Every transport maps the returned :class:`~loom.core.job.outcome.JobOutcome`
    to its own action; none of them classifies a failure. The runner emits the
    ``JOB_STARTED`` event and one of ``JOB_SUCCEEDED``, ``JOB_POSTPONED``,
    ``JOB_FAILED`` or ``JOB_ABANDONED`` for each run, and logs its decision on a
    failed run, ``job.delivery_retried`` or ``job.delivery_abandoned``, whatever
    the transport. It knows what its transport
    can do with a job that must run again, so it never postpones or fails a run that
    would not come back.

    Args:
        invocation: Runs the job of a delivery.
        redelivery: What the transport does with a job that must run again.
        execution_observer: Receives the job events; none when omitted.
        rng: The source of the backoff jitter; a fresh one when omitted.
    """

    def __init__(
        self,
        invocation: Invocation,
        *,
        redelivery: Redelivery,
        execution_observer: ExecutionObserver | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._invocation = invocation
        self._rng = random.Random() if rng is None else rng
        self._redelivery = redelivery
        self._execution_observer: ExecutionObserver = (
            NullExecutionObserver() if execution_observer is None else execution_observer
        )

    async def run(self, delivery: JobDelivery) -> JobOutcome:
        """Run *delivery* once.

        A :class:`~loom.core.job.errors.JobPostponement` postpones without spending an
        attempt when the transport can wait, else abandons. A
        :class:`~loom.core.job.errors.JobAbandonment` or an error that cannot
        succeed again abandons, and any other error fails while the transport
        retries and the policy's attempts last, then abandons.

        Args:
            delivery: The message, its attempt and its policy.

        Returns:
            What the run came to.
        """
        trace_id = delivery.envelope.trace_id
        name = delivery.job_type.__qualname__
        self._execution_observer.on_event(
            ExecutionEvent(EventKind.JOB_STARTED, name, trace_id=trace_id)
        )
        started = time.monotonic()
        whereabouts = _Whereabouts.of_delivery(delivery)
        with log_context(whereabouts.run_fields()):
            outcome = await self._run_once(delivery)
        self._execution_observer.on_event(
            self._build_event(outcome, name, trace_id, (time.monotonic() - started) * 1000)
        )
        whereabouts.log_decision(outcome)
        return outcome

    async def _run_once(self, delivery: JobDelivery) -> JobOutcome:
        try:
            result = await asyncio.wait_for(self._invocation(delivery), timeout=delivery.timeout_s)
        except JobPostponement as signal:
            if not self._redelivery.can_postpone:
                return Abandoned(AbandonReason.CANNOT_WAIT, signal)
            delay = signal.delay or delivery.policy.backoff.delay(
                delivery.postponements + 1, self._rng
            )
            return Postponed(delay, signal.reason)
        except JobAbandonment as signal:
            return Abandoned(AbandonReason.JOB_ASKED, signal)
        except Exception as exc:
            return self._decide(exc, delivery.attempt, delivery.policy)
        return Succeeded(result)

    def decide_unrunnable(
        self, error: Exception, attempt: int, policy: RetryPolicy, *, message_id: str | None = None
    ) -> Abandoned | Failed:
        """Decide a message that could not be run, and report it.

        Args:
            error: Why the message cannot run: it is undecodable or names no job.
            attempt: The attempt number, from 1.
            policy: The policy that governs the delivery.
            message_id: The transport's id of the message, for the abandonment log.

        Returns:
            What :meth:`run` would return for a job raising *error*; its event
            is emitted, named by the error's class.
        """
        outcome = self._decide(error, attempt, policy)
        self._execution_observer.on_event(
            self._build_event(outcome, type(error).__qualname__, None, None)
        )
        _Whereabouts.of_unrunnable(attempt, message_id).log_decision(outcome)
        return outcome

    def _decide(self, error: Exception, attempt: int, policy: RetryPolicy) -> Abandoned | Failed:
        if isinstance(error, LoomError) and not error.is_retryable:
            return Abandoned(AbandonReason.NOT_RETRYABLE, error)
        if not self._redelivery.can_retry:
            return Abandoned(AbandonReason.CANNOT_WAIT, error)
        if not policy.can_retry_after(attempt):
            return Abandoned(AbandonReason.ATTEMPTS_EXHAUSTED, error)
        return Failed(error, policy.backoff.delay(attempt, self._rng))

    @staticmethod
    def _build_event(
        outcome: JobOutcome, name: str, trace_id: str | None, duration_ms: float | None
    ) -> ExecutionEvent:
        reason: AbandonReason | None = None
        match outcome:
            case Succeeded():
                kind, status, error = EventKind.JOB_SUCCEEDED, Status.SUCCESS, None
            case Postponed():
                kind, status, error = EventKind.JOB_POSTPONED, Status.POSTPONED, None
            case Failed(error=error):
                kind, status = EventKind.JOB_FAILED, Status.FAILURE
            case Abandoned(error=error, reason=reason):
                kind, status = EventKind.JOB_ABANDONED, Status.ABANDONED
        return ExecutionEvent(
            kind,
            name,
            duration_ms=duration_ms,
            status=status,
            error=error,
            trace_id=trace_id,
            reason=reason,
        )
