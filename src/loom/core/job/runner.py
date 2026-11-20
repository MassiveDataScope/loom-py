"""The one place that settles what a job's run came to, and reports it."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from loom.core.engine.events import EventKind, RuntimeEvent, Status
from loom.core.engine.metrics import MetricsAdapter, NoMetrics
from loom.core.job.delivery import JobDelivery
from loom.core.job.outcome import (
    Abandoned,
    Failed,
    JobAbandonment,
    JobOutcome,
    JobPostponement,
    Postponed,
    Succeeded,
)
from loom.core.job.policy import Redelivery, RetryPolicy

Invocation = Callable[[JobDelivery], Awaitable[Any]]
"""Runs the job of a delivery and returns its result, or raises what it raised."""


class JobRunner:
    """Runs a delivery and decides whether it succeeded, postpones, fails or is abandoned.

    Every transport maps the returned :class:`~loom.core.job.outcome.JobOutcome`
    to its own action; none of them classifies a failure. The runner emits the
    ``JOB_STARTED`` event and one of ``JOB_SUCCEEDED``, ``JOB_POSTPONED``,
    ``JOB_FAILED`` or ``JOB_ABANDONED`` for each run. It knows what its transport
    can do with a job that must run again, so it never postpones or fails a run that
    would not come back.

    Args:
        invocation: Runs the job of a delivery.
        redelivery: What the transport does with a job that must run again.
        metrics: Receives the job events; none when omitted.
    """

    def __init__(
        self,
        invocation: Invocation,
        *,
        redelivery: Redelivery,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._invocation = invocation
        self._redelivery = redelivery
        self._metrics: MetricsAdapter = NoMetrics() if metrics is None else metrics

    async def run(self, delivery: JobDelivery) -> JobOutcome:
        """Run *delivery* once.

        A :class:`~loom.core.job.outcome.JobPostponement` postpones without spending an
        attempt when the transport can wait, else abandons. A
        :class:`~loom.core.job.outcome.JobAbandonment` or an error that cannot
        succeed again abandons, and any other error fails while the transport
        retries and the policy's attempts last, then abandons with the error's
        class as reason.

        Args:
            delivery: The message, its attempt and its policy.

        Returns:
            What the run came to.
        """
        trace_id = delivery.envelope.trace_id
        name = delivery.job_type.__qualname__
        self._metrics.on_event(RuntimeEvent(EventKind.JOB_STARTED, name, trace_id=trace_id))
        started = time.monotonic()
        outcome = await self._run_once(delivery)
        self._metrics.on_event(
            self._build_event(outcome, name, trace_id, (time.monotonic() - started) * 1000)
        )
        return outcome

    async def _run_once(self, delivery: JobDelivery) -> JobOutcome:
        try:
            result = await asyncio.wait_for(self._invocation(delivery), timeout=delivery.timeout_s)
        except JobPostponement as signal:
            if not self._redelivery.can_postpone:
                return Abandoned(f"cannot wait: {signal.reason}", signal)
            delay = signal.delay or delivery.policy.backoff.delay(delivery.postponements + 1)
            return Postponed(delay, signal.reason)
        except JobAbandonment as signal:
            return Abandoned(signal.reason, signal)
        except Exception as exc:
            return self._decide(exc, delivery.attempt, delivery.policy)
        return Succeeded(result)

    def decide_unrunnable(
        self, error: Exception, attempt: int, policy: RetryPolicy
    ) -> Abandoned | Failed:
        """Decide a message that could not be run, and report it.

        Args:
            error: Why the message cannot run: it is undecodable or names no job.
            attempt: The attempt number, from 1.
            policy: The policy that governs the delivery.

        Returns:
            What :meth:`run` would return for a job raising *error*; its event
            is emitted, named by the error's class.
        """
        outcome = self._decide(error, attempt, policy)
        self._metrics.on_event(self._build_event(outcome, type(error).__qualname__, None, None))
        return outcome

    def _decide(self, error: Exception, attempt: int, policy: RetryPolicy) -> Abandoned | Failed:
        can_retry = (
            getattr(error, "retryable", True)
            and self._redelivery.can_retry
            and policy.can_retry_after(attempt)
        )
        if can_retry:
            return Failed(error, policy.backoff.delay(attempt))
        return Abandoned(type(error).__qualname__, error)

    @staticmethod
    def _build_event(
        outcome: JobOutcome, name: str, trace_id: str | None, duration_ms: float | None
    ) -> RuntimeEvent:
        match outcome:
            case Succeeded():
                kind, status, error = EventKind.JOB_SUCCEEDED, Status.SUCCESS, None
            case Postponed():
                kind, status, error = EventKind.JOB_POSTPONED, Status.POSTPONED, None
            case Failed(error=error):
                kind, status = EventKind.JOB_FAILED, Status.FAILURE
            case Abandoned(error=error):
                kind, status = EventKind.JOB_ABANDONED, Status.ABANDONED
        return RuntimeEvent(
            kind, name, duration_ms=duration_ms, status=status, error=error, trace_id=trace_id
        )
