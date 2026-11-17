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
    Deferred,
    Failed,
    JobOutcome,
    JobRejection,
    JobRetry,
    Rejected,
    Succeeded,
)

Invocation = Callable[[JobDelivery], Awaitable[Any]]
"""Runs the job of a delivery and returns its result, or raises what it raised."""


class JobRunner:
    """Runs a delivery and decides whether it succeeded, defers, fails or is rejected.

    Every transport maps the returned :class:`~loom.core.job.outcome.JobOutcome`
    to its own action; none of them classifies a failure. The runner emits the
    ``JOB_STARTED`` event and one of ``JOB_SUCCEEDED``, ``JOB_DEFERRED``,
    ``JOB_FAILED`` or ``JOB_REJECTED`` for each run.

    Args:
        invocation: Runs the job of a delivery.
        metrics: Receives the job events; none when omitted.
    """

    def __init__(self, invocation: Invocation, metrics: MetricsAdapter | None = None) -> None:
        self._invocation = invocation
        self._metrics: MetricsAdapter = NoMetrics() if metrics is None else metrics

    async def run(self, delivery: JobDelivery) -> JobOutcome:
        """Run *delivery* once.

        A :class:`~loom.core.job.outcome.JobRetry` defers without spending an
        attempt, a :class:`~loom.core.job.outcome.JobRejection` or an error that
        cannot succeed again rejects, and any other error fails until the
        policy's attempts run out, then rejects with the error's class as reason.

        Args:
            delivery: The message, its attempt and its policy.

        Returns:
            What the run came to.
        """
        trace_id = delivery.envelope.trace_id
        name = delivery.job_type.__qualname__
        self._metrics.on_event(RuntimeEvent(EventKind.JOB_STARTED, name, trace_id=trace_id))
        started = time.monotonic()
        outcome = await self._outcome(delivery)
        self._metrics.on_event(
            self._event(outcome, name, trace_id, (time.monotonic() - started) * 1000)
        )
        return outcome

    async def _outcome(self, delivery: JobDelivery) -> JobOutcome:
        try:
            result = await asyncio.wait_for(self._invocation(delivery), timeout=delivery.timeout_s)
        except JobRetry as signal:
            delay = signal.delay or delivery.policy.backoff.delay(delivery.deferrals + 1)
            return Deferred(delay, signal.reason)
        except JobRejection as signal:
            return Rejected(signal.reason, signal)
        except Exception as exc:
            return self._failure(delivery, exc)
        return Succeeded(result)

    @staticmethod
    def _failure(delivery: JobDelivery, error: Exception) -> Rejected | Failed:
        if getattr(error, "retryable", True) and delivery.policy.allows_attempt_after(
            delivery.attempt
        ):
            return Failed(error, delivery.policy.backoff.delay(delivery.attempt))
        return Rejected(type(error).__qualname__, error)

    @staticmethod
    def _event(
        outcome: JobOutcome, name: str, trace_id: str | None, duration_ms: float
    ) -> RuntimeEvent:
        match outcome:
            case Succeeded():
                kind, status, error = EventKind.JOB_SUCCEEDED, Status.SUCCESS, None
            case Deferred():
                kind, status, error = EventKind.JOB_DEFERRED, Status.DEFERRED, None
            case Failed(error=error):
                kind, status = EventKind.JOB_FAILED, Status.FAILURE
            case Rejected(error=error):
                kind, status = EventKind.JOB_REJECTED, Status.REJECTED
        return RuntimeEvent(
            kind, name, duration_ms=duration_ms, status=status, error=error, trace_id=trace_id
        )
