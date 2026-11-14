"""What the executor reports: runtime events, metrics and structured logs.

Every attempted run reports exactly one terminal outcome. A run whose use
case was built starts an :class:`Attempt` (``[EXEC]`` log and ``EXEC_START``
event) and ends it once, either way: ``[DONE]`` and ``EXEC_DONE``, or
``[FAIL]`` and ``EXEC_ERROR`` with the ``error_kind`` of the phase that
failed. A run whose use case failed to build reports ``[FAIL]`` with
``error_kind="build"`` and no event: its execution never started.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, TypeVar

from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.errors import PostCommitError, RuleViolation, RuleViolations
from loom.core.logger import LoggerPort
from loom.core.tracing import get_trace_id

ResultT = TypeVar("ResultT")

# The phases an execution's failure is reported with, as ``error_kind``.
BEGIN = "begin"
BUSINESS = "business"
COMMIT = "commit"
_BUILD = "build"
_CANCELLED = "cancelled"
_POST_COMMIT = "post_commit"

_SUCCESS = "success"
_FAILURE = "failure"
_RULE_FAILURE = "rule_failure"


class Observer:
    """Where one executor's events, metrics and logs go.

    Args:
        logger: Receives the structured logs.
        metrics: Receives the runtime events, if given.
        debug: Whether each preparation step logs a ``[STEP]`` line.
    """

    __slots__ = ("_debug", "_logger", "_metrics")

    def __init__(self, logger: LoggerPort, metrics: MetricsAdapter | None, debug: bool) -> None:
        self._logger = logger
        self._metrics = metrics
        self._debug = debug

    def start(self, use_case: str) -> Attempt:
        """Report *use_case*'s execution started, and return the attempt that reports its end."""
        start = time.perf_counter()  # first: the duration includes the start's own reporting
        trace_id = get_trace_id()
        logger = self._logger.bind(trace_id=trace_id) if trace_id else self._logger
        logger.info(f"[EXEC] {use_case}", usecase=use_case)
        self.emit(
            RuntimeEvent(kind=EventKind.EXEC_START, use_case_name=use_case, trace_id=trace_id)
        )
        return Attempt(self, use_case, trace_id, logger, start)

    def unbuilt(self, use_case: str, error: Exception) -> None:
        """Report *use_case* failed to build: its terminal outcome, with no event."""
        trace_id = get_trace_id()
        logger = self._logger.bind(trace_id=trace_id) if trace_id else self._logger
        _log_failure(logger, use_case, error, _FAILURE, _BUILD)

    def step(self, label: str) -> None:
        """Log one preparation step, in debug mode only."""
        if self._debug:
            self._logger.info(f"[STEP] {label}")

    def rule_failed(self, use_case: str, rule: str, violation: RuleViolation) -> None:
        """Log one rule *use_case* broke; the run reports its outcome once every rule ran."""
        self._logger.warning(
            f"[RULE] {rule} failed: {violation.field}: {violation.message}",
            usecase=use_case,
            field=violation.field,
        )

    def emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)


@dataclass(slots=True)
class Attempt:
    """One started execution: its timings, the phase in flight, and its one terminal outcome.

    ``phase`` names the step in flight so a failure is reported with the
    ``error_kind`` of the step that raised; whoever runs a phase sets it and
    records its duration (``pipeline_ms``, ``commit_ms``).
    """

    observer: Observer
    use_case_name: str
    trace_id: str | None
    logger: LoggerPort
    start: float
    phase: str = BEGIN
    pipeline_ms: float | None = None
    commit_ms: float | None = None

    async def business(self, call: Awaitable[ResultT]) -> ResultT:
        """Await *call* as the business phase, recording its duration."""
        self.phase = BUSINESS
        started = time.perf_counter()
        try:
            return await call
        finally:
            self.pipeline_ms = (time.perf_counter() - started) * 1000

    def succeeded(self) -> None:
        elapsed_ms = (time.perf_counter() - self.start) * 1000
        self.logger.info(
            f"[DONE] {elapsed_ms:.1f}ms",
            usecase=self.use_case_name,
            duration_ms=elapsed_ms,
            status=_SUCCESS,
        )
        self.observer.emit(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name=self.use_case_name,
                duration_ms=elapsed_ms,
                status=_SUCCESS,
                trace_id=self.trace_id,
                pipeline_ms=self.pipeline_ms,
                commit_ms=self.commit_ms,
            )
        )

    def failed(self, error: BaseException) -> None:
        elapsed_ms = (time.perf_counter() - self.start) * 1000
        error_kind = self._error_kind(error)
        status = _RULE_FAILURE if isinstance(error, RuleViolations) else _FAILURE
        _log_failure(
            self.logger, self.use_case_name, error, status, error_kind, duration_ms=elapsed_ms
        )
        self.observer.emit(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name=self.use_case_name,
                duration_ms=elapsed_ms,
                status=status,
                error=error,
                trace_id=self.trace_id,
                error_kind=error_kind,
                pipeline_ms=self.pipeline_ms,
                commit_ms=self.commit_ms,
            )
        )

    def _error_kind(self, error: BaseException) -> str:
        if isinstance(error, asyncio.CancelledError):
            return _CANCELLED
        if isinstance(error, PostCommitError):
            # Raised by the drain of an inner execution that owned its own
            # unit of work: the failure is not this execution's business logic.
            return _POST_COMMIT
        return self.phase


def _log_failure(
    logger: LoggerPort,
    use_case: str,
    error: BaseException,
    status: str,
    error_kind: str,
    **timing: Any,
) -> None:
    """Log a failed run's terminal outcome; a broken rule is a warning, anything else an error."""
    message = f"[FAIL] {use_case}"
    if status == _RULE_FAILURE:
        logger.warning(message, usecase=use_case, **timing, status=status, error_kind=error_kind)
        return
    logger.error(
        message,
        usecase=use_case,
        **timing,
        status=status,
        error_kind=error_kind,
        error=str(error),
    )
