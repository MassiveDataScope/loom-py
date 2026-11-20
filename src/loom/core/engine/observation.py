"""What the executor reports: runtime events, metrics and one structured log per run.

Every attempted run logs exactly one terminal line. A run whose use case was
built starts a :class:`StartedExecution` (``EXEC_START`` event) and ends it
once, either way: ``[DONE]`` and ``EXEC_DONE``, or ``[FAIL]`` and
``EXEC_ERROR`` with the ``error_kind`` of the phase that failed. A run that
never started (its plan, identity, read-only check or constructor failed)
logs ``[FAIL]`` with ``error_kind`` ``begin`` or ``build`` and emits no
event. A rule failure's ``[FAIL]`` lists the rules broken; in debug mode the
terminal line lists the steps run.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, NamedTuple, TypeVar

from loom.core.engine.events import EventKind, Phase, RuntimeEvent, Status
from loom.core.engine.metrics import MetricsAdapter, NoMetrics
from loom.core.errors import PostCommitError, RuleViolation, RuleViolations
from loom.core.job.outcome import JobSignal, failure_log_fields
from loom.core.logger import LoggerPort
from loom.core.stores.uow.coordinator import InvocationOutcome
from loom.core.stores.uow.scope import InvocationScope
from loom.core.tracing import get_trace_id

ResultT = TypeVar("ResultT")

_NO_FIELDS: Mapping[str, Any] = MappingProxyType({})


class BrokenRule(NamedTuple):
    """One rule a command broke: the rule's name (``None`` when unknown), the field, why."""

    rule: str | None
    field: str
    message: str


class ExecutionReporter:
    """Where one executor's events, metrics and logs go.

    Args:
        logger: Receives the structured logs.
        metrics: Receives the runtime events, if given.
        debug: Whether each execution's terminal log lists the steps it ran.
    """

    __slots__ = ("_debug", "_logger", "_metrics")

    def __init__(self, logger: LoggerPort, metrics: MetricsAdapter | None, debug: bool) -> None:
        self._logger = logger
        self._metrics: MetricsAdapter = NoMetrics() if metrics is None else metrics
        self._debug = debug

    def start(self, use_case: str) -> StartedExecution:
        """Report *use_case*'s execution started, and return what reports its end.

        Raises:
            Exception: What the metrics adapter raised, once logged as the
                execution failing to begin.
        """
        execution = self._execution(use_case)
        try:
            self._metrics.on_event(
                RuntimeEvent(
                    kind=EventKind.EXEC_START, use_case_name=use_case, trace_id=execution.trace_id
                )
            )
        except Exception as exc:
            execution.log_failure(exc, Status.FAILURE, Phase.BEGIN)
            raise
        return execution

    def not_started(self, use_case: str, error: Exception, phase: Phase) -> None:
        """Report a run of *use_case* that failed in *phase* before it started, with no event."""
        self._execution(use_case).log_failure(error, Status.FAILURE, phase)

    def _execution(self, use_case: str) -> StartedExecution:
        trace_id = get_trace_id()
        logger = self._logger.bind(trace_id=trace_id) if trace_id else self._logger
        steps: list[str] | None = [] if self._debug else None
        return StartedExecution(
            self._metrics, use_case, trace_id, logger, time.perf_counter(), steps
        )


@dataclass(slots=True)
class StartedExecution:
    """One started execution: its timings, the phase in flight, and its one terminal outcome.

    ``phase`` names the step in flight so a failure is reported with the
    ``error_kind`` of the step that raised; whoever runs a phase sets it and
    records its duration (``pipeline_ms``, ``commit_ms``). ``steps`` collects
    the steps run, in debug mode only; ``broken_rules`` the rules broken.
    """

    metrics: MetricsAdapter
    use_case_name: str
    trace_id: str | None
    logger: LoggerPort
    start: float
    steps: list[str] | None
    phase: Phase = Phase.BEGIN
    pipeline_ms: float | None = None
    commit_ms: float | None = None
    broken_rules: tuple[BrokenRule, ...] = ()

    def step(self, label: str) -> None:
        """Record one step run, in debug mode only."""
        if self.steps is not None:
            self.steps.append(label)

    def rule_failed(self, rule: str, violation: RuleViolation) -> None:
        """Record one rule broken; the terminal ``[FAIL]`` lists them all."""
        self.broken_rules += (BrokenRule(rule, violation.field, violation.message),)

    async def business(self, call: Awaitable[ResultT]) -> ResultT:
        """Await *call* as the business phase, recording its duration."""
        self.phase = Phase.BUSINESS
        started = time.perf_counter()
        try:
            return await call
        finally:
            self.pipeline_ms = (time.perf_counter() - started) * 1000

    async def committing(
        self, scope: InvocationScope, error: BaseException | None
    ) -> InvocationOutcome:
        """End every participant of *scope*; from the flush on, the phase is the commit."""
        if error is not None:
            return await scope.coordinator.finish(scope, error)
        self.phase = Phase.COMMIT
        started = time.perf_counter()
        try:
            return await scope.coordinator.finish(scope, None)
        finally:
            self.commit_ms = (time.perf_counter() - started) * 1000

    def succeeded(self) -> None:
        """Report the execution's success: ``[DONE]`` and ``EXEC_DONE``."""
        elapsed_ms = (time.perf_counter() - self.start) * 1000
        ran = _NO_FIELDS if self.steps is None else {"steps": self.steps}
        self.logger.info(
            f"[DONE] {elapsed_ms:.1f}ms",
            usecase=self.use_case_name,
            duration_ms=elapsed_ms,
            status=Status.SUCCESS,
            **ran,
        )
        self.metrics.on_event(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name=self.use_case_name,
                duration_ms=elapsed_ms,
                status=Status.SUCCESS,
                trace_id=self.trace_id,
                pipeline_ms=self.pipeline_ms,
                commit_ms=self.commit_ms,
            )
        )

    def failed(self, error: BaseException) -> None:
        """Report the execution's failure: ``[FAIL]`` and ``EXEC_ERROR``."""
        elapsed_ms = (time.perf_counter() - self.start) * 1000
        error_kind = self._error_kind(error)
        status = self._status(error)
        self.log_failure(error, status, error_kind, duration_ms=elapsed_ms)
        self.metrics.on_event(
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

    def log_failure(
        self, error: BaseException, status: Status, error_kind: Phase, **timing: Any
    ) -> None:
        """Log the terminal ``[FAIL]``: a broken rule is a warning, anything else an error."""
        if isinstance(error, JobSignal):
            self.logger.info(
                f"[SIGNAL] {self.use_case_name}",
                usecase=self.use_case_name,
                **timing,
                status=status,
                signal=type(error).__name__,
                reason=str(error),
            )
            return
        message = f"[FAIL] {self.use_case_name}"
        fields: dict[str, Any] = {
            "usecase": self.use_case_name,
            **timing,
            "status": status,
            "error_kind": error_kind,
        }
        if self.steps is not None:
            fields["steps"] = self.steps
        if isinstance(error, RuleViolations):
            broken = self.broken_rules or tuple(
                BrokenRule(None, violation.field, violation.message)
                for violation in error.violations
            )
            fields["broken_rules"] = [rule._asdict() for rule in broken]
            self.logger.warning(message, **fields)
            return
        self.logger.error(message, **fields, **failure_log_fields(error), error=str(error))

    @staticmethod
    def _status(error: BaseException) -> Status:
        if isinstance(error, RuleViolations):
            return Status.RULE_FAILURE
        if isinstance(error, JobSignal):
            return error.status
        return Status.FAILURE

    def _error_kind(self, error: BaseException) -> Phase:
        if isinstance(error, asyncio.CancelledError):
            return Phase.CANCELLED
        if isinstance(error, PostCommitError):
            return Phase.POST_COMMIT
        return self.phase
