"""What the executor reports: runtime events and one structured log per run.

Every attempted run logs exactly one terminal line. A run whose use case was
built starts a :class:`StartedExecution` (``EXEC_START`` event) and ends it
once, either way: ``engine.use_case_done`` and ``EXEC_DONE``, or
``engine.use_case_failed`` and ``EXEC_ERROR`` with the phase that failed. A run
that never started (its plan, identity, read-only check or constructor failed)
logs ``engine.use_case_failed`` with the phase ``begin`` or ``build`` and emits
no event. A rule failure's record lists the rules broken; in debug mode the
terminal record lists the steps run.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, NamedTuple, TypeVar

from loom.core.engine.events import EventKind, ExecutionEvent, Phase, Status
from loom.core.engine.execution_observer import ExecutionObserver, NullExecutionObserver
from loom.core.errors import PostCommitError, RuleViolationError, RuleViolationsError
from loom.core.job.outcome import JobSignal, failure_log_fields
from loom.core.log_names import LogEvent, LogField, LogKey
from loom.core.stores.uow.coordinator import InvocationOutcome
from loom.core.stores.uow.scope import InvocationScope
from loom.core.tracing import get_trace_id

ResultT = TypeVar("ResultT")

_NO_FIELDS: Mapping[str, Any] = MappingProxyType({})
_log = logging.getLogger(__name__)


class UseCaseEvent(LogEvent):
    """How a use case execution ends, as the executor logs it."""

    DONE = "engine.use_case_done"
    SIGNALLED = "engine.use_case_signalled"
    FAILED = "engine.use_case_failed"


class UseCaseKey(LogKey):
    """The fields of a use case execution record."""

    STEPS = "loom.use_case.steps"
    PHASE = "loom.use_case.phase"
    BROKEN_RULES = "loom.use_case.broken_rules"


class BrokenRule(NamedTuple):
    """One rule a command broke: the rule's name (``None`` when unknown), the field, why."""

    rule: str | None
    field: str
    message: str

    @property
    def label(self) -> str:
        """``rule:field``, or the field alone when the rule is unknown."""
        return self.field if self.rule is None else f"{self.rule}:{self.field}"


class ExecutionReporter:
    """Where one executor's events and logs go.

    Args:
        execution_observer: Receives the runtime events, if given.
        debug: Whether each execution's terminal log lists the steps it ran.
    """

    __slots__ = ("_debug", "_execution_observer")

    def __init__(self, execution_observer: ExecutionObserver | None, debug: bool) -> None:
        self._execution_observer: ExecutionObserver = (
            NullExecutionObserver() if execution_observer is None else execution_observer
        )
        self._debug = debug

    def start(self, use_case: str) -> StartedExecution:
        """Report *use_case*'s execution started, and return what reports its end.

        Raises:
            Exception: What the execution observer raised, once logged as the
                execution failing to begin.
        """
        execution = self._execution(use_case)
        try:
            self._execution_observer.on_event(
                ExecutionEvent(
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
        steps: list[str] | None = [] if self._debug else None
        return StartedExecution(
            self._execution_observer, use_case, trace_id, time.perf_counter(), steps
        )


@dataclass(slots=True)
class StartedExecution:
    """One started execution: its timings, the phase in flight, and its one terminal outcome.

    ``phase`` names the step in flight so a failure is reported with the
    ``error_kind`` of the step that raised; whoever runs a phase sets it and
    records its duration (``pipeline_ms``, ``commit_ms``). ``steps`` collects
    the steps run, in debug mode only; ``broken_rules`` the rules broken.
    """

    execution_observer: ExecutionObserver
    use_case_name: str
    trace_id: str | None
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

    def rule_failed(self, rule: str, violation: RuleViolationError) -> None:
        """Record one rule broken; the terminal record lists them all."""
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
        """Report the execution's success: its log record and ``EXEC_DONE``."""
        elapsed_ms = (time.perf_counter() - self.start) * 1000
        ran: Mapping[str, Any] = (
            _NO_FIELDS if self.steps is None else {UseCaseKey.STEPS: self.steps}
        )
        _log.info(
            UseCaseEvent.DONE,
            extra={
                LogField.USE_CASE: self.use_case_name,
                LogField.DURATION_MS: elapsed_ms,
                **ran,
            },
        )
        self.execution_observer.on_event(
            ExecutionEvent(
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
        """Report the execution's failure: its log record and ``EXEC_ERROR``."""
        elapsed_ms = (time.perf_counter() - self.start) * 1000
        error_kind = self._error_kind(error)
        status = self._status(error)
        self.log_failure(error, status, error_kind, elapsed_ms)
        self.execution_observer.on_event(
            ExecutionEvent(
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
        self,
        error: BaseException,
        status: Status,
        error_kind: Phase,
        duration_ms: float | None = None,
    ) -> None:
        """Log how the execution ended: a broken rule is a warning, anything else an error."""
        timing: dict[str, object] = (
            {} if duration_ms is None else {LogField.DURATION_MS: duration_ms}
        )
        if isinstance(error, JobSignal):
            _log.info(
                UseCaseEvent.SIGNALLED,
                extra={
                    LogField.USE_CASE: self.use_case_name,
                    **timing,
                    LogField.STATUS: status,
                    LogField.REASON: error.reason,
                },
            )
            return
        fields: dict[str, Any] = {
            LogField.USE_CASE: self.use_case_name,
            **timing,
            LogField.STATUS: status,
            UseCaseKey.PHASE: error_kind,
        }
        if self.steps is not None:
            fields[UseCaseKey.STEPS] = self.steps
        if isinstance(error, RuleViolationsError):
            broken = self.broken_rules or tuple(
                BrokenRule(None, violation.field, violation.message)
                for violation in error.violations
            )
            fields[UseCaseKey.BROKEN_RULES] = [rule.label for rule in broken]
            _log.warning(UseCaseEvent.FAILED, extra=fields)
            return
        _log.error(
            UseCaseEvent.FAILED, extra={**fields, **failure_log_fields(error)}, exc_info=error
        )

    @staticmethod
    def _status(error: BaseException) -> Status:
        if isinstance(error, RuleViolationsError):
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
