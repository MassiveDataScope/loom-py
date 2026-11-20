from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class EventKind(StrEnum):
    """Discriminator for runtime and compile-time events.

    Used by ``MetricsRecorder`` and structured loggers to route events
    without isinstance checks.
    """

    COMPILE_START = "compile_start"
    COMPILE_DONE = "compile_done"
    EXEC_START = "exec_start"
    STEP_DONE = "step_done"
    EXEC_DONE = "exec_done"
    EXEC_ERROR = "exec_error"
    JOB_DISPATCHED = "job_dispatched"
    JOB_STARTED = "job_started"
    JOB_SUCCEEDED = "job_succeeded"
    JOB_POSTPONED = "job_postponed"
    JOB_FAILED = "job_failed"
    JOB_ABANDONED = "job_abandoned"


class Status(StrEnum):
    """Outcome label of a runtime event."""

    SUCCESS = "success"
    FAILURE = "failure"
    RULE_FAILURE = "rule_failure"
    POSTPONED = "postponed"
    ABANDONED = "abandoned"


class AbandonReason(StrEnum):
    """Why a job message will not run again."""

    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    NOT_RETRYABLE = "not_retryable"
    JOB_ASKED = "job_asked"
    CANNOT_WAIT = "cannot_wait"


class Phase(StrEnum):
    """The phase of an execution a failure is reported under, as its ``error_kind``."""

    BEGIN = "begin"
    BUSINESS = "business"
    COMMIT = "commit"
    BUILD = "build"
    CANCELLED = "cancelled"
    POST_COMMIT = "post_commit"


@dataclass(frozen=True)
class RuntimeEvent:
    """Immutable event emitted by the compiler and runtime executor.

    Consumed by ``MetricsRecorder`` and structured loggers. Carries no
    transport-specific data — adapters translate to their own formats.

    Args:
        kind: Event discriminator.
        use_case_name: Qualified name of the executing UseCase.
        step_name: Step label (e.g. ``"Load User"``), or ``None``.
        duration_ms: Wall-clock duration in milliseconds, or ``None``.
        status: Outcome label, or ``None``.
        error: Exception instance if the event represents a failure, or ``None``.
        trace_id: Trace the execution ran under, or ``None``.
        error_kind: Phase that failed on ``EXEC_ERROR``, or ``None``.
        pipeline_ms: Time spent in the use-case pipeline, or ``None`` when
            the pipeline did not start.
        commit_ms: Time spent closing the unit of work, or ``None`` when no
            commit was attempted.
        reason: Why the job was abandoned on ``JOB_ABANDONED``, or ``None``.

    Example::

        event = RuntimeEvent(
            kind=EventKind.EXEC_DONE,
            use_case_name="UpdateUserUseCase",
            duration_ms=12.4,
            status=Status.SUCCESS,
        )
    """

    kind: EventKind
    use_case_name: str
    step_name: str | None = None
    duration_ms: float | None = None
    status: Status | None = None
    error: BaseException | None = None
    trace_id: str | None = None
    error_kind: Phase | None = None
    pipeline_ms: float | None = None
    commit_ms: float | None = None
    reason: AbandonReason | None = None
