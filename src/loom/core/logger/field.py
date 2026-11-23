"""The keys of a log record's fields, and what loom may say about a failure.

A key is an OpenTelemetry semantic convention when one exists
(``service.name``, ``code.function.name``) and ``loom.*`` otherwise.
:class:`ErrorSummary` is the one owner of what loom repeats about an exception.
"""

from __future__ import annotations

from enum import StrEnum

import msgspec

from loom.core.errors.base import LoomError


class LogField(StrEnum):
    """A field key of a log record."""

    SERVICE_NAME = "service.name"
    SERVICE_VERSION = "service.version"
    ENVIRONMENT = "deployment.environment.name"
    RUNTIME = "loom.runtime"
    TRACE_ID = "trace_id"
    USE_CASE = "loom.use_case"
    JOB = "loom.job"
    TASK_ID = "loom.task_id"
    ATTEMPT = "loom.attempt"
    REASON = "loom.reason"
    CODE_FUNCTION = "code.function.name"
    CODE_LINE = "code.line.number"
    CODE_FILE = "code.file.path"
    SCOPE = "loom.scope"
    NAME = "loom.name"
    EVENT_ID = "loom.event_id"
    CORRELATION_ID = "loom.correlation_id"
    DURATION_MS = "loom.duration_ms"
    STATUS = "loom.status"
    BOOTSTRAP_MODULE = "loom.bootstrap.module"
    OBSERVER = "loom.observer"
    EVENT_KIND = "loom.event_kind"
    USE_CASE_COUNT = "loom.use_case_count"
    EXCEPTION_TYPE = "exception.type"
    EXCEPTION_STACKTRACE = "exception.stacktrace"
    ERROR_CODE = "loom.error.code"


class ErrorSummary(msgspec.Struct, frozen=True, kw_only=True):
    """What a failure is, without its message: its type and, for a :class:`LoomError`, its code."""

    type: str
    code: str | None = None

    @classmethod
    def of(cls, exc: BaseException) -> ErrorSummary:
        """Summarise *exc*."""
        code = str(exc.code) if isinstance(exc, LoomError) else None
        return cls(type=type(exc).__qualname__, code=code)

    @staticmethod
    def cite(exc: BaseException) -> str:
        """The text a loom message may repeat about *exc*, its cause.

        Loom's own exceptions carry text loom wrote, so their message is repeated;
        any other exception is named by its summary, since its text can hold values.
        """
        if type(exc).__module__.startswith("loom."):
            return str(exc)
        return ErrorSummary.of(exc).describe()

    def describe(self) -> str:
        """The type, followed by the code when there is one: ``DomainError (not_found)``."""
        return self.type if self.code is None else f"{self.type} ({self.code})"

    def fields(self) -> dict[str, str]:
        """The record fields: ``exception.type`` and, with a code, ``loom.error.code``."""
        if self.code is None:
            return {LogField.EXCEPTION_TYPE: self.type}
        return {LogField.EXCEPTION_TYPE: self.type, LogField.ERROR_CODE: self.code}
