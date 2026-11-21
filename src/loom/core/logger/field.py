"""The keys of a log record's fields, written once.

A key is an OpenTelemetry semantic convention when one exists
(``service.name``, ``code.function.name``) and ``loom.*`` otherwise.
"""

from __future__ import annotations

from enum import StrEnum


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
    CODE_FUNCTION = "code.function.name"
    CODE_LINE = "code.line.number"
    CODE_FILE = "code.file.path"
    SCOPE = "loom.scope"
    NAME = "loom.name"
    EVENT_ID = "loom.event_id"
    CORRELATION_ID = "loom.correlation_id"
    DURATION_MS = "loom.duration_ms"
    STATUS = "loom.status"
    ERROR_MESSAGE = "error.message"
    BOOTSTRAP_MODULE = "loom.bootstrap.module"
    OBSERVER = "loom.observer"
    EVENT_KIND = "loom.event_kind"
    USE_CASE_COUNT = "loom.use_case_count"
    EXCEPTION_TYPE = "exception.type"
    EXCEPTION_MESSAGE = "exception.message"
    EXCEPTION_STACKTRACE = "exception.stacktrace"
