"""The log events and field keys of a failure, declared by its owner, the errors package."""

from __future__ import annotations

from loom.core.fields import LogEvent, LogKey


class ErrorLogEvent(LogEvent):
    """What the loom boundary reports about a failure that leaves loom."""

    FAILURE_LEFT_LOOM = "errors.failure_left_loom"


class ErrorLogField(LogKey):
    """The field keys that describe a failure."""

    EXCEPTION_TYPE = "exception.type"
    ERROR_CODE = "loom.error.code"
    EXCEPTION_STACKTRACE = "exception.stacktrace"
    ORIGINAL_TYPE = "loom.error.original_type"
    ERROR_CLASS = "loom.error.class"
    MISDECLARATION = "loom.error.misdeclaration"
    VIOLATION_COUNT = "loom.error.violation_count"
    VIOLATION_CODES = "loom.error.violation_codes"
