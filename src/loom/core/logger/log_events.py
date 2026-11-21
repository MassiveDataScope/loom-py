"""The log events and field keys of the logging setup itself."""

from __future__ import annotations

from loom.core.logger.vocabulary import LogEvent, LogKey


class LoggerLogEvent(LogEvent):
    """What the logging setup reports about the process."""

    UNCAUGHT_EXCEPTION = "logger.uncaught_exception"
    UNRAISABLE_EXCEPTION = "logger.unraisable_exception"
    HANDLERS_UNFILTERED = "logger.handlers_unfiltered"
    FAILURE_HANDED_OFF = "logger.failure_handed_off"


class LoggerLogField(LogKey):
    """The field keys only the logging setup writes."""

    HANDLER_TYPES = "loom.logger.handler_types"
    THREAD_NAME = "thread.name"
