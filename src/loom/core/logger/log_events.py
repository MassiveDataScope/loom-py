"""The log events and field keys of the logging setup itself."""

from __future__ import annotations

from loom.core.logger.vocabulary import LogEvent, LogKey


class LoggerLogEvent(LogEvent):
    """What the logging setup reports about the process."""

    HANDLERS_UNFILTERED = "logger.handlers_unfiltered"


class LoggerLogField(LogKey):
    """The field keys only the logging setup writes."""

    HANDLER_TYPES = "loom.logger.handler_types"
