"""Logging the Python way: stdlib loggers, a closed vocabulary of events and fields.

Importing this package loads nothing third-party. Choosing a format and
installing it is :mod:`loom.core.logger.install`.
"""

from loom.core.logger.context import current_log_fields, log_context
from loom.core.logger.event import EVENT_FORM, LogEvent
from loom.core.logger.field import ErrorSummary, LogField
from loom.core.logger.stamp import ContextFilter, LogIdentity, LogRuntime

__all__ = [
    "EVENT_FORM",
    "ContextFilter",
    "ErrorSummary",
    "LogEvent",
    "LogField",
    "LogIdentity",
    "LogRuntime",
    "current_log_fields",
    "log_context",
]
