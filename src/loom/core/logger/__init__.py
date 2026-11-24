"""Logging the Python way: stdlib loggers, the names of events and fields.

Importing this package loads nothing third-party. Choosing a format and
installing it is :mod:`loom.core.logger.install`.
"""

from loom.core.log_names import FailureForm, LogEvent, LogField, LogKey, SubKey, ValueKind
from loom.core.logger.context import current_log_fields, log_context
from loom.core.logger.stamp import ContextFilter, LogIdentity, LogRuntime

__all__ = [
    "ContextFilter",
    "FailureForm",
    "LogEvent",
    "LogField",
    "LogKey",
    "LogIdentity",
    "LogRuntime",
    "SubKey",
    "ValueKind",
    "current_log_fields",
    "log_context",
]
