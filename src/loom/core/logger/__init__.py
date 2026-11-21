"""Logging the Python way: stdlib loggers, a closed vocabulary of events and field keys.

Importing this package loads nothing third-party. Making the records safe and
installing a format is :mod:`loom.core.logger.install`.
"""

from loom.core.logger.context import current_log_fields, log_context
from loom.core.logger.stamp import LogIdentity, LogRuntime
from loom.core.vocabulary import (
    EVENT_FORM,
    NO_ATTRIBUTES,
    LogEvent,
    LogField,
    LogKey,
    Scalar,
    SpanAttributes,
)

__all__ = [
    "EVENT_FORM",
    "NO_ATTRIBUTES",
    "LogEvent",
    "LogField",
    "LogIdentity",
    "LogKey",
    "LogRuntime",
    "Scalar",
    "SpanAttributes",
    "current_log_fields",
    "log_context",
]
