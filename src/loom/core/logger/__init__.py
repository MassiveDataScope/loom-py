"""Logging the Python way: stdlib loggers, the context every record carries, its identity.

The names of events and fields live in :mod:`loom.core.log_names`. Choosing a
format and installing it is :mod:`loom.core.logger.install`.
"""

from loom.core.logger.context import current_log_fields, log_context
from loom.core.logger.identity import LogIdentity, LogRuntime

__all__ = [
    "LogIdentity",
    "LogRuntime",
    "current_log_fields",
    "log_context",
]
