"""The log events of the REST transport."""

from __future__ import annotations

from loom.core.logger import LogEvent


class RestLogEvent(LogEvent):
    """What the REST transport logs on its own, outside any ``use_case`` scope."""

    REQUEST_FAILED = "rest.request_failed"
