"""The log events of the Lambda runtime."""

from __future__ import annotations

from loom.core.logger import LogEvent


class LambdaLogEvent(LogEvent):
    """What the Lambda boundary logs about an invocation or an Init that failed."""

    INVOCATION_FAILED = "lambda.invocation_failed"
