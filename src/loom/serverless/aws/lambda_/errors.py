"""The errors the Lambda adapter raises to the platform."""

from __future__ import annotations

from loom.core.errors import SystemError
from loom.jobs.aws.errors import SQSErrorCode


class SQSBatchFailedError(SystemError):
    """A record of the batch failed and the function does not report batch item failures."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=SQSErrorCode.BATCH_FAILED)


__all__ = ["SQSBatchFailedError"]
