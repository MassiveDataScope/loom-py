"""The errors the Lambda adapter raises to the platform."""

from __future__ import annotations

from loom.core.errors import LoomError


class SQSBatchFailedError(LoomError):
    """A record of the batch failed and the function does not report batch item failures."""


__all__ = ["SQSBatchFailedError"]
