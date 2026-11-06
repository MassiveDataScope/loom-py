"""The errors the SQS job service raises when a job cannot leave."""

from __future__ import annotations

from loom.core.errors import LoomError


class SQSDispatchError(LoomError):
    """A message SQS cannot carry, or a send that failed; ``code`` names which."""

    retryable = False


__all__ = ["SQSDispatchError"]
