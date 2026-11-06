"""The errors the SQS job service raises when a job cannot leave, and their codes."""

from __future__ import annotations

from enum import StrEnum, unique

from loom.core.errors import LoomError


@unique
class SQSErrorCode(StrEnum):
    """The codes of the failures only the SQS adapters report."""

    SEND_FAILED = "sqs_send_failed"
    BATCH_PARTIAL_FAILURE = "sqs_batch_partial_failure"
    BATCH_FAILED = "sqs_batch_failed"
    CALLBACKS = "sqs_callbacks"
    PRIORITY = "sqs_priority"
    QUEUE_UNKNOWN = "sqs_queue_unknown"
    INVALID_CHARACTER = "sqs_invalid_character"
    MESSAGE_TOO_LARGE = "sqs_message_too_large"
    DELAY_TOO_LONG = "sqs_delay_too_long"
    FIFO_DELAY = "sqs_fifo_delay"
    GROUP_MISSING = "sqs_group_missing"


class SQSDispatchError(LoomError):
    """A message SQS cannot carry, or a send that failed; ``code`` names which."""

    retryable = False

    def __init__(self, message: str, *, code: SQSErrorCode) -> None:
        super().__init__(message, code=code)


__all__ = ["SQSDispatchError", "SQSErrorCode"]
