"""What the job transports log: their events and the keys of their fields."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class JobsEvent(LogEvent):
    """The events the job transports and their settlement log."""

    SEND_FAILED = "jobs.send_failed"
    SETTLEMENT_QUEUE_UNREACHABLE = "jobs.settlement_queue_unreachable"
    SETTLEMENT_MESSAGE_DISCARDED = "jobs.settlement_message_discarded"
    SETTLEMENT_CALL_RAISED = "jobs.settlement_call_raised"
    SETTLEMENT_ENTRIES_FAILED = "jobs.settlement_entries_failed"


class JobsKey(LogKey):
    """The fields of a job transport record."""

    QUEUE_ARN = "loom.jobs.queue_arn"
    ACTION = "loom.jobs.action"
    FAILED_ENTRIES = "loom.jobs.failed_entries", ValueKind.ARRAY
