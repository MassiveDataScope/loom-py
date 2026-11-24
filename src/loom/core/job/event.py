"""The records the job runner logs, and the fields they carry."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey, ValueKind


class JobEvent(LogEvent):
    """What the job runner logs."""

    DELIVERY_ABANDONED = "job.delivery_abandoned"
    RETRY_POLICIES_DESCRIBED = "job.retry_policies_described"


class JobKey(LogKey):
    """The fields of a job record."""

    QUEUE = "loom.job.queue"
    POSTPONEMENTS = "loom.job.postponements", ValueKind.NUMBER
    COMMITTED = "loom.job.committed", ValueKind.BOOLEAN
    FAILURES = "loom.job.failures", ValueKind.NUMBER
    RETRY_POLICIES = "loom.job.retry_policies", ValueKind.ARRAY
