"""The log events and field keys of the job runtime."""

from __future__ import annotations

from loom.core.logger import LogEvent, LogKey


class JobLogEvent(LogEvent):
    """The job runner's decisions on a failed run."""

    DELIVERY_RETRIED = "job.delivery_retried"
    DELIVERY_ABANDONED = "job.delivery_abandoned"


class JobLogField(LogKey):
    """The field keys of a job's delivery."""

    PAYLOAD_SIZE = "loom.job.payload_size"
    POSTPONEMENTS = "loom.job.postponements"
    RETRY_DELAY_S = "loom.job.retry_delay_s"
