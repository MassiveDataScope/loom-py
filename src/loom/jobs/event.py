"""What the job transports log: their events and the keys of their fields."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from loom.core.job.config import JobSettings
from loom.core.job.job import Job
from loom.core.job.policy import Redelivery
from loom.core.log_names import LogEvent, LogKey

_log = logging.getLogger(__name__)


class JobsEvent(LogEvent):
    """The events the job transports and their settlement log."""

    SEND_FAILED = "jobs.send_failed"
    RETRY_POLICIES_DESCRIBED = "jobs.retry_policies_described"
    SETTLEMENT_QUEUE_UNREACHABLE = "jobs.settlement_queue_unreachable"
    SETTLEMENT_MESSAGE_DISCARDED = "jobs.settlement_message_discarded"
    SETTLEMENT_CALL_RAISED = "jobs.settlement_call_raised"
    SETTLEMENT_ENTRIES_FAILED = "jobs.settlement_entries_failed"


class JobsKey(LogKey):
    """The fields of a job transport record."""

    COUNT = "loom.jobs.count"
    ACTION = "loom.jobs.action"
    FAILED_ENTRIES = "loom.jobs.failed_entries"
    RETRY_POLICIES = "loom.jobs.retry_policies"


def log_retry_policies(
    settings: JobSettings, redelivery: Redelivery, jobs: Iterable[type[Job[Any]]]
) -> None:
    """Log each job's effective retry policy, as ``Job: policy``, once at start-up."""
    policies = [
        f"{described['job']}: {described['retry']}"
        for described in settings.describe_retries(redelivery, jobs)
    ]
    _log.info(JobsEvent.RETRY_POLICIES_DESCRIBED, extra={JobsKey.RETRY_POLICIES: policies})
