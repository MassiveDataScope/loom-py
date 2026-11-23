"""What the serverless runtimes log and the fields of each record."""

from __future__ import annotations

from loom.core.log_names import LogEvent, LogKey


class ServerlessEvent(LogEvent):
    """The events the serverless runtimes log."""

    EVENT_JOB_FAILED = "serverless.event_job_failed"
    SQS_JOB_POSTPONED = "serverless.sqs_job_postponed"
    SQS_JOB_FAILED = "serverless.sqs_job_failed"
    S3_TEST_EVENT_ACKNOWLEDGED = "serverless.s3_test_event_acknowledged"
    AUTOCRUD_IGNORED = "serverless.autocrud_ignored"
    JOB_TRANSPORT_SELECTED = "serverless.job_transport_selected"
    AGENT_ENDPOINT_IGNORED = "serverless.agent_endpoint_ignored"


class ServerlessKey(LogKey):
    """The fields of a serverless record."""

    MESSAGE_ID = "loom.serverless.message_id"
    TRANSPORT = "loom.serverless.transport"
    CONFIG_KEY = "loom.serverless.config_key"
