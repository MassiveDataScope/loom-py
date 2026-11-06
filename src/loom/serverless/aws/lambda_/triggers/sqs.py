"""SQS partial-batch handler: loom job envelopes, or an event source's messages via a queue.

A ``jobs`` function reads each message body as the ``JobEnvelope``
``SQSJobService`` sent; an event trigger ``via: sqs`` reads it into the jobs
it declares, through its :class:`QueueSource`. Either way the batch reports
the messages to retry; via sqs a final failure too, so the queue dead-letters
it instead of consuming it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.job.envelope import JobEnvelope, decode_job_envelope
from loom.core.job.outcome import failure_log_fields, is_retryable
from loom.core.logger import get_logger
from loom.jobs.aws import FIFO_SUFFIX
from loom.serverless.aws.lambda_._jobs import (
    TIMEOUT_SAFETY_MARGIN_MS,
    Read,
    fail_in_span,
    remaining_ms,
    run_envelope,
)
from loom.serverless.aws.lambda_.errors import SQSBatchFailedError

if TYPE_CHECKING:
    from loom.composition import Application, LoopHost
    from loom.core.job.job import Job

_log = get_logger(__name__)
MALFORMED_JOB = "<malformed>"
"""Job label of a record whose body could not be decoded."""


def _envelope(body: str) -> tuple[tuple[JobEnvelope, str | None]]:
    return ((decode_job_envelope(body), None),)


@dataclass(frozen=True)
class QueueSource:
    """How a queue's message bodies become jobs, and what a failure of theirs means.

    Attributes:
        read: Turns a body into its jobs; a ``JobEnvelope`` by default.
        label: Names their job in a failure's span and log; the envelope's
            by default.
        retry_final: Reports a failure that is not retryable as well, so the
            queue dead-letters the message instead of consuming it.
        job: The one job every body runs, when the function declares it;
            otherwise each envelope names its own.
    """

    read: Read = _envelope
    label: str | None = None
    retry_final: bool = False
    job: type[Job[Any]] | None = None


_JOB_ENVELOPES = QueueSource()


class SQSBatch:
    """The warm invocation handler: runs one SQS batch on the loop opened at Init."""

    def __init__(
        self,
        application: Application,
        jobs: Mapping[str, type[Job[Any]]],
        host: LoopHost,
        *,
        report_failures: bool,
        source: QueueSource = _JOB_ENVELOPES,
    ) -> None:
        self._application = application
        self._jobs = jobs
        self._host = host
        self._report_failures = report_failures
        self._source = source

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
        records: list[Any] = event["Records"]
        failures = self._failures(records, context)
        if self._report_failures:
            return {"batchItemFailures": failures}
        if failures:
            raise SQSBatchFailedError(
                f"{len(failures)} of {len(records)} SQS records failed; "
                "report_batch_item_failures is off, so the whole batch is retried",
                code="sqs_batch_failed",
            )
        return {}

    def _failures(self, records: list[Any], context: Any) -> list[dict[str, str]]:
        fifo = bool(records) and _is_fifo(records[0])
        failures: list[dict[str, str]] = []
        for index, record in enumerate(records):
            if _out_of_time(context):
                return failures + _failed(records[index:])
            if self._failed_and_retryable(record, context):
                if fifo:
                    return failures + _failed(records[index:])
                failures += _failed([record])
        return failures

    def _failed_and_retryable(self, record: Any, context: Any) -> bool:
        try:
            self._host.run(self._run(record, context))
        except Exception as exc:
            _log_failure(record, exc, self._source.label)
            return self._source.retry_final or is_retryable(exc)
        return False

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        self._host.close()

    async def _run(self, record: Any, context: Any) -> None:
        for envelope, span_id in self._decode(record):
            job_type = self._job_type(envelope, record)
            span_id = span_id or _message_id(record)
            await run_envelope(self._application, envelope, job_type, span_id, context)

    def _decode(self, record: Any) -> Sequence[tuple[JobEnvelope, str | None]]:
        label = self._source.label or MALFORMED_JOB
        if not isinstance(record, Mapping) or "body" not in record:
            self._fail_in_span(MalformedJobMessage("SQS record has no body"), label, record)
        try:
            return self._source.read(record["body"])
        except MalformedJobMessage as exc:
            self._fail_in_span(exc, label, record)

    def _job_type(self, envelope: JobEnvelope, record: Any) -> type[Job[Any]]:
        job_type = self._source.job or self._jobs.get(envelope.job_name)
        if job_type is None:
            self._fail_in_span(
                UnknownJob(envelope.job_name),
                envelope.job_name,
                record,
                trace_id=envelope.trace_id,
                payload_size=len(envelope.payload or {}),
            )
        return job_type

    def _fail_in_span(self, error: Exception, name: str, record: Any, **meta: Any) -> NoReturn:
        fail_in_span(self._application, error, name, id=_message_id(record) or None, **meta)


def _message_id(record: Any) -> str:
    return str(record.get("messageId", "")) if isinstance(record, Mapping) else ""


def _failed(records: Sequence[Any]) -> list[dict[str, str]]:
    return [{"itemIdentifier": _message_id(record)} for record in records]


def _is_fifo(record: Any) -> bool:
    return isinstance(record, Mapping) and str(record.get("eventSourceARN")).endswith(FIFO_SUFFIX)


def _out_of_time(context: Any) -> bool:
    remaining = remaining_ms(context)
    return remaining is not None and remaining < TIMEOUT_SAFETY_MARGIN_MS


def _job_label(record: Any) -> str:
    if isinstance(record, Mapping) and "body" in record:
        with suppress(MalformedJobMessage):
            return decode_job_envelope(record["body"]).job_name
    return MALFORMED_JOB


def _log_failure(record: Any, exc: Exception, label: str | None) -> None:
    _log.exception(
        "SQS job failed" if is_retryable(exc) else "SQS job failed finally",
        job=label or _job_label(record),
        message_id=_message_id(record),
        error_class=type(exc).__name__,
        **failure_log_fields(exc),
    )
