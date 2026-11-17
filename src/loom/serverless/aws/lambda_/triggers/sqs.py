"""SQS partial-batch handler: loom job envelopes, or an event source's messages via a queue.

A ``jobs`` function reads each message body as the ``JobEnvelope``
``SQSJobService`` sent; a queued event trigger reads it into the jobs
it declares, through its :class:`QueueSource`. Either way each record runs as a
delivery whose attempt and deferrals the record carries, and an
:class:`~loom.core.job.settlement.JobSettlement` settles its outcome; the batch
reports the messages the queue must deliver again.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.job.envelope import JobEnvelope, decode_job_envelope
from loom.core.job.outcome import (
    Deferred,
    Failed,
    JobOutcome,
    Rejected,
    Succeeded,
    failure_log_fields,
)
from loom.core.job.runner import JobRunner
from loom.core.logger import get_logger
from loom.jobs.aws.queue import SqsRecord
from loom.serverless.aws.lambda_._jobs import (
    TIMEOUT_SAFETY_MARGIN_MS,
    QueueConsumer,
    Read,
    fail_in_span,
    remaining_ms,
    run_envelope,
)

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
    """How a queue's message bodies become jobs.

    Attributes:
        read: Turns a body into its jobs; a ``JobEnvelope`` by default.
        label: Names their job in a failure's span and log; the envelope's
            by default.
        job: The one job every body runs, when the function declares it;
            otherwise each envelope names its own.
    """

    read: Read = _envelope
    label: str | None = None
    job: type[Job[Any]] | None = None


_JOB_ENVELOPES = QueueSource()


class SQSBatch:
    """The warm invocation handler: runs one SQS batch on the loop opened at Init.

    It builds a delivery per record and runs it; the settlement decides what each
    outcome means for its message. A record it never ran (out of time, or behind a
    failure on a FIFO queue) is reported without settling, as it is not an attempt.

    Args:
        application: The composed, open application.
        jobs: The jobs by the name an envelope gives them.
        host: Runs the batch's jobs on the application's loop.
        consumer: The policies its jobs run under, and what settles their messages.
        source: How a message body becomes jobs.
    """

    def __init__(
        self,
        application: Application,
        jobs: Mapping[str, type[Job[Any]]],
        host: LoopHost,
        consumer: QueueConsumer,
        source: QueueSource = _JOB_ENVELOPES,
    ) -> None:
        self._application = application
        self._jobs = jobs
        self._host = host
        self._consumer = consumer
        self._source = source

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
        records = [SqsRecord.from_event(raw) for raw in event["Records"]]
        fifo = bool(records) and records[0].is_fifo
        settled: list[tuple[SqsRecord, JobOutcome]] = []
        failed: list[SqsRecord] = []
        for index, record in enumerate(records):
            if _out_of_time(context):
                failed += records[index:]
                break
            if not record.is_settleable:
                failed.append(record)
                continue
            if record.unreadable:
                outcome: JobOutcome = JobRunner.decide(
                    MalformedJobMessage(record.unreadable),
                    record.receive_count,
                    self._consumer.unserved,
                )
                self._log_outcome(record, outcome)
            else:
                outcome = self._host.run(self._arrive(record, context))
            settled.append((record, outcome))
            if fifo and not isinstance(outcome, Succeeded):
                listed = self._consumer.settlement.settle_all(settled)
                failed += listed
                settled = []
                if listed:
                    failed += records[index + 1 :]
                    break
        failed += self._consumer.settlement.settle_all(settled)
        return {"batchItemFailures": [{"itemIdentifier": record.message_id} for record in failed]}

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        self._host.close()

    async def _arrive(self, record: SqsRecord, context: Any) -> JobOutcome:
        """The outcome of the record's jobs: the first one that did not succeed, else the last."""
        outcome: JobOutcome = Succeeded(None)
        try:
            for envelope, event_id in self._read(record):
                job_type = self._job_type(envelope, record)
                outcome = await run_envelope(
                    self._application,
                    envelope,
                    job_type,
                    event_id or envelope.task_id or record.message_id,
                    context,
                    settings=self._consumer.settings,
                    attempt=record.receive_count,
                    deferrals=record.deferrals,
                )
                if not isinstance(outcome, Succeeded):
                    break
        except (MalformedJobMessage, UnknownJob) as exc:
            outcome = JobRunner.decide(exc, record.receive_count, self._consumer.unserved)
        self._log_outcome(record, outcome)
        return outcome

    def _read(self, record: SqsRecord) -> Sequence[tuple[JobEnvelope, str | None]]:
        try:
            return self._source.read(record.body)
        except MalformedJobMessage as exc:
            self._fail_in_span(exc, self._source.label or MALFORMED_JOB, record)

    def _job_type(self, envelope: JobEnvelope, record: SqsRecord) -> type[Job[Any]]:
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

    def _fail_in_span(
        self, error: Exception, name: str, record: SqsRecord, **meta: Any
    ) -> NoReturn:
        fail_in_span(self._application, error, name, id=record.message_id or None, **meta)

    def _log_outcome(self, record: SqsRecord, outcome: JobOutcome) -> None:
        job = self._source.label or _job_label(record.body)
        match outcome:
            case Succeeded():
                return
            case Deferred(reason=reason, delay=delay):
                _log.info(
                    "SQS job deferred",
                    job=job,
                    message_id=record.message_id,
                    reason=reason,
                    delay_s=delay.seconds,
                )
            case Failed() | Rejected():
                error = outcome.failure()
                _log.exception(
                    "SQS job failed finally" if isinstance(outcome, Rejected) else "SQS job failed",
                    job=job,
                    message_id=record.message_id,
                    attempt=record.receive_count,
                    error_class=type(error).__name__,
                    exc_info=error,
                    **failure_log_fields(error),
                )


def _out_of_time(context: Any) -> bool:
    remaining = remaining_ms(context)
    return remaining is not None and remaining < TIMEOUT_SAFETY_MARGIN_MS


def _job_label(body: str) -> str:
    with suppress(MalformedJobMessage):
        return decode_job_envelope(body).job_name
    return MALFORMED_JOB
