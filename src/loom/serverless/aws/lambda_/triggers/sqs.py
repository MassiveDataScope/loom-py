"""SQS partial-batch handler: loom job envelopes, or an event source's messages via a queue.

A ``jobs`` function reads each message body as the ``JobEnvelope``
``SQSJobService`` sent; an event trigger ``via: sqs`` reads it into the jobs
it declares. Either way the batch reports the messages to retry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, cast

from loom.composition import LoopHost, Recipe
from loom.core.checks import REST_CHECKS
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.worker import resolve_worker_components
from loom.core.errors import LoomError, MalformedJobMessage, UnknownJob
from loom.core.job.envelope import JobEnvelope, decode_job_envelope
from loom.core.job.outcome import failure_log_fields, is_retryable
from loom.core.job.run import run_job
from loom.core.job.service import InlineJobService, JobService
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.persistence import LAMBDA_PROFILE
from loom.jobs.aws import FIFO_SUFFIX
from loom.serverless.aws.lambda_._refusals import refuse_manifest_agents

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_log = get_logger(__name__)
MALFORMED_JOB = "<malformed>"
"""Job label of a record whose body could not be decoded."""
TIMEOUT_SAFETY_MARGIN_MS = 1000
"""Stop starting records when fewer than one second remains in the invocation."""

ReadBody = Callable[[str], Sequence[tuple[JobEnvelope, str | None]]]
"""Reads a message body into the jobs it runs, in order, each with its span id or ``None``."""


class SQSBatchFailedError(LoomError):
    """A record of the batch failed and the function does not report batch item failures."""


class SQSHandler(Protocol):
    """The Lambda function handler of an SQS event source, owning its open application."""

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
        """Run one SQS batch and report the records to retry."""
        ...

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        ...


def jobs_recipe(
    source: CompositionSource,
    component: str,
    jobs: Sequence[type[Job[Any]]],
    use_cases: Sequence[type[Compilable]],
    metrics: MetricsAdapter | None,
) -> tuple[Recipe, dict[str, type[Job[Any]]]]:
    """The recipe of a function running jobs, with Lambda pool defaults, and the jobs by name.

    AutoCRUD use cases for discovered models are not compiled, and discovered
    callbacks are not registered.

    Raises:
        LambdaUnservedConfigError: When the manifest declares agents.
    """
    resolved = resolve_worker_components(source.config, jobs, use_cases)
    refuse_manifest_agents(resolved.agent_specs, component)
    if resolved.models:
        _log.warning(
            "Lambda ignores AutoCRUD use cases for discovered models",
            models=len(resolved.models),
        )
    if resolved.callbacks:
        _log.warning("Lambda ignores discovered job callbacks", callbacks=len(resolved.callbacks))
    recipe = Recipe(
        profile=LAMBDA_PROFILE,
        discovered=DiscoveryResult(
            models=tuple(dict.fromkeys(resolved.models)),
            use_cases=cast(Any, resolved.compilables),
            interfaces=(),
            jobs=resolved.jobs,
        ),
        skip_checks=frozenset({REST_CHECKS}),
        agents=False,
        metrics=metrics,
        connection_only=not resolved.models,
    )
    return recipe, {job_type.__qualname__: job_type for job_type in resolved.jobs}


def open_host(application: Application) -> LoopHost:
    """Open *application* at Init on a loop of its own, naming where dispatched jobs go.

    Raises:
        Exception: The failure to open the application, as itself.
    """
    service = application.kernel.container.resolve(JobService)
    host = LoopHost(application)
    host.start()
    if isinstance(service, InlineJobService):
        _log.info("Lambda dispatched jobs run inline in this invocation without a queue or retries")
    else:
        _log.warning(
            "Lambda dispatched jobs leave this invocation through the configured job service",
            job_service=type(service).__qualname__,
        )
    return host


def run_timeout(job_timeout: int | None, context: Any) -> float | None:
    """The time a job may take: its own timeout, or what remains of the invocation."""
    limits: list[float] = []
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    if callable(remaining):
        limits.append(
            max(
                0.0,
                (cast(Callable[[], int], remaining)() - TIMEOUT_SAFETY_MARGIN_MS) / 1000,
            )
        )
    if job_timeout is not None:
        limits.append(float(job_timeout))
    return min(limits) if limits else None


async def run_envelope(
    application: Application,
    envelope: JobEnvelope,
    job_type: type[Job[Any]],
    span_id: str | None,
    context: Any,
) -> None:
    """Run *envelope* as *job_type*, within its timeout and what remains of the invocation."""
    await asyncio.wait_for(
        run_job(
            envelope,
            job_type,
            factory=application.kernel.factory,
            executor=application.kernel.executor,
            observability=application.observability,
            message_id=span_id,
        ),
        timeout=run_timeout(application.job_settings.of(job_type).timeout, context),
    )


def fail_in_span(application: Application, error: Exception, name: str, **meta: Any) -> NoReturn:
    """Raise *error* inside a failed job span, as a message or event that never ran its job."""
    with application.observability.span(Scope.JOB, name, **meta):
        raise error


def _envelope(body: str) -> tuple[tuple[JobEnvelope, str | None]]:
    return ((decode_job_envelope(body), None),)


class SQSBatch:
    """The warm invocation handler: runs one SQS batch on the loop opened at Init.

    *read* turns a body into its jobs, a ``JobEnvelope`` by default; *label*
    names their job in a failure's span and log, the envelope's by default.
    """

    def __init__(
        self,
        application: Application,
        jobs: Mapping[str, type[Job[Any]]],
        host: LoopHost,
        *,
        report_failures: bool,
        read: ReadBody = _envelope,
        label: str | None = None,
    ) -> None:
        self._application = application
        self._jobs = jobs
        self._host = host
        self._report_failures = report_failures
        self._read = read
        self._label = label

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
            _log_failure(record, exc, self._label)
            return is_retryable(exc)
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
        label = self._label or MALFORMED_JOB
        if not isinstance(record, Mapping) or "body" not in record:
            self._fail_in_span(MalformedJobMessage("SQS record has no body"), label, record)
        try:
            return self._read(record["body"])
        except MalformedJobMessage as exc:
            self._fail_in_span(exc, label, record)

    def _job_type(self, envelope: JobEnvelope, record: Any) -> type[Job[Any]]:
        job_type = self._jobs.get(envelope.job_name)
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
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    return callable(remaining) and cast(Callable[[], int], remaining)() < TIMEOUT_SAFETY_MARGIN_MS


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
