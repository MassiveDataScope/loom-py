"""SQS partial-batch handler for a composed Lambda application."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, cast

from loom.composition import CompositionErrorCode, CompositionStateError, LoopHost, Recipe
from loom.core.checks import REST_CHECKS
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.worker import (
    DuplicateJobName as DuplicateJobName,
)
from loom.core.discovery.worker import resolve_worker_components
from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.job.envelope import JobEnvelope, decode_job_envelope
from loom.core.job.outcome import failure_log_fields, is_retryable
from loom.core.job.run import run_job
from loom.core.job.service import InlineJobService, JobService
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.persistence.stores import NONE_BACKEND
from loom.serverless.aws.lambda_._refusals import refuse_manifest_agents, refuse_unserved_config

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_log = get_logger(__name__)
_COMPONENT = "LambdaSQSAdapter"
MALFORMED_JOB = "<malformed>"
"""Job label of a record whose body could not be decoded."""
TIMEOUT_SAFETY_MARGIN_MS = 1000
"""Stop starting records when fewer than one second remains in the invocation."""


class SQSHandler(Protocol):
    """The Lambda function handler of an SQS event source, owning its open application."""

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
        """Run one SQS batch and report the records to retry."""
        ...

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        ...


class LambdaSQSAdapter:
    """Compose jobs with Lambda pool defaults and serve SQS batches."""

    def __init__(
        self,
        *,
        jobs: Sequence[type[Job[Any]]] = (),
        use_cases: Sequence[type[Compilable]] = (),
        metrics: MetricsAdapter | None = None,
    ) -> None:
        """Create the adapter.

        Args:
            jobs: Explicit job classes.
            use_cases: Explicit use case classes to compile.
            metrics: Receives executor events. Job lifecycle metric events are
                deferred for this adapter version.
        """
        self._jobs = tuple(jobs)
        self._use_cases = tuple(use_cases)
        self._metrics = metrics
        self._declared: dict[str, type[Job[Any]]] | None = None
        self._handler: _SQSBatch | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """Discover jobs and declared use cases for Lambda.

        AutoCRUD use cases for discovered models are not compiled here.
        Discovered callbacks are not registered by this adapter.

        Raises:
            LambdaUnservedConfigError: When the configuration enables metrics
                or declares agents, before discovery; or the manifest declares
                agents.
            CompositionStateError: When this instance already composed an
                application.
        """
        if self._declared is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=_COMPONENT)
            )
        refuse_unserved_config(source, _COMPONENT)
        resolved = resolve_worker_components(source.config, self._jobs, self._use_cases)
        refuse_manifest_agents(resolved.agent_specs, _COMPONENT)
        if resolved.models:
            _log.warning(
                "Lambda ignores AutoCRUD use cases for discovered models",
                models=len(resolved.models),
            )
        if resolved.callbacks:
            _log.warning(
                "Lambda ignores discovered job callbacks", callbacks=len(resolved.callbacks)
            )
        declared = {job_type.__qualname__: job_type for job_type in resolved.jobs}
        self._declared = declared
        return Recipe(
            profile=LAMBDA_PROFILE,
            discovered=DiscoveryResult(
                models=tuple(dict.fromkeys(resolved.models)),
                use_cases=cast(Any, resolved.compilables),
                interfaces=(),
                jobs=resolved.jobs,
            ),
            backend=source.backend(default=NONE_BACKEND),
            skip_checks=frozenset({REST_CHECKS}),
            agents=False,
            metrics=self._metrics,
            connection_only=not resolved.models,
        )

    def bind(self, application: Application) -> SQSHandler:
        """Open once at Lambda Init and return the warm invocation handler.

        Raises:
            CompositionStateError: When :meth:`recipe` has not run on this
                instance, or it is already bound.
            Exception: The failure to open the application, as itself.
        """
        if self._declared is None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component=_COMPONENT)
            )
        if self._handler is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component=_COMPONENT)
            )
        service = application.kernel.container.resolve(JobService)
        host = LoopHost(application)
        host.start()
        self._handler = _SQSBatch(application, self._declared, host)
        if isinstance(service, InlineJobService):
            _log.info(
                "Lambda dispatched jobs run inline in this invocation without a queue or retries"
            )
        else:
            _log.warning(
                "Lambda dispatched jobs leave this invocation through the configured job service",
                job_service=type(service).__qualname__,
            )
        return self._handler

    def close(self) -> None:
        """Close the bound handler, as :meth:`SQSHandler.close` does."""
        if self._handler is not None:
            self._handler.close()


class _SQSBatch:
    """The warm invocation handler: runs one SQS batch on the loop opened at Init."""

    def __init__(
        self,
        application: Application,
        jobs: Mapping[str, type[Job[Any]]],
        host: LoopHost,
    ) -> None:
        self._application = application
        self._jobs = jobs
        self._host = host

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
        records: list[Any] = event["Records"]
        fifo = bool(records) and _is_fifo(records[0])
        failures: list[dict[str, str]] = []
        for index, record in enumerate(records):
            if _out_of_time(context):
                return _batch_with_remaining(failures, records[index:])
            try:
                self._host.run(self._run(record, context))
            except Exception as exc:
                _log_failure(record, exc)
                if not is_retryable(exc):
                    continue
                if fifo:
                    return _batch_with_remaining(failures, records[index:])
                failures.extend(_failed([record]))
        return {"batchItemFailures": failures}

    def close(self) -> None:
        """Close the application and its loop; a second call does nothing."""
        self._host.close()

    async def _run(self, record: Any, context: Any) -> None:
        envelope = self._decode(record)
        job_type = self._job_type(envelope, record)
        await asyncio.wait_for(
            run_job(
                envelope,
                job_type,
                factory=self._application.kernel.factory,
                executor=self._application.kernel.executor,
                observability=self._application.observability,
                message_id=_message_id(record),
            ),
            timeout=_record_timeout(job_type, context),
        )

    def _decode(self, record: Any) -> JobEnvelope:
        if not isinstance(record, Mapping) or "body" not in record:
            self._fail_in_span(MalformedJobMessage("SQS record has no body"), MALFORMED_JOB, record)
        try:
            return decode_job_envelope(record["body"])
        except MalformedJobMessage as exc:
            self._fail_in_span(exc, MALFORMED_JOB, record)

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
        """Raise ``error`` inside a failed job span, as a record that never ran its job."""
        with self._application.observability.span(
            Scope.JOB, name, id=_message_id(record) or None, **meta
        ):
            raise error


def _batch_with_remaining(
    failures: list[dict[str, str]], records: Sequence[Any]
) -> dict[str, list[dict[str, str]]]:
    return {"batchItemFailures": failures + _failed(records)}


def _record_timeout(job_type: type[Job[Any]], context: Any) -> float | None:
    limits: list[float] = []
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    if callable(remaining):
        limits.append(
            max(
                0.0,
                (cast(Callable[[], int], remaining)() - TIMEOUT_SAFETY_MARGIN_MS) / 1000,
            )
        )
    if job_type.__timeout__ is not None:
        limits.append(float(job_type.__timeout__))
    return min(limits) if limits else None


def _message_id(record: Any) -> str:
    return str(record.get("messageId", "")) if isinstance(record, Mapping) else ""


def _failed(records: Sequence[Any]) -> list[dict[str, str]]:
    return [{"itemIdentifier": _message_id(record)} for record in records]


def _is_fifo(record: Any) -> bool:
    return isinstance(record, Mapping) and str(record.get("eventSourceARN", "")).endswith(".fifo")


def _out_of_time(context: Any) -> bool:
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    return callable(remaining) and cast(Callable[[], int], remaining)() < TIMEOUT_SAFETY_MARGIN_MS


def _job_label(record: Any) -> str:
    if isinstance(record, Mapping) and "body" in record:
        with suppress(MalformedJobMessage):
            return decode_job_envelope(record["body"]).job_name
    return MALFORMED_JOB


def _log_failure(record: Any, exc: Exception) -> None:
    _log.exception(
        "SQS job failed" if is_retryable(exc) else "SQS job failed finally",
        job=_job_label(record),
        message_id=_message_id(record),
        error_class=type(exc).__name__,
        **failure_log_fields(exc),
    )
