"""SQS partial-batch handler for a composed Lambda application."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

from loom.composition import CompositionErrorCode, CompositionStateError, Recipe
from loom.composition.host import ApplicationHost
from loom.core.checks import REST_CHECKS
from loom.core.config.errors import ConfigError
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.worker import (
    apply_job_overrides,
    resolve_worker_components,
)
from loom.core.engine.post_commit import PostCommitError
from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.job.envelope import TASK_JOB_PREFIX, decode_job_envelope, job_context
from loom.core.job.outcome import is_retryable
from loom.core.job.service import InlineJobService, JobService
from loom.core.logger import get_logger
from loom.core.observability.event import Scope
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.persistence.stores import NONE_BACKEND

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_log = get_logger(__name__)
SQSHandler = Callable[[dict[str, Any], Any], dict[str, list[dict[str, str]]]]
TIMEOUT_SAFETY_MARGIN_MS = 1000
"""Stop starting records when fewer than one second remains in the invocation."""


class DuplicateJobName(ConfigError):
    """Two declared jobs have the same wire qualname."""


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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._host: ApplicationHost | None = None
        self._bound = False

    def recipe(self, source: CompositionSource) -> Recipe:
        """Discover jobs and declared use cases for Lambda.

        AutoCRUD use cases for discovered models are not compiled here.
        Discovered callbacks are not registered by this adapter.
        """
        if self._declared is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component="LambdaSQSAdapter")
            )
        resolved = resolve_worker_components(source.config, self._jobs, self._use_cases)
        if resolved.models:
            _log.warning(
                "Lambda ignores AutoCRUD use cases for discovered models",
                models=len(resolved.models),
            )
        if resolved.callbacks:
            _log.warning(
                "Lambda ignores discovered job callbacks", callbacks=len(resolved.callbacks)
            )
        declared: dict[str, type[Job[Any]]] = {}
        for job_type in resolved.jobs:
            name = job_type.__qualname__
            if name in declared:
                raise DuplicateJobName(f"Duplicate job qualname {name!r}")
            declared[name] = job_type
        for job_type in resolved.jobs:
            apply_job_overrides(source.config, job_type)
        self._declared = declared
        return Recipe(
            profile=LAMBDA_PROFILE,
            discovered=DiscoveryResult(
                models=tuple(dict.fromkeys(resolved.models)),
                use_cases=cast(Any, resolved.compilables),
                interfaces=(),
            ),
            backend=source.backend(default=NONE_BACKEND),
            skip_checks=frozenset({REST_CHECKS}),
            agents=False,
            metrics=self._metrics,
            connection_only=not resolved.models,
        )

    def bind(self, application: Application) -> SQSHandler:
        """Open once at Lambda Init and return the warm invocation handler."""
        if self._declared is None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_RECIPE_MISSING.issue(component="LambdaSQSAdapter")
            )
        if self._bound:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component="LambdaSQSAdapter")
            )
        jobs = self._declared
        service = application.kernel.container.resolve(JobService)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        host = ApplicationHost(application)
        try:
            loop.run_until_complete(host.start())
        except BaseException:
            loop.close()
            asyncio.set_event_loop(None)
            raise
        self._loop = loop
        self._host = host
        self._bound = True
        if isinstance(service, InlineJobService):
            _log.info(
                "Lambda dispatched jobs run inline in this invocation without a queue or retries"
            )
        else:
            _log.warning("Lambda dispatched jobs publish to the Celery broker")

        async def run_record(record: Mapping[str, Any]) -> None:
            try:
                envelope = decode_job_envelope(record["body"])
            except (KeyError, TypeError, ValueError) as exc:
                with application.observability.span(
                    Scope.JOB,
                    "<malformed>",
                    id=_message_id(record) or None,
                ):
                    raise MalformedJobMessage(str(exc)) from exc
            job_name = envelope.job.removeprefix(f"{TASK_JOB_PREFIX}.")
            with (
                job_context(envelope.trace_id, envelope.identity) as caller,
                application.observability.span(
                    Scope.JOB,
                    job_name,
                    trace_id=envelope.trace_id,
                    id=_message_id(record),
                    payload_size=len(envelope.payload or {}),
                ),
            ):
                try:
                    job_type = jobs[job_name]
                except KeyError as exc:
                    raise UnknownJob(job_name) from exc
                instance = application.kernel.factory.build(job_type)
                await application.kernel.executor.execute(
                    instance,
                    params=envelope.params,
                    payload=envelope.payload or {},
                    identity=caller,
                )

        def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
            records: list[Any] = event["Records"]
            fifo = any(_is_fifo(record) for record in records)
            failures: list[dict[str, str]] = []
            for index, record in enumerate(records):
                if _out_of_time(context):
                    return {"batchItemFailures": failures + _failed(records[index:])}
                try:
                    loop.run_until_complete(run_record(record))
                except Exception as exc:
                    _log_failure(record, exc)
                    if not is_retryable(exc):
                        continue
                    if fifo:
                        return {"batchItemFailures": failures + _failed(records[index:])}
                    failures.extend(_failed([record]))
            return {"batchItemFailures": failures}

        return handler

    def close(self) -> None:
        """Close the host on its loop when the environment is retired."""
        loop, host = self._loop, self._host
        if loop is None or host is None:
            return
        self._loop = None
        self._host = None
        try:
            loop.run_until_complete(host.stop())
        finally:
            loop.close()
            asyncio.set_event_loop(None)


def _message_id(record: Any) -> str:
    return str(record.get("messageId", "")) if isinstance(record, Mapping) else ""


def _failed(records: Sequence[Any]) -> list[dict[str, str]]:
    return [{"itemIdentifier": _message_id(record)} for record in records]


def _is_fifo(record: Any) -> bool:
    return isinstance(record, Mapping) and str(record.get("eventSourceARN", "")).endswith(".fifo")


def _out_of_time(context: Any) -> bool:
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    return callable(remaining) and cast(Callable[[], int], remaining)() < TIMEOUT_SAFETY_MARGIN_MS


def _job_name(record: Any) -> str:
    with suppress(KeyError, TypeError, ValueError):
        return decode_job_envelope(record["body"]).job
    return "<malformed>"


def _log_failure(record: Any, exc: Exception) -> None:
    committed = {"committed": exc.committed} if isinstance(exc, PostCommitError) else {}
    _log.exception(
        "SQS job failed" if is_retryable(exc) else "SQS job failed finally",
        job=_job_name(record),
        message_id=_message_id(record),
        error_class=type(exc).__name__,
        **committed,
    )
