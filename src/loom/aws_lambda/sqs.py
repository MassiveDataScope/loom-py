"""SQS partial-batch handler for a composed Lambda application."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from loom.composition import CompositionErrorCode, CompositionStateError, Recipe
from loom.composition.host import ApplicationHost
from loom.core.checks import REST_CHECKS
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.discovery import (
    _apply_job_config_if_present,
    _resolve_compilables_and_jobs,
)
from loom.core.job.envelope import decode_job_envelope, job_context
from loom.core.logger import get_logger
from loom.core.persistence import LAMBDA_PROFILE
from loom.core.persistence.stores import NONE_BACKEND

if TYPE_CHECKING:
    from loom.composition import Application, CompositionSource
    from loom.core.engine.compilable import Compilable
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.job import Job

_log = get_logger(__name__)
SQSHandler = Callable[[dict[str, Any], Any], dict[str, list[dict[str, str]]]]


class LambdaSQSAdapter:
    """Compose jobs with Lambda pool defaults and serve SQS batches."""

    def __init__(
        self,
        *,
        jobs: Sequence[type[Job[Any]]] = (),
        use_cases: Sequence[type[Compilable]] = (),
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._jobs = tuple(jobs)
        self._use_cases = tuple(use_cases)
        self._metrics = metrics
        self._declared: dict[str, type[Job[Any]]] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._host: ApplicationHost | None = None

    def recipe(self, source: CompositionSource) -> Recipe:
        """Discover the same jobs and use cases as a Celery worker."""
        if self._declared is not None:
            raise CompositionStateError(
                CompositionErrorCode.ADAPTER_ALREADY_COMPOSED.issue(component="LambdaSQSAdapter")
            )
        resolved = _resolve_compilables_and_jobs(source.config, self._jobs, self._use_cases)
        for job_type in resolved.jobs:
            _apply_job_config_if_present(source.config, job_type)
        declared = {job_type.__qualname__: job_type for job_type in resolved.jobs}
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
        jobs = self._declared
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

        async def run_record(record: Mapping[str, Any]) -> None:
            envelope = decode_job_envelope(record["body"])
            job_name = envelope.job.removeprefix("loom.job.")
            job_type = jobs[job_name]
            with job_context(envelope.trace_id, envelope.identity) as caller:
                instance = application.kernel.factory.build(job_type)
                await application.kernel.executor.execute(
                    instance, params=envelope.params, payload=envelope.payload, identity=caller
                )

        def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
            del context
            records = event["Records"]
            fifo = any(
                str(record.get("eventSourceARN", "")).endswith(".fifo")
                or "MessageGroupId" in record.get("attributes", {})
                for record in records
            )
            failures: list[dict[str, str]] = []
            for index, record in enumerate(records):
                try:
                    loop.run_until_complete(run_record(record))
                except Exception:
                    _log.exception("SQS job failed", message_id=record["messageId"])
                    failures.append({"itemIdentifier": record["messageId"]})
                    if fifo:
                        failures.extend(
                            {"itemIdentifier": following["messageId"]}
                            for following in records[index + 1 :]
                        )
                        break
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
