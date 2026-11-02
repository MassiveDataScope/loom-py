"""Transport-neutral execution of one job envelope."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loom.core.job.envelope import JobEnvelope, job_context
from loom.core.observability.event import Scope

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.job.job import Job
    from loom.core.observability.runtime import ObservabilityRuntime
    from loom.core.use_case.factory import UseCaseFactory


async def run_job(
    envelope: JobEnvelope,
    job_type: type[Job[Any]],
    *,
    factory: UseCaseFactory,
    executor: RuntimeExecutor,
    observability: ObservabilityRuntime,
    message_id: str | None,
) -> Any:
    """Build and execute a job inside its caller and observability context."""
    with (
        job_context(envelope.trace_id, envelope.identity) as caller,
        observability.span(
            Scope.JOB,
            envelope.job_name,
            trace_id=envelope.trace_id,
            id=message_id,
            payload_size=len(envelope.payload or {}),
        ),
    ):
        instance = factory.build(job_type)
        return await executor.execute(
            instance, params=envelope.params, payload=envelope.payload or {}, identity=caller
        )
