"""Transport-neutral invocation of a job from its envelope."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loom.core.job.envelope import job_context
from loom.core.observability.event import Scope

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.job.delivery import JobDelivery
    from loom.core.observability.observability import ObservabilityRuntime
    from loom.core.use_case.factory import UseCaseFactory


class EnvelopeInvocation:
    """Builds and executes a delivery's job inside its caller and observability context.

    Args:
        factory: Builds the job with its dependencies.
        executor: Drives the compiled execution plan.
        observability: Opens the job's span.
    """

    def __init__(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        observability: ObservabilityRuntime,
    ) -> None:
        self._factory = factory
        self._executor = executor
        self._observability = observability

    async def __call__(self, delivery: JobDelivery) -> Any:
        """Run the job of *delivery* as the caller its envelope carries.

        Returns:
            The value the job's ``execute`` returned.
        """
        envelope = delivery.envelope
        with (
            job_context(envelope.trace_id, envelope.identity) as caller,
            self._observability.span(
                Scope.JOB,
                envelope.job_name,
                trace_id=envelope.trace_id,
                id=delivery.task_id,
                payload_size=len(envelope.payload or {}),
            ),
        ):
            return await self._executor.run(
                delivery.job_type,
                factory=self._factory,
                params=envelope.params,
                payload=envelope.payload or {},
                identity=caller,
                delivery=delivery,
            )
