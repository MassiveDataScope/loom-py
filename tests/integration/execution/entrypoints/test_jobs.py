"""The inline job service, an inline dispatch and the job runner build inside the execution.

The job runner is the transport-neutral body the Celery worker and the Lambda
``jobs`` handler share (``loom.core.job.run.run_job``).
"""

from __future__ import annotations

from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import ANONYMOUS
from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import JobEnvelope
from loom.core.job.handle import JobHandle
from loom.core.job.run import run_job
from loom.core.job.service import InlineJobService
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.factory import UseCaseFactory

from .doubles import ALICE, AcmeProbeJob, AcmeRun


class TestInlineJobService:
    async def test_run_builds_inside_an_execution_as_the_ambient_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        job = await InlineJobService(factory, executor).run(AcmeProbeJob)

        assert job.session.caller is ANONYMOUS
        assert job.session.execution is job.ran_in

    async def test_a_dispatch_runs_as_its_dispatcher_in_an_execution_of_its_own(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        jobs = InlineJobService(factory, executor)
        handles: list[JobHandle[AcmeProbeJob]] = []

        async def dispatch() -> None:
            handles.append(jobs.dispatch(AcmeProbeJob))

        await executor.execute(AcmeRun(dispatch), identity=ALICE)

        job = handles[0].wait()
        assert job.session.caller is ALICE
        assert job.session.execution is job.ran_in


class TestJobRunner:
    async def test_builds_inside_an_execution_as_the_envelope_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        envelope = JobEnvelope(job="AcmeProbeJob", identity=encode_identity(ALICE))

        job = await run_job(
            envelope,
            AcmeProbeJob,
            factory=factory,
            executor=executor,
            observability=ObservabilityRuntime.noop(),
            message_id=None,
        )

        assert job.session.caller == ALICE
        assert job.session.execution is job.ran_in
