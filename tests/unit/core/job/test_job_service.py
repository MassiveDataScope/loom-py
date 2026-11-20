"""``JobService`` sends each job through the transport that serves its effective queue."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import JobTimeoutError
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.handle import JobHandle
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase
from loom.jobs.inline import InlineJobTransport
from loom.testing import RecordingJobTransport
from tests.helpers.jobs import EmailJob, JobKernel
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory


class _ReportJob(Job[str]):
    __queue__ = "reports"

    def execute(self) -> str:
        return "report"


@pytest.fixture(autouse=True)
def _no_leftover_dispatches() -> None:
    clear_pending_dispatches()


@pytest.fixture
def default() -> RecordingJobTransport:
    return RecordingJobTransport()


@pytest.fixture
def emails() -> RecordingJobTransport:
    return RecordingJobTransport()


@pytest.fixture
def service(default: RecordingJobTransport, emails: RecordingJobTransport) -> JobService:
    return JobService(default, routes={"emails": emails})


class TestJobService:
    @pytest.mark.parametrize(
        ("job_type", "queue", "expected"),
        [
            (EmailJob, None, {"default": [], "emails": [EmailJob]}),
            (_ReportJob, None, {"default": [_ReportJob], "emails": []}),
            (_ReportJob, "emails", {"default": [], "emails": [_ReportJob]}),
            (EmailJob, "reports", {"default": [EmailJob], "emails": []}),
        ],
        ids=["declared queue", "undeclared queue", "per-call queue", "per-call undeclared queue"],
    )
    async def test_a_job_goes_to_the_transport_of_its_effective_queue(
        self,
        service: JobService,
        default: RecordingJobTransport,
        emails: RecordingJobTransport,
        job_type: type[Job[str]],
        queue: str | None,
        expected: dict[str, list[type[Job[str]]]],
    ) -> None:
        handle = service.dispatch_after_commit(job_type, queue=queue)
        await flush_pending_dispatches()

        sent = {
            "default": [record.job_type for record in default.dispatched],
            "emails": [record.job_type for record in emails.dispatched],
        }
        assert sent == expected
        assert handle.queue == (queue or job_type.__queue__)

    async def test_run_executes_in_process_whatever_the_queue(
        self, default: RecordingJobTransport, emails: RecordingJobTransport
    ) -> None:
        kernel = JobKernel()
        service = JobService(
            default, factory=kernel.factory, executor=kernel.executor, routes={"emails": emails}
        )

        assert await service.run(EmailJob) == "email"
        assert (default.dispatched, emails.dispatched) == ([], [])

    async def test_run_without_a_factory_and_executor_is_not_implemented(
        self, service: JobService
    ) -> None:
        with pytest.raises(NotImplementedError, match="requires factory and executor"):
            await service.run(EmailJob)

    async def test_parallel_jobs_of_one_transport_are_all_sent_through_it(
        self, service: JobService, emails: RecordingJobTransport
    ) -> None:
        group = service.dispatch_parallel([(EmailJob, {}), (EmailJob, {})])
        await flush_pending_dispatches()

        assert len(group.handles) == 2
        assert len(emails.dispatched) == 2

    async def test_parallel_jobs_of_mixed_transports_keep_the_input_order(
        self,
        service: JobService,
        default: RecordingJobTransport,
        emails: RecordingJobTransport,
    ) -> None:
        group = service.dispatch_parallel([(_ReportJob, {}), (EmailJob, {}), (_ReportJob, {})])
        await flush_pending_dispatches()

        assert [handle.queue for handle in group.handles] == ["reports", "emails", "reports"]
        assert (len(default.dispatched), len(emails.dispatched)) == (2, 1)


class _Report(UseCase[Any, None]):
    def __init__(self, jobs: JobService) -> None:
        self.jobs = jobs
        self.handle: JobHandle[str] | None = None
        self.ran_before_commit = True

    async def execute(self) -> None:
        self.handle = self.jobs.dispatch_after_commit(_ReportJob, queue="reports")
        try:
            self.handle.wait()
        except JobTimeoutError:
            self.ran_before_commit = False


class TestRoutedInlineTransport:
    async def test_an_inline_routed_queue_runs_the_job_only_after_the_unit_of_work_commits(
        self, default: RecordingJobTransport
    ) -> None:
        kernel = JobKernel()
        kernel.factory.register(_ReportJob)
        inline = InlineJobTransport(kernel.factory, kernel.executor)
        service = JobService(default, routes={"reports": inline})
        executor = RuntimeExecutor(
            UseCaseCompiler(),
            coordinator=OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())}),
        )
        use_case = _Report(service)

        await executor.execute(use_case)

        assert use_case.ran_before_commit is False
        assert use_case.handle is not None
        assert use_case.handle.wait() == "report"
        assert default.dispatched == []


class TestDispatchArguments:
    def test_dispatch_after_commit_has_no_eta(self, service: JobService) -> None:
        with pytest.raises(TypeError, match="eta"):
            service.dispatch_after_commit(EmailJob, eta=object())  # type: ignore[call-arg]

    async def test_dispatch_now_has_no_eta(self, service: JobService) -> None:
        with pytest.raises(TypeError, match="eta"):
            await service.dispatch_now(EmailJob, eta=object())  # type: ignore[call-arg]


class TestDispatchNowRouting:
    async def test_a_routed_queue_sends_through_its_own_transport(
        self,
        service: JobService,
        default: RecordingJobTransport,
        emails: RecordingJobTransport,
    ) -> None:
        await service.dispatch_now(EmailJob)

        assert (len(default.dispatched), len(emails.dispatched)) == (0, 1)
