"""``JobService`` sends each job through the transport that serves its effective queue."""

from __future__ import annotations

import pytest

from loom.core.errors import MixedJobTransportsError
from loom.core.job.callback import JobCallback
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.testing import RecordingJobTransport
from tests.helpers.jobs import EmailJob, JobKernel


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

    @pytest.mark.parametrize("callback", ["on_all_success", "on_any_failure"])
    async def test_parallel_callbacks_over_mixed_transports_are_refused(
        self,
        service: JobService,
        default: RecordingJobTransport,
        emails: RecordingJobTransport,
        callback: str,
    ) -> None:
        with pytest.raises(MixedJobTransportsError):
            service.dispatch_parallel([(_ReportJob, {}), (EmailJob, {})], **{callback: JobCallback})

        await flush_pending_dispatches()
        assert (default.dispatched, emails.dispatched) == ([], [])
