"""``JobService`` sends each job to the backend that serves its effective queue."""

from __future__ import annotations


from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.errors import MixedJobTransportsError
from loom.core.job.callback import JobCallback
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.dispatch import JobDispatch
from loom.core.job.job import Job
from loom.core.job.service import InlineJobTransport, JobService
from tests.helpers.jobs import EmailJob


class _ReportJob(Job[str]):
    __queue__ = "reports"

    def execute(self) -> str:
        return "report"


class _Transport(InlineJobTransport):
    """An inline transport that records which queue's send reaches it."""

    def __init__(self, name: str, seen: list[str]) -> None:
        super().__init__(MagicMock(), MagicMock())
        self._name = name
        self._seen = seen

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        self._seen.append(f"{self._name}:{dispatch.job_type.__name__}")
        return super().send_after_commit(dispatch)

    def check_parallel_callbacks(
        self, on_all_success: type[JobCallback] | None, on_any_failure: type[JobCallback] | None
    ) -> None:
        self._seen.append(f"{self._name}:parallel")


@pytest.fixture
def seen() -> list[str]:
    return []


@pytest.fixture
def executor() -> MagicMock:
    executor = MagicMock()
    executor.run = AsyncMock(return_value="in-process")
    return executor


@pytest.fixture
def router(seen: list[str]) -> JobService:
    return JobService(MagicMock(), MagicMock(), _Transport("inline", seen), routes={"emails": _Transport("celery", seen)})


class TestJobService:
    @pytest.mark.parametrize(
        ("job_type", "queue", "expected"),
        [
            (EmailJob, None, "celery:EmailJob"),
            (_ReportJob, None, "inline:_ReportJob"),
            (_ReportJob, "emails", "celery:_ReportJob"),
            (EmailJob, "reports", "inline:EmailJob"),
        ],
        ids=["declared queue", "undeclared queue", "per-call queue", "per-call undeclared queue"],
    )
    def test_a_job_goes_to_the_backend_of_its_effective_queue(
        self,
        router: JobService,
        seen: list[str],
        job_type: type[Job[str]],
        queue: str | None,
        expected: str,
    ) -> None:
        handle = router.dispatch_after_commit(job_type, queue=queue)

        assert seen == [expected]
        assert handle.queue == (queue or job_type.__queue__)

    async def test_run_executes_in_process_through_the_default_whatever_the_queue(
        self,
        executor: MagicMock,
    ) -> None:
        routed = MagicMock()
        router = JobService(MagicMock(), executor, InlineJobTransport(MagicMock(), executor), routes={"emails": routed})

        assert await router.run(EmailJob, payload={"to": "a@b.c"}) == "in-process"
        executor.run.assert_awaited_once()
        routed.run.assert_not_called()

    def test_parallel_jobs_of_one_backend_go_to_its_parallel_dispatch(
        self, router: JobService, seen: list[str]
    ) -> None:
        group = router.dispatch_parallel([(EmailJob, {}), (EmailJob, {})])

        assert seen == ["celery:parallel", "celery:EmailJob", "celery:EmailJob"]
        assert len(group.handles) == 2

    def test_parallel_jobs_of_mixed_backends_keep_the_input_order(
        self, router: JobService, seen: list[str]
    ) -> None:
        group = router.dispatch_parallel([(_ReportJob, {}), (EmailJob, {}), (_ReportJob, {})])

        assert seen == ["inline:_ReportJob", "celery:EmailJob", "inline:_ReportJob"]
        assert [handle.queue for handle in group.handles] == ["reports", "emails", "reports"]

    @pytest.mark.parametrize("callback", ["on_all_success", "on_any_failure"])
    def test_parallel_callbacks_over_mixed_backends_are_refused(
        self, router: JobService, seen: list[str], callback: str
    ) -> None:
        jobs = [(_ReportJob, {}), (EmailJob, {})]
        callbacks = {callback: JobCallback}

        with pytest.raises(MixedJobTransportsError):
            router.dispatch_parallel(jobs, **callbacks)

        assert seen == []

    async def test_a_routed_inline_dispatch_runs_after_the_flush(self, executor: MagicMock) -> None:
        router = JobService(MagicMock(), executor, InlineJobTransport(MagicMock(), executor))

        handle = router.dispatch_after_commit(_ReportJob)
        executor.run.assert_not_awaited()
        await flush_pending_dispatches()

        assert handle.wait() == "in-process"
