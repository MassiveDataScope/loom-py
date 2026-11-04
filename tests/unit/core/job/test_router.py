"""``JobRouter`` sends each job to the backend that serves its effective queue."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.job.callback import NullJobCallback
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.router import JobRouter, MixedJobBackendsError
from loom.core.job.service import InlineJobService
from tests.helpers.jobs import EmailJob


class _ReportJob(Job[str]):
    __queue__ = "reports"

    def execute(self) -> str:
        return "report"


class _Backend(InlineJobService):
    """An inline backend that records, by name, what reaches it."""

    def __init__(self, name: str, seen: list[str]) -> None:
        super().__init__(MagicMock(), MagicMock())
        self._name = name
        self._seen = seen

    def dispatch(self, job_type: type[Job[Any]], **kwargs: Any) -> JobHandle[Any]:
        self._seen.append(f"{self._name}:{job_type.__name__}")
        return super().dispatch(job_type, **kwargs)

    def dispatch_parallel(
        self, jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]], **kwargs: Any
    ) -> JobGroup:
        self._seen.append(f"{self._name}:parallel")
        return super().dispatch_parallel(jobs, **kwargs)


@pytest.fixture
def seen() -> list[str]:
    return []


@pytest.fixture
def executor() -> MagicMock:
    executor = MagicMock()
    executor.execute = AsyncMock(return_value="in-process")
    return executor


@pytest.fixture
def router(seen: list[str]) -> JobRouter:
    return JobRouter({"emails": _Backend("celery", seen)}, default=_Backend("inline", seen))


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
    router: JobRouter,
    seen: list[str],
    job_type: type[Job[str]],
    queue: str | None,
    expected: str,
) -> None:
    handle = router.dispatch(job_type, queue=queue)

    assert seen == [expected]
    assert handle.queue == (queue or job_type.__queue__)


async def test_run_executes_in_process_through_the_default_whatever_the_queue(
    executor: MagicMock,
) -> None:
    routed = MagicMock()
    router = JobRouter({"emails": routed}, default=InlineJobService(MagicMock(), executor))

    assert await router.run(EmailJob, payload={"to": "a@b.c"}) == "in-process"
    executor.execute.assert_awaited_once()
    routed.run.assert_not_called()


def test_parallel_jobs_of_one_backend_go_to_its_parallel_dispatch(
    router: JobRouter, seen: list[str]
) -> None:
    group = router.dispatch_parallel([(EmailJob, {}), (EmailJob, {})])

    assert seen == ["celery:parallel", "celery:EmailJob", "celery:EmailJob"]
    assert len(group.handles) == 2


def test_parallel_jobs_of_mixed_backends_keep_the_input_order(
    router: JobRouter, seen: list[str]
) -> None:
    group = router.dispatch_parallel([(_ReportJob, {}), (EmailJob, {}), (_ReportJob, {})])

    assert seen == ["inline:_ReportJob", "celery:EmailJob", "inline:_ReportJob"]
    assert [handle.queue for handle in group.handles] == ["reports", "emails", "reports"]


@pytest.mark.parametrize("callback", ["on_all_success", "on_any_failure"])
def test_parallel_callbacks_over_mixed_backends_are_refused(
    router: JobRouter, seen: list[str], callback: str
) -> None:
    jobs = [(_ReportJob, {}), (EmailJob, {})]
    callbacks = {callback: NullJobCallback}

    with pytest.raises(MixedJobBackendsError):
        router.dispatch_parallel(jobs, **callbacks)

    assert seen == []


async def test_a_routed_inline_dispatch_runs_after_the_flush(executor: MagicMock) -> None:
    inline = InlineJobService(MagicMock(), executor)
    router = JobRouter({}, default=inline)

    handle = router.dispatch(_ReportJob)
    executor.execute.assert_not_awaited()
    await flush_pending_dispatches()

    assert handle.wait() == "in-process"
