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
from loom.core.job.router import (
    DuplicateJobQueue,
    JobRouter,
    MixedJobBackendsError,
    UnroutedJobError,
    UnservedJobQueue,
)
from loom.core.job.service import InlineJobService, JobService


class _EmailJob(Job[str]):
    __queue__ = "emails"

    def execute(self) -> str:
        return "email"


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


def _router(
    backends: Sequence[tuple[JobService, Sequence[str]]],
    default: JobService | None,
    executor: MagicMock | None = None,
) -> JobRouter:
    return JobRouter(
        backends, default=default, factory=MagicMock(), executor=executor or MagicMock()
    )


@pytest.fixture
def router(seen: list[str]) -> JobRouter:
    celery = _Backend("celery", seen)
    return _router([(celery, ("emails",))], default=_Backend("inline", seen))


@pytest.mark.parametrize(
    ("job_type", "queue", "expected"),
    [
        (_EmailJob, None, "celery:_EmailJob"),
        (_ReportJob, None, "inline:_ReportJob"),
        (_ReportJob, "emails", "celery:_ReportJob"),
        (_EmailJob, "reports", "inline:_EmailJob"),
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


def test_a_queue_declared_by_two_backends_is_refused(seen: list[str]) -> None:
    backends = [(_Backend("a", seen), ("emails",)), (_Backend("b", seen), ("emails",))]

    with pytest.raises(DuplicateJobQueue, match="'emails'"):
        _router(backends, default=None)


def test_a_dispatch_to_a_queue_no_backend_serves_registers_nothing(seen: list[str]) -> None:
    router = _router([(_Backend("celery", seen), ("emails",))], default=None)

    with pytest.raises(UnroutedJobError, match="'reports'"):
        router.dispatch(_EmailJob, queue="reports")

    assert seen == []


def test_a_job_whose_queue_no_backend_serves_is_refused_at_startup(seen: list[str]) -> None:
    router = _router([(_Backend("celery", seen), ("emails",))], default=None)

    with pytest.raises(UnservedJobQueue, match="_ReportJob.*'reports'"):
        router.require_served([_EmailJob, _ReportJob])


def test_a_default_backend_serves_every_job(router: JobRouter) -> None:
    router.require_served([_EmailJob, _ReportJob])


async def test_run_executes_in_process_whatever_the_queue_routes_to(
    seen: list[str], executor: MagicMock
) -> None:
    router = _router([(_Backend("celery", seen), ("emails",))], default=None, executor=executor)

    assert await router.run(_EmailJob, payload={"to": "a@b.c"}) == "in-process"
    assert seen == []
    executor.execute.assert_awaited_once()


def test_parallel_jobs_of_one_backend_go_to_its_parallel_dispatch(
    router: JobRouter, seen: list[str]
) -> None:
    group = router.dispatch_parallel([(_EmailJob, {}), (_EmailJob, {})])

    assert seen == ["celery:parallel", "celery:_EmailJob", "celery:_EmailJob"]
    assert len(group.handles) == 2


def test_parallel_jobs_of_mixed_backends_keep_the_input_order(
    router: JobRouter, seen: list[str]
) -> None:
    group = router.dispatch_parallel([(_ReportJob, {}), (_EmailJob, {}), (_ReportJob, {})])

    assert seen == ["inline:_ReportJob", "celery:_EmailJob", "inline:_ReportJob"]
    assert [handle.queue for handle in group.handles] == ["reports", "emails", "reports"]


@pytest.mark.parametrize("callback", ["on_all_success", "on_any_failure"])
def test_parallel_callbacks_over_mixed_backends_are_refused(
    router: JobRouter, seen: list[str], callback: str
) -> None:
    jobs = [(_ReportJob, {}), (_EmailJob, {})]
    callbacks = {callback: NullJobCallback}

    with pytest.raises(MixedJobBackendsError):
        router.dispatch_parallel(jobs, **callbacks)

    assert seen == []


def test_a_parallel_job_no_backend_serves_registers_none_of_the_group(seen: list[str]) -> None:
    router = _router([(_Backend("celery", seen), ("emails",))], default=None)
    jobs = [(_EmailJob, {}), (_ReportJob, {})]

    with pytest.raises(UnroutedJobError):
        router.dispatch_parallel(jobs)

    assert seen == []


async def test_a_routed_inline_dispatch_runs_after_the_flush(executor: MagicMock) -> None:
    inline = InlineJobService(MagicMock(), executor)
    router = _router([], default=inline)

    handle = router.dispatch(_ReportJob)
    executor.execute.assert_not_awaited()
    await flush_pending_dispatches()

    assert handle.wait() == "in-process"
