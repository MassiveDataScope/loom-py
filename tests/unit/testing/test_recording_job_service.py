"""``RecordingJobService`` records what would be sent once the execution commits."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.composition import compose_application
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import ANONYMOUS, Identity
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase
from loom.testing import DispatchedJob, RecordingJobService
from tests.helpers.jobs import JobsAdapter, job_source
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory

pytestmark = pytest.mark.timeout(10)

_ALICE = Identity(subject="alice", mechanism="jwt")


class ShipJob(Job[str]):
    __queue__ = "shipping"

    def execute(self, order: int) -> str:
        return f"shipped {order}"


class _Ship(UseCase[Any, None]):
    def __init__(self, jobs: RecordingJobService, *, fail: bool) -> None:
        self.jobs = jobs
        self.fail = fail

    async def execute(self) -> None:
        self.jobs.dispatch(ShipJob, params={"order": 7}, payload={"note": "fragile"})
        if self.fail:
            raise RuntimeError("rolled back")


@pytest.fixture(autouse=True)
def _no_leftover_dispatches() -> Iterator[None]:
    yield
    clear_pending_dispatches()


@pytest.fixture
def jobs() -> RecordingJobService:
    return RecordingJobService()


@pytest.fixture
def executor() -> RuntimeExecutor:
    return RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())})
    )


async def test_a_committed_dispatch_is_recorded_with_its_caller(
    jobs: RecordingJobService, executor: RuntimeExecutor
) -> None:
    await executor.execute(_Ship(jobs, fail=False), identity=_ALICE)

    assert jobs.dispatched == [
        DispatchedJob(
            job_type=ShipJob,
            payload={"note": "fragile"},
            params={"order": 7},
            queue="shipping",
            identity=_ALICE,
        )
    ]


async def test_a_rolled_back_dispatch_is_not_recorded(
    jobs: RecordingJobService, executor: RuntimeExecutor
) -> None:
    failing = _Ship(jobs, fail=True)

    with pytest.raises(RuntimeError, match="rolled back"):
        await executor.execute(failing)

    assert jobs.dispatched == []


async def test_a_dispatch_outside_an_execution_is_recorded_on_flush(
    jobs: RecordingJobService,
) -> None:
    jobs.dispatch_parallel([(ShipJob, {"a": 1}), (ShipJob, {"b": 2})])
    assert jobs.dispatched == []

    await flush_pending_dispatches()

    assert [record.payload for record in jobs.dispatched] == [{"a": 1}, {"b": 2}]
    assert jobs.dispatched[0].identity is ANONYMOUS


@pytest.fixture
def composed(tmp_path: Path) -> tuple[RecordingJobService, type[Job[str]]]:
    """``app.job_service`` selects the service; ``jobs.<Job>.queue`` moves a job defined here."""

    class MovedJob(Job[str]):
        def execute(self) -> str:
            return "ran"

    config = {"jobs": {"MovedJob": {"queue": "bulk"}}}
    source = job_source(tmp_path, config, "loom.testing:RecordingJobService")
    application = compose_application(source, JobsAdapter(MovedJob))
    service = application.kernel.container.resolve(JobService)
    assert isinstance(service, RecordingJobService)
    return service, MovedJob


@pytest.mark.parametrize(("queue", "expected"), [(None, "bulk"), ("urgent", "urgent")])
async def test_the_recorded_queue_is_the_effective_one(
    composed: tuple[RecordingJobService, type[Job[str]]], queue: str | None, expected: str
) -> None:
    service, moved = composed
    service.dispatch(moved, queue=queue)
    await flush_pending_dispatches()

    assert [(record.job_type, record.queue) for record in service.dispatched] == [(moved, expected)]


async def test_a_composed_service_runs_a_job_in_process(
    composed: tuple[RecordingJobService, type[Job[str]]],
) -> None:
    service, moved = composed

    assert await service.run(moved) == "ran"


async def test_a_service_without_a_context_cannot_run(jobs: RecordingJobService) -> None:
    with pytest.raises(RuntimeError, match="built with a context"):
        await jobs.run(ShipJob)


async def test_a_payload_changed_after_dispatch_is_recorded_as_dispatched(
    jobs: RecordingJobService,
) -> None:
    payload, params = {"note": "fragile"}, {"order": 7}
    jobs.dispatch(ShipJob, payload=payload, params=params)
    payload["note"], params["order"] = "sturdy", 8

    await flush_pending_dispatches()

    assert (jobs.dispatched[0].payload, jobs.dispatched[0].params) == (
        {"note": "fragile"},
        {"order": 7},
    )
