"""``RecordingJobTransport`` records what would be sent once the execution commits."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import ANONYMOUS, Identity
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import OrderedCommits
from loom.core.use_case.use_case import UseCase
from loom.testing import RecordingJobTransport
from tests.helpers.jobs import JobsAdapter, job_project
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory

pytestmark = pytest.mark.timeout(10)

_ALICE = Identity(subject="alice", mechanism="jwt")


class ShipJob(Job[str]):
    __queue__ = "shipping"

    def execute(self, order: int) -> str:
        return f"shipped {order}"


class _Ship(UseCase[Any, None]):
    def __init__(self, jobs: JobService, *, fail: bool) -> None:
        self.jobs = jobs
        self.fail = fail

    async def execute(self) -> None:
        self.jobs.dispatch_after_commit(ShipJob, params={"order": 7}, payload={"note": "fragile"})
        if self.fail:
            raise RuntimeError("rolled back")


@pytest.fixture(autouse=True)
def _no_leftover_dispatches() -> Iterator[None]:
    yield
    clear_pending_dispatches()


@pytest.fixture
def transport() -> RecordingJobTransport:
    return RecordingJobTransport()


@pytest.fixture
def service(transport: RecordingJobTransport) -> JobService:
    return JobService(transport)


@pytest.fixture
def executor() -> RuntimeExecutor:
    return RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())})
    )


def _composed_service(tmp_path: Path, config: dict[str, Any], *jobs: type[Job[Any]]) -> JobService:
    """``app.job_service`` selects the recording transport; ``jobs.<Job>`` configures a job."""
    project = job_project(tmp_path, config, "loom.testing:RecordingJobTransport")
    application = project.compose(JobsAdapter(*jobs))
    return application.kernel.container.resolve(JobService)


class TestRecordingJobTransport:
    async def test_a_committed_dispatch_is_recorded_with_its_caller(
        self, service: JobService, transport: RecordingJobTransport, executor: RuntimeExecutor
    ) -> None:
        await executor.execute(_Ship(service, fail=False), identity=_ALICE)

        (recorded,) = transport.dispatched
        assert (
            recorded.job_type,
            recorded.payload,
            recorded.params,
            recorded.queue,
            recorded.countdown,
            recorded.identity,
        ) == (ShipJob, {"note": "fragile"}, {"order": 7}, "shipping", 0, _ALICE)

    async def test_an_anonymous_caller_is_recorded_as_no_caller(
        self, service: JobService, transport: RecordingJobTransport, executor: RuntimeExecutor
    ) -> None:
        """The capture every transport makes: the job wire carries no anonymous caller."""
        await executor.execute(_Ship(service, fail=False), identity=ANONYMOUS)

        assert [record.identity for record in transport.dispatched] == [None]

    async def test_a_rolled_back_dispatch_is_not_recorded(
        self, service: JobService, transport: RecordingJobTransport, executor: RuntimeExecutor
    ) -> None:
        failing = _Ship(service, fail=True)

        with pytest.raises(RuntimeError, match="rolled back"):
            await executor.execute(failing)

        assert transport.dispatched == []

    @pytest.mark.parametrize(("queue", "expected"), [(None, "bulk"), ("urgent", "urgent")])
    async def test_the_recorded_queue_is_the_effective_one(
        self, tmp_path: Path, queue: str | None, expected: str
    ) -> None:
        class MovedJob(Job[str]):
            def execute(self) -> str:
                return "ran"

        service = _composed_service(tmp_path, {"jobs": {"MovedJob": {"queue": "bulk"}}}, MovedJob)

        service.dispatch_after_commit(MovedJob, queue=queue)
        await flush_pending_dispatches()

        recorded = service.default_transport.dispatched  # type: ignore[attr-defined]
        assert [(record.job_type, record.queue) for record in recorded] == [(MovedJob, expected)]

    async def test_a_composed_service_runs_a_job_in_process(self, tmp_path: Path) -> None:
        class MovedJob(Job[str]):
            def execute(self) -> str:
                return "ran"

        service = _composed_service(tmp_path, {}, MovedJob)

        assert await service.run(MovedJob) == "ran"

    async def test_a_payload_changed_after_dispatch_is_recorded_as_dispatched(
        self, service: JobService, transport: RecordingJobTransport
    ) -> None:
        payload, params = {"note": "fragile"}, {"order": 7}
        service.dispatch_after_commit(ShipJob, payload=payload, params=params)
        payload["note"], params["order"] = "sturdy", 8

        await flush_pending_dispatches()

        assert (transport.dispatched[0].payload, transport.dispatched[0].params) == (
            {"note": "fragile"},
            {"order": 7},
        )


class TestRecordedCountdown:
    async def test_a_configured_countdown_overrides_the_class(self, tmp_path: Path) -> None:
        class TimedJob(Job[None]):
            __countdown__ = 3

            def execute(self) -> None:
                return None

        service = _composed_service(tmp_path, {"jobs": {"TimedJob": {"countdown": 7}}}, TimedJob)
        service.dispatch_after_commit(TimedJob)
        await flush_pending_dispatches()
        await service.dispatch_now(TimedJob, countdown=2)

        recorded = service.default_transport.dispatched  # type: ignore[attr-defined]
        assert [record.countdown for record in recorded] == [7, 2]
