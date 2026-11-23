from __future__ import annotations

import logging
from typing import Any, ClassVar

import pytest

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import CompilationError, UseCaseCompiler
from loom.core.engine.events import EventKind, ExecutionEvent, Status
from loom.core.engine.executor import UseCaseExecutor
from loom.core.engine.observation import UseCaseEvent
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment, JobPostponement
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case import Delivery
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.kinds import declare_kinds
from loom.testing import JobTestData
from tests.helpers.jobs import InlineServiceFixture


class RecordingExecutionObserver:
    def __init__(self) -> None:
        self.events: list[ExecutionEvent] = []

    def on_event(self, event: ExecutionEvent) -> None:
        self.events.append(event)


class InnerJob(Job[None]):
    seen: ClassVar[list[JobDelivery | None]] = []

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        self.seen.append(delivery)


class OuterJob(Job[None]):
    seen: ClassVar[list[JobDelivery | None]] = []
    service: ClassVar[InlineServiceFixture]

    async def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        self.seen.append(delivery)
        await self.service.run(InnerJob)


class RetryingJob(Job[None]):
    async def execute(self) -> None:
        raise JobPostponement("later", 5)


class AbandoningJob(Job[None]):
    async def execute(self) -> None:
        raise JobAbandonment("never")


class TwoDeliveriesJob(Job[None]):
    async def execute(self, first: Any = Delivery(), second: Any = Delivery()) -> None:
        return None


@pytest.fixture
def observer() -> RecordingExecutionObserver:
    return RecordingExecutionObserver()


@pytest.fixture
def factory() -> UseCaseFactory:
    container = LoomContainer(classify_key=classify_key, implicit=main_repository)
    declare_kinds(container.kinds)
    factory = UseCaseFactory(container)
    for job_type in (InnerJob, OuterJob, RetryingJob, AbandoningJob):
        factory.register(job_type)
    return factory


@pytest.fixture
def executor(observer: RecordingExecutionObserver) -> UseCaseExecutor:
    return UseCaseExecutor(UseCaseCompiler(), execution_observer=observer)


class TestDeliveryMarker:
    async def test_a_job_receives_the_delivery_it_was_run_with(
        self, factory: UseCaseFactory, executor: UseCaseExecutor
    ) -> None:
        InnerJob.seen.clear()
        delivery = JobTestData().with_job_type(InnerJob).delivery(attempt=2, postponements=1)
        await executor.run(InnerJob, factory=factory, delivery=delivery)
        assert InnerJob.seen == [delivery]

    async def test_a_job_run_without_a_delivery_receives_none(
        self, factory: UseCaseFactory, executor: UseCaseExecutor
    ) -> None:
        InnerJob.seen.clear()
        await executor.run(InnerJob, factory=factory)
        assert InnerJob.seen == [None]

    async def test_a_job_run_by_another_job_does_not_see_the_delivery_of_its_caller(
        self, factory: UseCaseFactory, executor: UseCaseExecutor
    ) -> None:
        InnerJob.seen.clear()
        OuterJob.seen.clear()
        OuterJob.service = InlineServiceFixture(factory, executor)
        outer = JobTestData().with_job_type(OuterJob).delivery(attempt=2, postponements=1)

        await executor.run(OuterJob, factory=factory, delivery=outer)

        assert OuterJob.seen == [outer]
        assert InnerJob.seen == [None]

    def test_two_delivery_parameters_are_refused(self) -> None:
        with pytest.raises(CompilationError, match="only one Delivery"):
            UseCaseCompiler().compile(TwoDeliveriesJob)


class TestJobSignalReporting:
    @pytest.mark.parametrize(
        ("job_type", "status"),
        [(RetryingJob, Status.POSTPONED), (AbandoningJob, Status.ABANDONED)],
        ids=["retry", "abandonment"],
    )
    async def test_a_signal_is_not_logged_as_a_failure_nor_reported_as_one(
        self,
        factory: UseCaseFactory,
        executor: UseCaseExecutor,
        observer: RecordingExecutionObserver,
        caplog: pytest.LogCaptureFixture,
        job_type: type[Job[Any]],
        status: Status,
    ) -> None:
        with caplog.at_level(logging.DEBUG), pytest.raises((JobPostponement, JobAbandonment)):
            await executor.run(job_type, factory=factory)

        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
        assert not [r for r in caplog.records if r.getMessage() == UseCaseEvent.FAILED]
        assert any(r.getMessage() == UseCaseEvent.SIGNALLED for r in caplog.records)
        [reported] = [e for e in observer.events if e.kind is EventKind.EXEC_ERROR]
        assert reported.status is status
        assert reported.status is not Status.FAILURE
