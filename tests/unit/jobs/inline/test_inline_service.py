"""The inline job service runs a dispatched job in process, after the commit, once it is idle."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from loom.core.errors import JobFailedError, JobTimeoutError, SystemError
from loom.core.job.callback import JobCallback
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.delivery import JobDelivery
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment
from loom.core.use_case import Delivery
from tests.helpers.boundary import type_name
from tests.helpers.jobs import InlineServiceFixture, JobKernel


class FinalError(SystemError):
    is_retryable = False


class DoubleJob(Job[int]):
    ran: ClassVar[list[int]] = []

    async def execute(self, value: int = 0) -> int:
        self.ran.append(value)
        return value * 2


class SevenJob(Job[int]):
    def execute(self) -> int:
        return 7


class UpperJob(Job[str]):
    def execute(self) -> str:
        return "UP"


class FinalFailureJob(Job[None]):
    def execute(self) -> None:
        raise FinalError("boom")


class AbandoningJob(Job[None]):
    def execute(self) -> None:
        raise JobAbandonment("no tenant")


class DeliveryJob(Job[None]):
    seen: ClassVar[list[JobDelivery | None]] = []

    def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        self.seen.append(delivery)


class Hooks(JobCallback):
    calls: ClassVar[list[tuple[str, str]]] = []


class RecordingCallback(Hooks):
    def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
        self.calls.append(("success", job_id))

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
        self.calls.append(("failure", exc_type))


class BrokenCallback(Hooks):
    def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
        raise RuntimeError("callback broke")


@pytest.fixture(autouse=True)
def _forget_runs() -> None:
    for recorded in (DoubleJob.ran, DeliveryJob.seen, Hooks.calls):
        recorded.clear()


@pytest.fixture
def service() -> InlineServiceFixture:
    kernel = JobKernel()
    for job_type in (DoubleJob, SevenJob, UpperJob, FinalFailureJob, AbandoningJob, DeliveryJob):
        kernel.factory.register(job_type)
    for callback in (RecordingCallback, BrokenCallback):
        kernel.factory.register(callback)
    return InlineServiceFixture(kernel.factory, kernel.executor)


class TestRun:
    async def test_run_executes_immediately_and_returns_the_result(
        self, service: InlineServiceFixture
    ) -> None:
        assert await service.run(DoubleJob, params={"value": 5}) == 10


class TestPostCommit:
    def test_a_dispatch_does_not_run_before_the_commit(self, service: InlineServiceFixture) -> None:
        service.dispatch_after_commit(DoubleJob, params={"value": 1})

        assert DoubleJob.ran == []

    def test_the_handle_is_a_uuid_and_waiting_before_the_flush_times_out(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(DoubleJob)

        assert (isinstance(handle, JobHandle), len(handle.job_id)) == (True, 36)
        with pytest.raises(JobTimeoutError) as caught:
            handle.wait(timeout=1.0)
        assert caught.value.job_id == handle.job_id

    async def test_the_flush_runs_the_job_and_settles_the_handle(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(DoubleJob, params={"value": 21})
        await flush_pending_dispatches()
        await service.wait_until_idle()

        assert handle.wait() == 42

    async def test_the_job_reads_the_delivery_of_the_dispatch(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(DeliveryJob)
        await flush_pending_dispatches()
        await service.wait_until_idle()

        (delivery,) = DeliveryJob.seen
        assert delivery is not None
        assert (delivery.task_id, delivery.attempt, delivery.postponements) == (handle.job_id, 1, 0)

    async def test_a_failed_job_settles_the_handle_with_its_error_as_the_cause(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(FinalFailureJob)
        await flush_pending_dispatches()
        await service.wait_until_idle()

        with pytest.raises(JobFailedError) as caught:
            handle.wait()
        assert caught.value.job_id == handle.job_id
        assert isinstance(caught.value.cause, FinalError)
        assert caught.value.__cause__ is caught.value.cause

    async def test_a_job_abandonment_settles_the_handle_as_failed(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(AbandoningJob)
        await flush_pending_dispatches()
        await service.wait_until_idle()

        with pytest.raises(JobFailedError) as caught:
            handle.wait()
        assert isinstance(caught.value.cause, JobAbandonment)


class TestCallbacks:
    async def test_the_success_callback_runs_after_the_flush(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(
            DoubleJob, params={"value": 1}, on_success=RecordingCallback
        )
        await flush_pending_dispatches()
        await service.wait_until_idle()

        assert (handle.wait(), Hooks.calls) == (2, [("success", handle.job_id)])

    async def test_the_failure_callback_runs_when_the_job_fails(
        self, service: InlineServiceFixture
    ) -> None:
        service.dispatch_after_commit(FinalFailureJob, on_failure=RecordingCallback)
        await flush_pending_dispatches()
        await service.wait_until_idle()

        assert Hooks.calls == [("failure", type_name(FinalError))]

    async def test_a_failing_success_callback_keeps_the_job_result(
        self, service: InlineServiceFixture
    ) -> None:
        handle = service.dispatch_after_commit(
            DoubleJob, params={"value": 3}, on_success=BrokenCallback
        )
        await flush_pending_dispatches()
        await service.wait_until_idle()

        assert handle.wait() == 6


class TestParallel:
    def test_a_group_has_one_handle_per_job(self, service: InlineServiceFixture) -> None:
        group = service.dispatch_parallel([(SevenJob, {}), (UpperJob, {})])

        assert (isinstance(group, JobGroup), len(group.handles)) == (True, 2)

    async def test_every_job_of_the_group_runs_after_the_flush(
        self, service: InlineServiceFixture
    ) -> None:
        group = service.dispatch_parallel([(SevenJob, {}), (UpperJob, {})])
        await flush_pending_dispatches()
        await service.wait_until_idle()

        assert group.wait_all() == [7, "UP"]
