from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.errors import JobFailedError, JobTimeoutError, SystemError
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment
from tests.helpers.jobs import InlineServiceFixture

# ---------------------------------------------------------------------------
# Minimal job fixtures
# ---------------------------------------------------------------------------


class DoubleJob(Job[int]):
    async def execute(self, value: int) -> int:  # type: ignore[override]
        return value * 2


class FinalError(SystemError):
    is_retryable = False


class UpperJob(Job[str]):
    def execute(self, text: str) -> str:  # type: ignore[override]
        return text.upper()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_service(return_value: Any = 42) -> InlineServiceFixture:
    factory = MagicMock()
    executor = MagicMock()
    executor.run = AsyncMock(return_value=return_value)
    factory.build = MagicMock(return_value=MagicMock())
    return InlineServiceFixture(factory=factory, executor=executor)


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------


class TestInProcessJobService:
    async def test_run_executes_immediately_and_returns_result(self) -> None:
        service = _make_service(return_value=10)
        result = await service.run(DoubleJob, params={"value": 5})
        assert result == 10
        service._executor.run.assert_awaited_once()  # type: ignore[attr-defined]

    # ---------------------------------------------------------------------------
    # dispatch_after_commit() — post-commit semantics
    # ---------------------------------------------------------------------------

    def test_dispatch_does_not_execute_immediately(self) -> None:
        service = _make_service()
        service.dispatch_after_commit(DoubleJob, payload={})
        service._executor.run.assert_not_awaited()  # type: ignore[attr-defined]

    def test_dispatch_returns_handle_with_uuid(self) -> None:
        service = _make_service()
        handle = service.dispatch_after_commit(DoubleJob)
        assert isinstance(handle, JobHandle)
        assert len(handle.job_id) == 36  # UUID4 format

    async def test_dispatch_then_flush_executes_and_settles_the_handle(self) -> None:
        service = _make_service(return_value=99)
        handle = service.dispatch_after_commit(DoubleJob, payload={})
        await flush_pending_dispatches()
        assert handle.wait() == 99

    def test_wait_before_flush_raises_job_timeout_error(self) -> None:
        service = _make_service()
        handle = service.dispatch_after_commit(DoubleJob, payload={})
        with pytest.raises(JobTimeoutError) as caught:
            handle.wait(timeout=1.0)
        assert caught.value.job_id == handle.job_id

    async def test_wait_on_a_failed_job_raises_job_failed_error_with_its_cause(self) -> None:
        service = _make_service()
        boom = FinalError("boom")
        service._executor.run.side_effect = boom  # type: ignore[attr-defined]
        handle = service.dispatch_after_commit(DoubleJob, payload={})
        await flush_pending_dispatches()
        with pytest.raises(JobFailedError) as caught:
            handle.wait()
        assert caught.value.job_id == handle.job_id
        assert caught.value.cause is boom
        assert caught.value.__cause__ is boom

    def test_dispatch_respects_queue_override(self) -> None:
        service = _make_service()
        handle = service.dispatch_after_commit(DoubleJob, queue="custom-queue")
        assert handle.queue == "custom-queue"

    def test_dispatch_uses_job_default_queue_when_not_overridden(self) -> None:
        service = _make_service()
        handle = service.dispatch_after_commit(DoubleJob)
        assert handle.queue == DoubleJob.__queue__

    # ---------------------------------------------------------------------------
    # Callbacks
    # ---------------------------------------------------------------------------

    async def test_dispatch_calls_on_success_callback_after_flush(self) -> None:
        success_calls: list[str] = []

        class RecordCallback:
            def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
                success_calls.append(job_id)

            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
                # No-op by design: this test validates the success callback path.
                return None

        factory = MagicMock()
        executor = MagicMock()
        executor.run = AsyncMock(return_value=7)
        factory.build = MagicMock(
            side_effect=lambda cls: RecordCallback() if cls is not DoubleJob else MagicMock()
        )

        service = InlineServiceFixture(factory=factory, executor=executor)
        handle = service.dispatch_after_commit(DoubleJob, on_success=RecordCallback)
        await flush_pending_dispatches()

        assert handle.wait() == 7
        assert len(success_calls) == 1

    async def test_dispatch_calls_on_failure_callback_on_exception(self) -> None:
        failure_calls: list[str] = []

        class ErrCallback:
            def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
                # No-op by design: this test validates the failure callback path.
                return None

            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
                failure_calls.append(exc_type)

        factory = MagicMock()
        executor = MagicMock()
        executor.run = AsyncMock(side_effect=FinalError("boom"))
        factory.build = MagicMock(
            side_effect=lambda cls: ErrCallback() if cls is not DoubleJob else MagicMock()
        )

        service = InlineServiceFixture(factory=factory, executor=executor)
        service.dispatch_after_commit(DoubleJob, on_failure=ErrCallback)

        # fire-and-forget: exception is silenced after the failure callback runs
        await flush_pending_dispatches()

        assert failure_calls == ["FinalError"]

    async def test_the_executor_gets_the_delivery_of_the_dispatch(self) -> None:
        executor = MagicMock(run=AsyncMock(return_value=1))
        service = InlineServiceFixture(factory=MagicMock(), executor=executor)
        handle = service.dispatch_after_commit(DoubleJob, payload={"value": 1})
        await flush_pending_dispatches()

        delivery = executor.run.await_args.kwargs["delivery"]
        assert (delivery.task_id, delivery.attempt, delivery.postponements) == (handle.job_id, 1, 0)
        assert delivery.envelope.task_id == handle.job_id

    async def test_a_job_abandonment_settles_the_handle_as_failed(self) -> None:
        executor = MagicMock(run=AsyncMock(side_effect=JobAbandonment("no tenant")))
        service = InlineServiceFixture(factory=MagicMock(), executor=executor)
        handle = service.dispatch_after_commit(DoubleJob, payload={"value": 1})
        await flush_pending_dispatches()

        with pytest.raises(JobFailedError) as raised:
            handle.wait()
        assert type(raised.value.cause).__name__ == "JobAbandonment"

    async def test_a_failing_success_callback_keeps_the_job_result(self) -> None:
        class BrokenCallback:
            def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
                raise RuntimeError("callback broke")

            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
                return None

        factory = MagicMock()
        executor = MagicMock()
        executor.run = AsyncMock(return_value=7)
        factory.build = MagicMock(
            side_effect=lambda cls: BrokenCallback() if cls is not DoubleJob else MagicMock()
        )

        service = InlineServiceFixture(factory=factory, executor=executor)
        handle = service.dispatch_after_commit(DoubleJob, on_success=BrokenCallback)
        await flush_pending_dispatches()

        assert handle.wait() == 7

    # ---------------------------------------------------------------------------
    # dispatch_parallel()
    # ---------------------------------------------------------------------------

    def test_dispatch_parallel_returns_job_group(self) -> None:
        service = _make_service(return_value=1)
        group = service.dispatch_parallel([(DoubleJob, {}), (UpperJob, {})])
        assert isinstance(group, JobGroup)
        assert len(group.handles) == 2

    async def test_dispatch_parallel_all_execute_after_flush(self) -> None:
        results = [10, 20]
        executor = MagicMock()
        executor.run = AsyncMock(side_effect=results)
        factory = MagicMock()
        factory.build = MagicMock(return_value=MagicMock())

        service = InlineServiceFixture(factory=factory, executor=executor)
        group = service.dispatch_parallel([(DoubleJob, {}), (UpperJob, {})])
        await flush_pending_dispatches()

        assert group.wait_all() == [10, 20]
