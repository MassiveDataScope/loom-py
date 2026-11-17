"""Dispatch API: countdown bound, eta gone, dispatch_now on every backend."""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.celery.service import CeleryJobService
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.policy import Countdown
from loom.core.job.router import JobRouter
from loom.core.job.service import InlineJobService, JobService
from loom.testing.jobs import RecordingJobService
from tests.helpers.jobs import EmailJob, SQSClient, sqs_service

pytestmark = pytest.mark.timeout(10)


@pytest.fixture(params=["inline", "celery", "sqs", "recording"])
def service(request: pytest.FixtureRequest) -> Iterator[JobService]:
    built: JobService
    match request.param:
        case "inline":
            executor = MagicMock()
            executor.run = AsyncMock(return_value="ran")
            built = InlineJobService(MagicMock(), executor)
        case "celery":
            built = CeleryJobService(MagicMock())
        case "sqs":
            built = sqs_service(SQSClient())
        case _:
            built = RecordingJobService()
    yield built
    clear_pending_dispatches()


class TestCountdownBound:
    @pytest.mark.parametrize("seconds", [0, Countdown.MAX_SECONDS])
    def test_dispatch_after_commit_accepts_the_bound(
        self, service: JobService, seconds: int
    ) -> None:
        service.dispatch_after_commit(EmailJob, countdown=seconds)

    @pytest.mark.parametrize("seconds", [-1, Countdown.MAX_SECONDS + 1])
    def test_dispatch_after_commit_rejects_outside_the_bound(
        self, service: JobService, seconds: int
    ) -> None:
        with pytest.raises(ValueError, match=str(Countdown.MAX_SECONDS)):
            service.dispatch_after_commit(EmailJob, countdown=seconds)

    @pytest.mark.parametrize("seconds", [-1, Countdown.MAX_SECONDS + 1])
    async def test_dispatch_now_rejects_outside_the_bound(
        self, service: JobService, seconds: int
    ) -> None:
        with pytest.raises(ValueError, match=str(Countdown.MAX_SECONDS)):
            await service.dispatch_now(EmailJob, countdown=seconds)


class TestEtaRemoved:
    def test_dispatch_after_commit_rejects_eta(self, service: JobService) -> None:
        with pytest.raises(TypeError, match="eta"):
            service.dispatch_after_commit(EmailJob, eta=object())  # type: ignore[call-arg]

    async def test_dispatch_now_rejects_eta(self, service: JobService) -> None:
        with pytest.raises(TypeError, match="eta"):
            await service.dispatch_now(EmailJob, eta=object())  # type: ignore[call-arg]


class TestDispatchNow:
    async def test_inline_runs_the_job_immediately(self) -> None:
        executor = MagicMock()
        executor.run = AsyncMock(return_value=7)
        service = InlineJobService(MagicMock(), executor)

        handle = await service.dispatch_now(EmailJob)

        executor.run.assert_awaited_once()
        assert handle.wait() == 7

    async def test_celery_sends_immediately(self) -> None:
        app = MagicMock()
        service = CeleryJobService(app)

        await service.dispatch_now(EmailJob)

        app.send_task.assert_called_once()
        await flush_pending_dispatches()
        assert app.send_task.call_count == 1

    async def test_sqs_sends_immediately(self) -> None:
        client = SQSClient()
        service = sqs_service(client)

        handle = await service.dispatch_now(EmailJob)

        assert len(client.requests) == 1
        assert handle.job_id
        await flush_pending_dispatches()
        assert len(client.requests) == 1

    async def test_recording_records_immediately(self) -> None:
        service = RecordingJobService()

        await service.dispatch_now(EmailJob, payload={"n": 1})

        assert [record.payload for record in service.dispatched] == [{"n": 1}]
        await flush_pending_dispatches()
        assert len(service.dispatched) == 1

    async def test_router_sends_through_the_backend(self) -> None:
        app = MagicMock()
        router = JobRouter({"emails": CeleryJobService(app)}, default=MagicMock())

        await router.dispatch_now(EmailJob)

        app.send_task.assert_called_once()
        router._default.dispatch_now.assert_not_called()
