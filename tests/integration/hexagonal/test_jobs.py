"""Acme shipping's job on the inline job service: dispatched, run and gathered as each caller.

No transport is mounted: the jobs run in process, on the inline job service
``app.job_service: inline`` composes. H1 on inline jobs, H7 over concurrent
runs, and the job a caller-less dispatch runs failing closed.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom.composition import Application
from loom.core.engine.observation import UseCaseEvent
from loom.core.errors import Unauthenticated
from loom.core.execution import ExecutionRequired
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.job import JobService
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.handle import JobFailedError, JobHandle
from loom.core.log_names import LogField
from tests.helpers.loggers import CapturedLog
from tests.integration.hexagonal.conftest import (
    ANA,
    BO,
    CALLERS,
    PARAMS,
    Compose,
    Seen,
    acted_as,
    notice_for,
)
from tests.integration.hexagonal.shipping import ShipmentQueries, TrackShipmentJob


@pytest.fixture
def application(compose: Compose) -> Application:
    """Acme shipping composed with no transport, its jobs run by the inline job service."""
    return compose("inline-jobs")[0]


async def _dispatched_by(application: Application, caller: Identity | None) -> JobHandle[str]:
    """Dispatch the job as *caller* and return its handle once the job has run."""
    token = set_identity(caller)
    try:
        handle = application.jobs.dispatch_after_commit(TrackShipmentJob, params=PARAMS)
    finally:
        reset_identity(token)
    await flush_pending_dispatches()
    await application.jobs.wait_until_idle()
    return handle


async def _run_as(application: Application, caller: Identity, shipment_id: str) -> str:
    set_identity(caller)
    service = application.kernel.container.resolve(JobService)
    return await service.run(TrackShipmentJob, params={"shipment_id": shipment_id})


def _failed_jobs(caplog: pytest.LogCaptureFixture) -> list[dict[str, Any]]:
    return [
        entry
        for _, entry in CapturedLog(caplog).events
        if entry.get(LogField.USE_CASE) == TrackShipmentJob.__name__
        and entry.get(LogField.STATUS) == "failure"
    ]


class TestTrackShipmentJobInline:
    @CALLERS
    async def test_runs_through_adapters_acting_as_the_dispatcher(
        self, application: Application, seen: Seen, caller: Identity
    ) -> None:
        async with application.running():
            handle = await _dispatched_by(application, caller)

        assert handle.wait() == notice_for(caller)
        assert seen.calls == acted_as(caller)

    async def test_two_concurrent_runs_never_see_each_others_caller(
        self, application: Application, seen: Seen
    ) -> None:
        seen.rendezvous = asyncio.Barrier(2)

        async with application.running():
            ana, bo = await asyncio.gather(
                _run_as(application, ANA, "ACME-1"), _run_as(application, BO, "ACME-2")
            )

        assert (ana, bo) == (notice_for(ANA), notice_for(BO, "ACME-2"))
        assert sorted(seen.calls, key=repr) == sorted(acted_as(ANA, BO), key=repr)


class TestACallerlessDispatch:
    async def test_fails_the_job_before_reaching_an_adapter(
        self, application: Application, seen: Seen, caplog: pytest.LogCaptureFixture
    ) -> None:
        async with application.running():
            await _dispatched_by(application, None)

        (failure,) = _failed_jobs(caplog)
        assert CapturedLog(caplog).exception_types(UseCaseEvent.FAILED) == [Unauthenticated] * 2
        assert "error" not in failure
        assert seen.calls == []

    async def test_waits_into_job_failed_error(self, application: Application) -> None:
        async with application.running():
            handle = await _dispatched_by(application, None)

        with pytest.raises(JobFailedError):
            handle.wait()


class TestAnAdapterOutsideAnExecution:
    async def test_fails_closed_when_used(self, application: Application, seen: Seen) -> None:
        async with application.running():
            queries = application.kernel.container.resolve(ShipmentQueries)
            with pytest.raises(ExecutionRequired):
                await queries.status_of("ACME-1")

        assert seen.calls == []
