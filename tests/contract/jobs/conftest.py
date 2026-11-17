"""Backends the job outcome contract runs against: one driver per transport.

A backend joins the contract by adding a driver here and its name to
``BACKENDS``; the contract itself never changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from unittest.mock import MagicMock

import pytest

from loom.core.job.config import JobSettings
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope
from loom.core.job.job import Job
from loom.core.job.outcome import Deferred, Failed, JobOutcome, Rejected, Succeeded
from loom.core.job.service import InlineJobService

BACKENDS = ["inline"]


@dataclass(frozen=True)
class Settlement:
    """What a transport did with one delivery, in terms every transport shares."""

    kind: str
    delay_s: int | None = None
    reason: str | None = None
    result: Any = None


class Driver(Protocol):
    """Runs one delivery of a job the way a transport does and says how it settled."""

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, deferrals: int = 0
    ) -> Settlement: ...


class _ExecuteInPlace:
    """An executor running the job class itself, as the transports' executors do."""

    async def run(self, job_type: type[Job[Any]], **_: Any) -> Any:
        return await job_type().execute()


class InlineDriver:
    def __init__(self) -> None:
        self._service = InlineJobService(MagicMock(), _ExecuteInPlace())  # type: ignore[arg-type]

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, deferrals: int = 0
    ) -> Settlement:
        policy = JobSettings().of(job_type).retry.policy
        delivery = JobDelivery(
            job_type=job_type,
            envelope=JobEnvelope(job=job_type.__qualname__),
            task_id="task-1",
            attempt=attempt,
            deferrals=deferrals,
            policy=policy,
        )
        return _settlement(await self._service.runner.run(delivery))


def _settlement(outcome: JobOutcome) -> Settlement:
    match outcome:
        case Succeeded(result=result):
            return Settlement("succeeded", result=result)
        case Deferred(delay=delay, reason=reason):
            return Settlement("deferred", delay.seconds, reason)
        case Rejected(reason=reason):
            return Settlement("rejected", reason=reason)
        case Failed(error=error, delay=delay):
            return Settlement("failed", delay.seconds, type(error).__qualname__)


@pytest.fixture(params=BACKENDS)
def driver(request: pytest.FixtureRequest) -> Driver:
    return {"inline": InlineDriver}[request.param]()
