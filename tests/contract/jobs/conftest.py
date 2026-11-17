"""Backends the job outcome contract runs against: one driver per transport.

A backend joins the contract by adding a driver here and its name to
``BACKENDS``; the contract itself never changes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

import pytest

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.config import JobSettings
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope
from loom.core.job.job import Job
from loom.core.job.outcome import Deferred, Failed, JobOutcome, Rejected, Succeeded
from loom.core.job.run import EnvelopeInvocation
from loom.core.job.runner import JobRunner
from loom.core.job.service import InlineJobService
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.kinds import declare_kinds
from loom.testing.jobs import ManualTimer

BACKENDS = ["inline", "envelope"]


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


class _Kernel:
    """A real factory and executor, serving the jobs a test delivers."""

    def __init__(self) -> None:
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        declare_kinds(container.kinds)
        self.factory = UseCaseFactory(container)
        self.executor = RuntimeExecutor(UseCaseCompiler())

    def delivery(self, job_type: type[Job[Any]], *, attempt: int, deferrals: int) -> JobDelivery:
        self.factory.register(job_type)
        return JobDelivery(
            job_type=job_type,
            envelope=JobEnvelope(job=job_type.__qualname__),
            task_id="task-1",
            attempt=attempt,
            deferrals=deferrals,
            policy=JobSettings().of(job_type).retry.policy,
        )

    def invoke_envelope(self) -> Callable[[JobDelivery], Awaitable[Any]]:
        return EnvelopeInvocation(self.factory, self.executor, ObservabilityRuntime.noop())


class RunnerDriver:
    """Delivers through a :class:`JobRunner` over the envelope invocation of the transports."""

    def __init__(self, kernel: _Kernel) -> None:
        self._runner = JobRunner(kernel.invoke_envelope())
        self._kernel = kernel

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, deferrals: int = 0
    ) -> Settlement:
        delivery = self._kernel.delivery(job_type, attempt=attempt, deferrals=deferrals)
        return _settlement(await self._runner.run(delivery))


class InlineDriver:
    """Delivers through the real :class:`InlineJobService`, its follow-ups on a manual timer."""

    def __init__(self, kernel: _Kernel) -> None:
        self.timer = ManualTimer()
        self._service = InlineJobService(kernel.factory, kernel.executor, timer=self.timer)
        self._kernel = kernel

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, deferrals: int = 0
    ) -> Settlement:
        delivery = self._kernel.delivery(job_type, attempt=attempt, deferrals=deferrals)
        return _settlement(await self._service.deliver(delivery))


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


@pytest.fixture
def inline_driver() -> InlineDriver:
    return InlineDriver(_Kernel())


@pytest.fixture(params=BACKENDS)
def driver(request: pytest.FixtureRequest) -> Driver:
    if request.param == "inline":
        return request.getfixturevalue("inline_driver")  # type: ignore[no-any-return]
    return RunnerDriver(_Kernel())
