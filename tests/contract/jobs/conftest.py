"""Backends the job outcome contract runs against: one driver per transport.

A backend joins the contract by adding a driver here and its name to
``BACKENDS``; the contract itself never changes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Generator
from dataclasses import dataclass
from typing import Any, Protocol

import pytest
from celery import Celery  # type: ignore[import-untyped]
from celery.exceptions import Retry  # type: ignore[import-untyped]

from loom.celery.runner import _CeleryAsyncRuntime, _make_job_task
from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.config import JobSettings
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import JobEnvelope
from loom.core.job.job import Job
from loom.core.job.outcome import (
    Deferred,
    Failed,
    JobOutcome,
    JobRejection,
    JobRetry,
    Rejected,
    Succeeded,
)
from loom.core.job.run import EnvelopeInvocation
from loom.core.job.runner import JobRunner
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.kinds import declare_kinds

BACKENDS = ["inline", "envelope", "celery"]


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

    def invoke_directly(self) -> Callable[[JobDelivery], Awaitable[Any]]:
        async def invoke(delivery: JobDelivery) -> Any:
            envelope = delivery.envelope
            return await self.executor.run(
                delivery.job_type,
                factory=self.factory,
                params=envelope.params,
                payload=envelope.payload,
                delivery=delivery,
            )

        return invoke

    def invoke_envelope(self) -> Callable[[JobDelivery], Awaitable[Any]]:
        return EnvelopeInvocation(self.factory, self.executor, ObservabilityRuntime.noop())


class RunnerDriver:
    """Delivers through a :class:`JobRunner` over one way of invoking the job."""

    def __init__(
        self, invocation: Callable[[JobDelivery], Awaitable[Any]], kernel: _Kernel
    ) -> None:
        self._runner = JobRunner(invocation)
        self._kernel = kernel

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, deferrals: int = 0
    ) -> Settlement:
        self._kernel.factory.register(job_type)
        delivery = JobDelivery(
            job_type=job_type,
            envelope=JobEnvelope(job=job_type.__qualname__),
            task_id="task-1",
            attempt=attempt,
            deferrals=deferrals,
            policy=JobSettings().of(job_type).retry.policy,
        )
        return _settlement(await self._runner.run(delivery))


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


class CeleryDriver:
    """Delivers through the Celery task a worker runs, the way its request carries the attempt."""

    def __init__(self, kernel: _Kernel) -> None:
        self._kernel = kernel
        self._runtime = _CeleryAsyncRuntime()
        self._runtime.initialize()
        self._app = Celery(broker="memory://", backend="cache+memory://")

    def close(self) -> None:
        self._runtime.shutdown()

    async def deliver(
        self,
        job_type: type[Job[Any]],
        *,
        attempt: int = 1,
        deferrals: int = 0,
    ) -> Settlement:
        self._kernel.factory.register(job_type)
        task = _make_job_task(
            self._app,
            job_type,
            self._kernel.factory,
            self._kernel.executor,
            self._runtime,
            settings=JobSettings(),
        )
        return await asyncio.to_thread(self._run, task, attempt, deferrals)

    @staticmethod
    def _run(task: Any, attempt: int, deferrals: int) -> Settlement:
        kwargs = {"deferrals": deferrals}
        task.push_request(
            id="task-1",
            retries=attempt - 1 + deferrals,
            kwargs=kwargs,
            is_eager=False,
            called_directly=False,
        )
        try:
            return Settlement("succeeded", result=task.run(**kwargs))
        except Retry as retry:
            delay = int(retry.when)
            error = retry.exc
            if isinstance(error, JobRetry):
                return Settlement("deferred", delay, error.reason)
            return Settlement("failed", delay, type(error).__qualname__)
        except JobRejection as rejection:
            return Settlement("rejected", reason=rejection.reason)
        except Exception as error:
            return Settlement("rejected", reason=type(error).__qualname__)
        finally:
            task.pop_request()


@pytest.fixture(params=BACKENDS)
def driver(request: pytest.FixtureRequest) -> Generator[Driver]:
    kernel = _Kernel()
    if request.param == "celery":
        celery_driver = CeleryDriver(kernel)
        yield celery_driver
        celery_driver.close()
        return
    invocations = {"inline": kernel.invoke_directly, "envelope": kernel.invoke_envelope}
    yield RunnerDriver(invocations[request.param](), kernel)
