"""Backends the job outcome contract runs against: one driver per transport.

A transport that settles a delivery it is handed joins the settlement
contract by adding a driver here and its name to ``BACKENDS``. The inline
service, which starts every job at its first attempt and waits on a timer, has
its own contract in ``test_inline_contract``, over the same jobs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

import pytest
from celery import Celery  # type: ignore[import-untyped]
from celery.exceptions import Retry  # type: ignore[import-untyped]

from loom.celery.runner import _CeleryAsyncRuntime, _make_job_task
from loom.core.connections.aws import AwsClients
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
from loom.core.job.runner import JobRunner
from loom.core.job.service import InlineJobService
from loom.jobs.aws.queue import (
    DEFERRALS_ATTRIBUTE,
    REJECTION_REASON_ATTRIBUTE,
    QueueConfig,
    SqsRecord,
)
from loom.jobs.aws.settlement import SqsSettlement
from loom.jobs.aws.sqs import SQSConfig
from loom.testing.jobs import ManualTimer
from tests.helpers.extras import patched_aws_clients
from tests.helpers.jobs import JobKernel
from tests.helpers.sqs import ACCOUNT, URL_BASE, FakeSqs

BACKENDS = ["envelope", "celery", "sqs"]


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


class RunnerDriver:
    """Delivers through a :class:`JobRunner` over one way of invoking the job."""

    def __init__(
        self, invocation: Callable[[JobDelivery], Awaitable[Any]], kernel: JobKernel
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


class SqsDriver:
    """Delivers a record through the runner and settles it with SQS, read off the calls made.

    SQS is told the delay of a failed or deferred message and the reason of a
    rejected one; the reason of the others is the outcome's own.
    """

    QUEUE = f"{URL_BASE}/jobs"

    def __init__(self, kernel: JobKernel, client: FakeSqs, settlement: SqsSettlement) -> None:
        self._runner = JobRunner(kernel.invoke_envelope())
        self._kernel = kernel
        self._client = client
        self._settlement = settlement

    @staticmethod
    @contextmanager
    def open(kernel: JobKernel) -> Iterator[SqsDriver]:
        policy = {
            "deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq",
            "maxReceiveCount": 3,
        }
        client = FakeSqs({SqsDriver.QUEUE: policy})
        config = SQSConfig(queues={"jobs": QueueConfig(url=SqsDriver.QUEUE)})
        with patched_aws_clients(client):
            settlement = SqsSettlement(AwsClients(), config, "jobs")
            settlement.queue.read_dead_letter(client)
            yield SqsDriver(kernel, client, settlement)

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, deferrals: int = 0
    ) -> Settlement:
        self._kernel.factory.register(job_type)
        record = SqsRecord.from_event(
            {
                "messageId": "message-1",
                "receiptHandle": "handle-1",
                "body": "{}",
                "attributes": {"ApproximateReceiveCount": str(attempt)},
                "messageAttributes": {
                    DEFERRALS_ATTRIBUTE: {"dataType": "Number", "stringValue": str(deferrals)}
                },
                "eventSourceARN": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs",
            }
        )
        outcome = await self._runner.run(
            JobDelivery(
                job_type=job_type,
                envelope=JobEnvelope(job=job_type.__qualname__),
                task_id=record.message_id,
                attempt=record.receive_count,
                deferrals=record.deferrals,
                policy=JobSettings().of(job_type).retry.policy,
            )
        )
        self._client.batches.clear()
        redelivered = self._settlement.settle_all([(record, outcome)])
        return self._observed(outcome, redelivered)

    def _observed(self, outcome: JobOutcome, redelivered: list[SqsRecord]) -> Settlement:
        expected = _settlement(outcome)
        (visibility,) = self._client.entries("change_message_visibility_batch") or [None]
        (resent,) = self._client.entries("send_message_batch", self.QUEUE) or [None]
        (dead,) = self._client.entries("send_message_batch", f"{URL_BASE}/jobs-dlq") or [None]
        if visibility is not None:
            return Settlement("failed", visibility["VisibilityTimeout"], expected.reason)
        if resent is not None:
            return Settlement("deferred", resent["DelaySeconds"], expected.reason)
        if dead is not None:
            reason = dead["MessageAttributes"][REJECTION_REASON_ATTRIBUTE]["StringValue"]
            return Settlement("rejected", reason=reason)
        assert redelivered == []
        return Settlement("succeeded", result=expected.result)


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

    def __init__(self, kernel: JobKernel) -> None:
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
def driver(request: pytest.FixtureRequest) -> Iterator[Driver]:
    kernel = JobKernel()
    if request.param == "sqs":
        with SqsDriver.open(kernel) as sqs:
            yield sqs
        return
    if request.param == "celery":
        celery_driver = CeleryDriver(kernel)
        yield celery_driver
        celery_driver.close()
        return
    yield RunnerDriver(kernel.invoke_envelope(), kernel)


@dataclass
class Inline:
    """The real inline job service on a manual timer, over a real factory and executor."""

    service: InlineJobService
    timer: ManualTimer


@pytest.fixture
def inline() -> Inline:
    kernel = JobKernel()
    timer = ManualTimer()
    return Inline(InlineJobService(kernel.factory, kernel.executor, timer=timer), timer)
