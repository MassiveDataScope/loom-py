"""Backends the job outcome contract runs against: one driver per transport.

A transport that settles a delivery it is handed joins the settlement
contract by adding a driver here and its name to ``BACKENDS``; one that
redelivers differently from a delayed message (``Redelivery.SAME`` or
``Redelivery.NONE``) adds a driver here and its name to ``UNDELAYED_BACKENDS``.
The inline service starts every job at its first attempt and waits on a timer:
it cannot be handed an attempt or a number of postponements, so it has its own
contract in ``test_inline_contract``, over the same jobs.
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
from loom.core.job.job import Job
from loom.core.job.outcome import (
    Abandoned,
    Failed,
    JobAbandonment,
    JobOutcome,
    JobPostponement,
    Postponed,
    Succeeded,
)
from loom.core.job.policy import Redelivery
from loom.core.job.runner import JobRunner
from loom.jobs.aws.queue import (
    ABANDONMENT_REASON_ATTRIBUTE,
    POSTPONEMENTS_ATTRIBUTE,
    QueueConfig,
    SQSRecord,
)
from loom.jobs.aws.settlement import SQSJobSettlement
from loom.jobs.aws.sqs import SQSConfig
from loom.testing.jobs import JobTestData, ManualTimer
from tests.helpers.aws import ACCOUNT
from tests.helpers.aws_island import FakeAwsIsland
from tests.helpers.jobs import InlineServiceFixture, JobKernel
from tests.helpers.sqs import URL_BASE, FakeSqs

BACKENDS = ["envelope", "celery", "sqs"]
UNDELAYED_BACKENDS = ["envelope", "sqs-fifo"]


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
        self, job_type: type[Job[Any]], *, attempt: int = 1, postponements: int = 0
    ) -> Settlement: ...


class RunnerDriver:
    """Delivers through a :class:`JobRunner` over one way of invoking the job."""

    def __init__(
        self,
        invocation: Callable[[JobDelivery], Awaitable[Any]],
        kernel: JobKernel,
        redelivery: Redelivery = Redelivery.DELAYED,
    ) -> None:
        self._runner = JobRunner(invocation, redelivery=redelivery)
        self._kernel = kernel

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, postponements: int = 0
    ) -> Settlement:
        self._kernel.factory.register(job_type)
        delivery = (
            JobTestData()
            .with_job_type(job_type)
            .with_policy(JobSettings().of(job_type).retry.policy)
            .delivery(attempt=attempt, postponements=postponements)
        )
        return _settlement(await self._runner.run(delivery))


class SQSDriver:
    """Delivers a record through the runner and settles it with SQS, read off the calls made.

    SQS is told the delay of a failed or postponed message and the reason of a
    abandoned one; the reason of the others is the outcome's own.
    """

    def __init__(
        self,
        kernel: JobKernel,
        client: FakeSqs,
        settlement: SQSJobSettlement,
        redelivery: Redelivery,
        suffix: str,
    ) -> None:
        self._runner = JobRunner(kernel.invoke_envelope(), redelivery=redelivery)
        self._kernel = kernel
        self._client = client
        self._settlement = settlement
        self._suffix = suffix

    @property
    def queue(self) -> str:
        return f"{URL_BASE}/jobs{self._suffix}"

    @staticmethod
    @contextmanager
    def open(kernel: JobKernel, *, fifo: bool = False) -> Iterator[SQSDriver]:
        suffix = ".fifo" if fifo else ""
        queue = f"{URL_BASE}/jobs{suffix}"
        policy = {
            "deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq{suffix}",
            "maxReceiveCount": 3,
        }
        client = FakeSqs({queue: policy})
        config = SQSConfig(queues={"jobs": QueueConfig(url=queue)})
        with pytest.MonkeyPatch.context() as patch:
            FakeAwsIsland(client=client).install(patch)
            settlement = SQSJobSettlement(AwsClients(), config, "jobs")
            settlement.queue.read_dead_letter(client)
            redelivery = Redelivery.SAME if fifo else Redelivery.DELAYED
            yield SQSDriver(kernel, client, settlement, redelivery, suffix)

    async def deliver(
        self, job_type: type[Job[Any]], *, attempt: int = 1, postponements: int = 0
    ) -> Settlement:
        self._kernel.factory.register(job_type)
        record = SQSRecord.from_event(
            {
                "messageId": "message-1",
                "receiptHandle": "handle-1",
                "body": "{}",
                "attributes": {"ApproximateReceiveCount": str(attempt)},
                "messageAttributes": {
                    POSTPONEMENTS_ATTRIBUTE: {
                        "dataType": "Number",
                        "stringValue": str(postponements),
                    }
                },
                "eventSourceARN": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs{self._suffix}",
            }
        )
        data = (
            JobTestData()
            .with_job_type(job_type)
            .with_policy(JobSettings().of(job_type).retry.policy)
        )
        outcome = await self._runner.run(
            data.delivery(
                envelope=data.envelope(task_id=record.message_id),
                attempt=record.receive_count,
                postponements=record.postponements,
            )
        )
        self._client.batches.clear()
        redelivered = self._settlement.settle_all([(record, outcome)])
        return self._observed(outcome, redelivered)

    def _observed(self, outcome: JobOutcome, redelivered: list[SQSRecord]) -> Settlement:
        expected = _settlement(outcome)
        (visibility,) = self._client.entries("change_message_visibility_batch") or [None]
        (postponed_send,) = self._client.entries("send_message_batch", self.queue) or [None]
        (dead,) = self._client.entries(
            "send_message_batch", f"{URL_BASE}/jobs-dlq{self._suffix}"
        ) or [None]
        if visibility is not None:
            return Settlement("failed", visibility["VisibilityTimeout"], expected.reason)
        if postponed_send is not None:
            return Settlement("postponed", postponed_send["DelaySeconds"], expected.reason)
        if dead is not None:
            reason = dead["MessageAttributes"][ABANDONMENT_REASON_ATTRIBUTE]["StringValue"]
            return Settlement("abandoned", reason=reason)
        assert redelivered == []
        return Settlement("succeeded", result=expected.result)


def _settlement(outcome: JobOutcome) -> Settlement:
    match outcome:
        case Succeeded(result=result):
            return Settlement("succeeded", result=result)
        case Postponed(delay=delay, reason=reason):
            return Settlement("postponed", delay.seconds, reason)
        case Abandoned(reason=reason):
            return Settlement("abandoned", reason=reason)
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
        postponements: int = 0,
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
        return await asyncio.to_thread(self._run, task, attempt, postponements)

    @staticmethod
    def _run(task: Any, attempt: int, postponements: int) -> Settlement:
        kwargs = {"postponements": postponements}
        task.push_request(
            id="task-1",
            retries=attempt - 1 + postponements,
            kwargs=kwargs,
            is_eager=False,
            called_directly=False,
        )
        try:
            return Settlement("succeeded", result=task.run(**kwargs))
        except Retry as retry:
            delay = int(retry.when)
            error = retry.exc
            if isinstance(error, JobPostponement):
                return Settlement("postponed", delay, error.reason)
            return Settlement("failed", delay, type(error).__qualname__)
        except JobAbandonment as abandonment:
            return Settlement("abandoned", reason=abandonment.reason)
        except Exception as error:
            return Settlement("abandoned", reason=type(error).__qualname__)
        finally:
            task.pop_request()


@pytest.fixture(params=BACKENDS)
def driver(request: pytest.FixtureRequest) -> Iterator[Driver]:
    kernel = JobKernel()
    if request.param == "sqs":
        with SQSDriver.open(kernel) as sqs:
            yield sqs
        return
    if request.param == "celery":
        celery_driver = CeleryDriver(kernel)
        yield celery_driver
        celery_driver.close()
        return
    yield RunnerDriver(kernel.invoke_envelope(), kernel)


@contextmanager
def _undelayed(backend: str, redelivery: Redelivery) -> Iterator[Driver]:
    kernel = JobKernel()
    if backend == "sqs-fifo":
        with SQSDriver.open(kernel, fifo=True) as sqs:
            yield sqs
        return
    yield RunnerDriver(kernel.invoke_envelope(), kernel, redelivery)


@pytest.fixture
def same_message_driver(request: pytest.FixtureRequest) -> Iterator[Driver]:
    """A transport that brings the same message back, without a per-message delay."""
    with _undelayed(request.param, Redelivery.SAME) as driver:
        yield driver


@pytest.fixture
def no_redelivery_driver() -> Iterator[Driver]:
    """A transport that never brings a job back."""
    with _undelayed("envelope", Redelivery.NONE) as driver:
        yield driver


@dataclass
class Inline:
    """The real inline job service on a manual timer, over a real factory and executor."""

    service: InlineServiceFixture
    timer: ManualTimer


@pytest.fixture
def inline() -> Inline:
    kernel = JobKernel()
    timer = ManualTimer()
    return Inline(InlineServiceFixture(kernel.factory, kernel.executor, timer=timer), timer)
