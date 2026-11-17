"""Backends the job outcome contract runs against: one driver per transport.

A backend joins the contract by adding a driver here and its name to
``BACKENDS``; the contract itself never changes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

import pytest

from loom.core.connections.aws import AwsClients
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
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.kinds import declare_kinds
from loom.jobs.aws.queue import (
    DEFERRALS_ATTRIBUTE,
    REJECTION_REASON_ATTRIBUTE,
    QueueConfig,
    SqsRecord,
)
from loom.jobs.aws.settlement import SqsSettlement
from loom.jobs.aws.sqs import SQSConfig
from tests.helpers.extras import patched_aws_clients
from tests.helpers.sqs import ACCOUNT, URL_BASE, FakeSqs

BACKENDS = ["inline", "envelope", "sqs"]


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


class SqsDriver:
    """Delivers a record through the runner and settles it with SQS, read off the calls made.

    SQS is told the delay of a failed or deferred message and the reason of a
    rejected one; the reason of the others is the outcome's own.
    """

    QUEUE = f"{URL_BASE}/jobs"

    def __init__(self, kernel: _Kernel, client: FakeSqs, settlement: SqsSettlement) -> None:
        self._runner = JobRunner(kernel.invoke_envelope())
        self._kernel = kernel
        self._client = client
        self._settlement = settlement

    @staticmethod
    @contextmanager
    def open(kernel: _Kernel) -> Iterator[SqsDriver]:
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


@pytest.fixture(params=BACKENDS)
def driver(request: pytest.FixtureRequest) -> Iterator[Driver]:
    kernel = _Kernel()
    if request.param == "sqs":
        with SqsDriver.open(kernel) as sqs:
            yield sqs
        return
    invocations = {"inline": kernel.invoke_directly, "envelope": kernel.invoke_envelope}
    yield RunnerDriver(invocations[request.param](), kernel)
