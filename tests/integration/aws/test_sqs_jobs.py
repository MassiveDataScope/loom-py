"""``app.job_service: sqs`` through a real queue: sent after commit, run by the Lambda adapter."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from botocore.client import BaseClient  # type: ignore[import-untyped]

from loom.composition import Application, build_application
from loom.core.job.envelope import decode_job_envelope
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.use_case.use_case import UseCase
from loom.serverless.aws.lambda_ import LambdaAdapter
from loom.testing import lambda_event
from tests.helpers.jobs import JobsAdapter
from tests.integration.aws.conftest import AwsClientFactory, ConfigWriter, unique

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]

_runs: list[tuple[str, str]] = []


class WelcomeJob(Job[None]):
    __queue__ = "emails"

    def execute(self, marker: str) -> None:
        _runs.append(("WelcomeJob", marker))


class OrderJob(Job[None]):
    __queue__ = "orders"
    __group_by__ = "customer"

    def execute(self, marker: str) -> None:
        _runs.append(("OrderJob", marker))


class _Rollback(Exception):
    """Fails the execution after its dispatch."""


class Dispatch(UseCase[Any, str]):
    """Dispatch the job its params name; ``fail`` fails the execution afterwards."""

    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self, job: str, marker: str, customer: str, fail: bool) -> str:
        job_type = WelcomeJob if job == "WelcomeJob" else OrderJob
        handle = self._jobs.dispatch(
            job_type, params={"marker": marker}, payload={"customer": customer}
        )
        if fail:
            raise _Rollback
        return handle.job_id


class DispatchMany(UseCase[Any, list[str]]):
    """Dispatch ``count`` jobs of one customer in one execution, marked by their order."""

    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self, job: str, count: int) -> list[str]:
        job_type = WelcomeJob if job == "WelcomeJob" else OrderJob
        return [
            self._jobs.dispatch(
                job_type, params={"marker": str(n)}, payload={"customer": "c-7"}
            ).job_id
            for n in range(count)
        ]


@dataclass(frozen=True)
class _Queue:
    url: str
    arn: str


@dataclass(frozen=True)
class _Queues:
    standard: _Queue
    fifo: _Queue

    def of(self, job_type: type[Job[Any]]) -> _Queue:
        return self.fifo if job_type is OrderJob else self.standard


@pytest.fixture(autouse=True)
def _forget_runs() -> Iterator[None]:
    _runs.clear()
    yield
    _runs.clear()


@pytest.fixture
def sqs(aws: AwsClientFactory) -> Any:
    return aws("sqs")


@pytest.fixture
def queues(sqs: Any) -> Iterator[_Queues]:
    created = _Queues(
        standard=_create_queue(sqs, unique("loom-it-emails"), {}),
        fifo=_create_queue(sqs, f"{unique('loom-it-orders')}.fifo", {"FifoQueue": "true"}),
    )
    yield created
    for queue in (created.standard, created.fifo):
        sqs.delete_queue(QueueUrl=queue.url)


def _create_queue(sqs: Any, name: str, attributes: dict[str, str]) -> _Queue:
    url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]
    return _Queue(url=url, arn=arn["QueueArn"])


@pytest.fixture
def config_path(write_config: ConfigWriter, queues: _Queues) -> Path:
    return write_config(
        {
            "app": {"name": "loom-it-sqs", "job_service": "sqs"},
            "sqs": {"queues": {"emails": queues.standard.url, "orders": queues.fifo.url}},
            "lambda": {"functions": {"worker": {"trigger": {"source": "jobs"}}}},
        }
    )


@pytest.fixture
def producer(config_path: Path, tmp_path: Path) -> Application:
    return build_application(
        str(config_path),
        adapter=JobsAdapter(WelcomeJob, OrderJob),
        environment="test",
        code_path=str(tmp_path),
    )


def _dispatch(producer: Application, job_type: type[Job[Any]], *, fail: bool = False) -> str:
    use_case = producer.kernel.factory.build(Dispatch)
    params = {"job": job_type.__qualname__, "marker": "m-1", "customer": "c-7", "fail": fail}
    result: str = asyncio.run(producer.kernel.executor.execute(use_case, params=params))
    return result


def _receive(sqs: Any, queue: _Queue, wait: int = 5) -> list[dict[str, Any]]:
    response = sqs.receive_message(
        QueueUrl=queue.url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=wait,
        MessageSystemAttributeNames=["All"],
    )
    messages: list[dict[str, Any]] = response.get("Messages", [])
    return messages


@pytest.mark.parametrize("job_type", [WelcomeJob, OrderJob], ids=["standard", "fifo"])
def test_a_committed_dispatch_reaches_the_queue_as_a_job_envelope(
    producer: Application, sqs: Any, queues: _Queues, job_type: type[Job[Any]]
) -> None:
    job_id = _dispatch(producer, job_type)

    (message,) = _receive(sqs, queues.of(job_type))
    envelope = decode_job_envelope(message["Body"])
    assert (envelope.job_name, envelope.params, envelope.payload) == (
        job_type.__qualname__,
        {"marker": "m-1"},
        {"customer": "c-7"},
    )
    if job_type is OrderJob:
        attributes = message["Attributes"]
        assert attributes["MessageGroupId"] == hashlib.sha256(b"c-7").hexdigest()
        assert attributes["MessageDeduplicationId"] == job_id


@pytest.mark.parametrize("job_type", [WelcomeJob, OrderJob], ids=["standard", "fifo"])
def test_a_failed_execution_sends_nothing(
    producer: Application, sqs: Any, queues: _Queues, job_type: type[Job[Any]]
) -> None:
    with pytest.raises(_Rollback):
        _dispatch(producer, job_type, fail=True)

    assert _receive(sqs, queues.of(job_type), wait=1) == []


@pytest.mark.parametrize("job_type", [WelcomeJob, OrderJob], ids=["standard", "fifo"])
def test_the_lambda_sqs_adapter_runs_the_received_message(
    producer: Application,
    sqs: Any,
    queues: _Queues,
    config_path: Path,
    tmp_path: Path,
    job_type: type[Job[Any]],
) -> None:
    _dispatch(producer, job_type)
    queue = queues.of(job_type)
    (message,) = _receive(sqs, queue)
    event = lambda_event(
        "jobs", body=message["Body"], message_id=message["MessageId"], queue_arn=queue.arn
    )
    adapter = LambdaAdapter(function="worker", jobs=[WelcomeJob, OrderJob])
    consumer = build_application(
        str(config_path), adapter=adapter, environment="test", code_path=str(tmp_path)
    )
    handler = adapter.bind(consumer)
    try:
        result = handler(event, None)
    finally:
        adapter.close()

    assert result == {"batchItemFailures": []}
    assert _runs == [(job_type.__qualname__, "m-1")]


@pytest.fixture
def sqs_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The name of each SQS send operation any boto3 client makes."""
    calls: list[str] = []
    make_api_call = BaseClient._make_api_call

    def spy(client: Any, operation: str, params: dict[str, Any]) -> Any:
        if operation.startswith("SendMessage"):
            calls.append(operation)
        return make_api_call(client, operation, params)

    monkeypatch.setattr(BaseClient, "_make_api_call", spy)
    return calls


def _drain(sqs: Any, queue: _Queue, count: int) -> list[str]:
    """Receive and delete until *count* messages arrived; their markers in arrival order."""
    markers: list[str] = []
    deadline = time.monotonic() + 30
    while len(markers) < count and time.monotonic() < deadline:
        for message in _receive(sqs, queue, wait=1):
            markers.append(str(decode_job_envelope(message["Body"]).params["marker"]))
            sqs.delete_message(QueueUrl=queue.url, ReceiptHandle=message["ReceiptHandle"])
    return markers


@pytest.mark.parametrize("job_type", [WelcomeJob, OrderJob], ids=["standard", "fifo"])
def test_one_execution_sends_its_jobs_in_batches_of_ten(
    producer: Application,
    sqs: Any,
    queues: _Queues,
    sqs_calls: list[str],
    job_type: type[Job[Any]],
) -> None:
    use_case = producer.kernel.factory.build(DispatchMany)
    params = {"job": job_type.__qualname__, "count": 25}
    asyncio.run(producer.kernel.executor.execute(use_case, params=params))

    assert sqs_calls == ["SendMessageBatch"] * 3
    markers = _drain(sqs, queues.of(job_type), 25)
    arrived = markers if job_type is OrderJob else sorted(markers, key=int)
    assert arrived == [str(n) for n in range(25)]
