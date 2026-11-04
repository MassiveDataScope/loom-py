"""``app.job_service: sqs``: each job leaves as one SQS message after the commit."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import AppConfig, JobBackendError, compose_application
from loom.core.config import ConfigContext
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitError, active_channel
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.envelope import decode_job_envelope
from loom.core.job.job import Job
from loom.core.job.router import JobRouter
from loom.core.job.service import JobService
from loom.core.tracing import get_trace_id, reset_trace_id, set_trace_id
from loom.core.use_case.use_case import UseCase
from loom.jobs.aws.sqs import (
    MAX_MESSAGE_BYTES,
    SQSDispatchError,
    SQSJobService,
    SQSQueueConfigError,
)
from loom.serverless.aws.lambda_ import LambdaSQSAdapter
from tests.helpers.composition import composition_source
from tests.helpers.extras import patched_aws_clients, without_boto3
from tests.helpers.jobs import BROKER, EmailJob, JobsAdapter, job_source
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory

pytestmark = pytest.mark.timeout(10)

_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/emails"
_FIFO_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo"
_CONFIG = {
    "aws": {"region": "eu-west-1"},
    "sqs": {"queues": {"emails": _URL, "orders": _FIFO_URL}},
}


class OrderJob(Job[None]):
    __queue__ = "orders"
    __group_by__ = "customer"

    def execute(self) -> None:
        return None


class UngroupedOrderJob(Job[None]):
    __queue__ = "orders"

    def execute(self) -> None:
        return None


class ReportJob(Job[None]):
    __queue__ = "reports"

    def execute(self) -> None:
        return None


class _Client:
    """Stands in for the boto3 ``sqs`` client: records each ``send_message``."""

    def __init__(self, error: Exception | None = None) -> None:
        self.requests: list[dict[str, Any]] = []
        self.sent = threading.Event()
        self._error = error

    def send_message(self, **request: Any) -> dict[str, str]:
        if self._error is not None:
            raise self._error
        self.requests.append(request)
        self.sent.set()
        return {"MessageId": "m-1"}


@pytest.fixture(autouse=True)
def _no_leftover_dispatches() -> Iterator[None]:
    yield
    clear_pending_dispatches()


@pytest.fixture
def client() -> _Client:
    return _Client()


@pytest.fixture
def service(tmp_path: Path, client: _Client) -> JobService:
    return _compose(tmp_path, _CONFIG, client, EmailJob, OrderJob)


def _compose(
    tmp_path: Path, config: dict[str, Any], client: _Client, *jobs: type[Job[Any]]
) -> JobService:
    with patched_aws_clients(client):
        application = compose_application(job_source(tmp_path, config, "sqs"), JobsAdapter(*jobs))
    return application.kernel.container.resolve(JobService)


async def test_a_standard_queue_gets_the_envelope_and_the_delay(
    service: JobService, client: _Client
) -> None:
    service.dispatch(EmailJob, payload={"to": "a@b.c"}, countdown=30)
    await flush_pending_dispatches()

    (request,) = client.requests
    assert (request["QueueUrl"], request["DelaySeconds"]) == (_URL, 30)
    assert "MessageGroupId" not in request
    envelope = decode_job_envelope(request["MessageBody"])
    assert (envelope.job_name, envelope.payload) == ("EmailJob", {"to": "a@b.c"})


async def test_a_fifo_queue_gets_group_and_deduplication_ids(
    service: JobService, client: _Client
) -> None:
    handle = service.dispatch(OrderJob, payload={"customer": 7})
    await flush_pending_dispatches()

    (request,) = client.requests
    assert request["QueueUrl"] == _FIFO_URL
    assert (request["MessageGroupId"], request["MessageDeduplicationId"]) == ("7", handle.job_id)
    assert "DelaySeconds" not in request


@pytest.mark.parametrize(
    ("sqs", "region"),
    [({}, "eu-west-1"), ({"region": "us-east-1"}, "us-east-1")],
    ids=["inherits aws", "overrides aws"],
)
def test_the_client_takes_aws_and_sqs_overrides_it(
    tmp_path: Path, sqs: dict[str, Any], region: str
) -> None:
    config = {"aws": {"region": "eu-west-1"}, "sqs": {**sqs, "queues": {"emails": _URL}}}
    source = job_source(tmp_path, config, "sqs")
    with patched_aws_clients() as clients:
        compose_application(source, JobsAdapter(EmailJob))

    assert clients.calls == [("sqs", region)]


@pytest.mark.parametrize(
    ("job", "message"),
    [
        (ReportJob, "queue 'reports', which sqs.queues does not map"),
        (UngroupedOrderJob, "FIFO queue 'orders' but declares no __group_by__"),
    ],
)
def test_startup_refuses_a_job_sqs_cannot_place(
    tmp_path: Path, job: type[Job[Any]], message: str
) -> None:
    source = job_source(tmp_path, _CONFIG, "sqs")
    adapter = JobsAdapter(job)
    with patched_aws_clients(), pytest.raises(SQSQueueConfigError, match=message):
        compose_application(source, adapter)


def test_jobs_on_celery_queues_need_no_sqs_url(tmp_path: Path) -> None:
    config = {**_CONFIG, "celery": {**BROKER, "queues": ["default", "reports"]}}
    source = job_source(tmp_path, config, "sqs")
    with patched_aws_clients():
        application = compose_application(source, JobsAdapter(EmailJob, ReportJob))

    assert isinstance(application.kernel.container.resolve(JobService), JobRouter)


def test_startup_without_the_extra_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    without_boto3(monkeypatch)
    source = job_source(tmp_path, _CONFIG, "sqs")
    adapter = JobsAdapter(EmailJob)
    with pytest.raises(JobBackendError, match=r"loom\[sqs\] extra is not installed"):
        compose_application(source, adapter)


_LATER = datetime.now(UTC) + timedelta(minutes=20)


@pytest.mark.parametrize(
    ("job", "options", "code"),
    [
        (EmailJob, {"on_success": object}, "sqs_callbacks"),
        (EmailJob, {"on_failure": object}, "sqs_callbacks"),
        (EmailJob, {"priority": 5}, "sqs_priority"),
        (EmailJob, {"queue": "reports"}, "sqs_queue_unknown"),
        (EmailJob, {"countdown": 901}, "sqs_delay_too_long"),
        (EmailJob, {"eta": _LATER}, "sqs_delay_too_long"),
        (OrderJob, {"payload": {"customer": 1}, "countdown": 5}, "sqs_fifo_delay"),
        (OrderJob, {"payload": {}}, "sqs_group_missing"),
        (EmailJob, {"payload": {"blob": "x" * MAX_MESSAGE_BYTES}}, "sqs_message_too_large"),
    ],
)
async def test_dispatch_refuses_what_sqs_cannot_carry_and_sends_nothing(
    service: JobService,
    client: _Client,
    job: type[Job[Any]],
    options: dict[str, Any],
    code: str,
) -> None:
    with pytest.raises(SQSDispatchError) as raised:
        service.dispatch(job, **options)
    await flush_pending_dispatches()

    assert (raised.value.code, raised.value.retryable) == (code, False)
    assert client.requests == []


@pytest.mark.parametrize("callback", ["on_all_success", "on_any_failure"])
def test_dispatch_parallel_refuses_callbacks(service: JobService, callback: str) -> None:
    jobs = [(EmailJob, {})]
    options = {callback: cast(Any, object)}
    with pytest.raises(SQSDispatchError, match="no callbacks"):
        service.dispatch_parallel(jobs, **options)


async def test_dispatch_parallel_sends_each_job(service: JobService, client: _Client) -> None:
    group = service.dispatch_parallel([(EmailJob, {}), (OrderJob, {"customer": 1})])
    await flush_pending_dispatches()

    assert len(group.handles) == 2
    assert [request["QueueUrl"] for request in client.requests] == [_URL, _FIFO_URL]


async def test_a_failed_send_names_job_task_and_queue_never_the_payload(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    service = _compose(tmp_path, _CONFIG, _Client(RuntimeError("throttled")), EmailJob)
    handle = service.dispatch(EmailJob, payload={"secret": "hunter2"})
    with pytest.raises(PostCommitError):
        await flush_pending_dispatches()

    (fields,) = [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == "SQS send failed"
    ]
    assert (fields["job"], fields["task_id"], fields["queue"]) == (
        "loom.job.EmailJob",
        handle.job_id,
        "emails",
    )
    assert all("hunter2" not in str(record.msg) for record in caplog.records)


class _Gate:
    def __init__(self) -> None:
        self.started, self.release = asyncio.Event(), asyncio.Event()

    async def __call__(self) -> None:
        self.started.set()
        await self.release.wait()


class _DispatchAfterCommit(UseCase[Any, None]):
    def __init__(self, service: SQSJobService, bump: _Gate) -> None:
        self.service = service
        self.bump = bump

    async def execute(self) -> None:
        channel = active_channel()
        assert channel is not None
        channel.enqueue_priority(self.bump)
        self.service.dispatch(EmailJob)


async def test_a_send_committed_before_the_cancellation_still_reaches_sqs(
    client: _Client,
) -> None:
    log = Log()
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(log))
    service = SQSJobService(client, {"emails": _URL}, factory=cast(Any, None), executor=executor)
    bump = _Gate()
    task = asyncio.create_task(executor.execute(_DispatchAfterCommit(service, bump)))
    await bump.started.wait()
    assert "uow.commit" in log.entries

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.requests == []
    bump.release.set()

    assert await asyncio.to_thread(client.sent.wait, 5)
    assert len(client.requests) == 1


class ProbeJob(Job[None]):
    seen: list[tuple[str, str, str | None]] = []

    def execute(self, marker: str) -> None:
        ProbeJob.seen.append((marker, current_identity().subject, get_trace_id()))


def test_the_lambda_sqs_adapter_runs_what_the_service_sent(
    tmp_path: Path, service: JobService, client: _Client
) -> None:
    identity, trace = set_identity(Identity(subject="alice")), set_trace_id("trace-1")
    try:
        service.dispatch(EmailJob.__mro__[0], queue="emails")
        cast(SQSJobService, service).dispatch(ProbeJob, params={"marker": "m"}, queue="emails")
    finally:
        reset_trace_id(trace)
        reset_identity(identity)
    asyncio.run(flush_pending_dispatches())
    ProbeJob.seen.clear()
    adapter = LambdaSQSAdapter(jobs=[ProbeJob, EmailJob])
    source = composition_source(
        ConfigContext.from_dict({"app": {"name": "lambda-sqs"}}),
        app=AppConfig(name="lambda-sqs", job_service="inline"),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    handler = adapter.bind(compose_application(source, adapter))
    records = [
        {"messageId": str(index), "body": request["MessageBody"], "eventSourceARN": "queue"}
        for index, request in enumerate(client.requests)
    ]
    try:
        result = handler({"Records": records}, None)
    finally:
        adapter.close()

    assert result == {"batchItemFailures": []}
    assert ProbeJob.seen == [("m", "alice", "trace-1")]
