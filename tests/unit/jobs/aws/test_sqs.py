"""``app.job_service: sqs``: each job leaves as one SQS message after the commit."""

from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import JobBackendError, compose_application
from loom.core.config import ConfigError
from loom.core.engine import post_commit
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import (
    PostCommitChannel,
    PostCommitError,
    active_channel,
    bind_channel,
    reset_channel,
)
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.identity.wire import decode_identity
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.envelope import decode_job_envelope
from loom.core.job.job import Job
from loom.core.job.router import JobRouter
from loom.core.job.service import JobService
from loom.core.tracing import reset_trace_id, set_trace_id
from loom.core.use_case.use_case import UseCase
from loom.jobs.aws.sqs import (
    MAX_MESSAGE_BYTES,
    SQSDispatchError,
    SQSJobService,
    SQSQueueConfigError,
)
from tests.helpers.extras import patched_aws_clients, without_boto3
from tests.helpers.jobs import (
    BROKER,
    SQS_FIFO_URL,
    SQS_URL,
    EmailJob,
    JobsAdapter,
    SQSClient,
    job_source,
    sqs_service,
)
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory

pytestmark = pytest.mark.timeout(10)

_UNREPORTED = "PostCommitActionFailedAfterInterruption"
_NEVER_RAN = "PostCommitShieldedActionNeverRan"

_URL, _FIFO_URL = SQS_URL, SQS_FIFO_URL
_SEND_FAILED = "SQS send failed"
_CONFIG = {"aws": {"region": "eu-west-1"}, "sqs": {"queues": {"emails": _URL, "orders": _FIFO_URL}}}


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


class _ClientError(Exception):
    """botocore-shaped: its message echoes a payload value, its response holds the code."""

    def __init__(self) -> None:
        super().__init__("InvalidMessageContents: hunter2")
        self.response = {"Error": {"Code": "InvalidMessageContents"}}


@pytest.fixture(autouse=True)
def _no_leftover_dispatches() -> Iterator[None]:
    yield
    clear_pending_dispatches()


@pytest.fixture
def client() -> SQSClient:
    return SQSClient()


@pytest.fixture
def service(tmp_path: Path, client: SQSClient) -> JobService:
    return _compose(tmp_path, _CONFIG, client, EmailJob, OrderJob)


def _compose(
    tmp_path: Path, config: dict[str, Any], client: SQSClient, *jobs: type[Job[Any]]
) -> JobService:
    with patched_aws_clients(client):
        application = compose_application(job_source(tmp_path, config, "sqs"), JobsAdapter(*jobs))
    return application.kernel.container.resolve(JobService)


def _sqs_config(**knobs: Any) -> dict[str, Any]:
    return {**_CONFIG, "sqs": {**_CONFIG["sqs"], **knobs}}


async def _sent(
    service: JobService, client: SQSClient, job: type[Job[Any]], **options: Any
) -> tuple[str, dict[str, Any]]:
    handle = service.dispatch(job, **options)
    await flush_pending_dispatches()
    (request,) = client.requests
    return handle.job_id, request


async def test_a_standard_queue_gets_the_envelope_and_the_delay(
    service: JobService, client: SQSClient
) -> None:
    identity, trace = set_identity(Identity(subject="alice")), set_trace_id("trace-1")
    try:
        _, request = await _sent(service, client, EmailJob, payload={"to": "a@b.c"}, countdown=30)
    finally:
        reset_trace_id(trace)
        reset_identity(identity)

    assert (request["QueueUrl"], request["DelaySeconds"]) == (_URL, 30)
    assert "MessageGroupId" not in request
    envelope = decode_job_envelope(request["MessageBody"])
    assert (envelope.job_name, envelope.payload, envelope.trace_id) == (
        "EmailJob",
        {"to": "a@b.c"},
        "trace-1",
    )
    assert getattr(decode_identity(envelope.identity), "subject", None) == "alice"


async def test_control_characters_and_emoji_reach_sqs(
    service: JobService, client: SQSClient
) -> None:
    note = "\x00\x1f\t \U0001f600"
    _, request = await _sent(service, client, EmailJob, payload={"note": note})

    assert decode_job_envelope(request["MessageBody"]).payload == {"note": note}


@pytest.mark.parametrize("customer", [7, "a b \U0001f600", "x" * 500])
async def test_a_fifo_queue_gets_a_hashed_group_and_the_task_as_deduplication_id(
    service: JobService, client: SQSClient, customer: object
) -> None:
    task_id, request = await _sent(service, client, OrderJob, payload={"customer": customer})

    assert request["QueueUrl"] == _FIFO_URL
    assert request["MessageGroupId"] == hashlib.sha256(str(customer).encode()).hexdigest()
    assert request["MessageDeduplicationId"] == task_id
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


async def test_the_jobs_section_gives_the_queue_the_delay_and_the_priority(
    tmp_path: Path, client: SQSClient
) -> None:
    """``reports`` has no URL: the job starts and is sent only because its section moves it."""
    section = {"ReportJob": {"queue": "emails", "countdown": 7, "priority": 3}}
    service = _compose(tmp_path, {**_CONFIG, "jobs": section}, client, ReportJob)

    _, request = await _sent(service, client, ReportJob, priority=3)

    assert (request["QueueUrl"], request["DelaySeconds"]) == (_URL, 7)
    assert (ReportJob.__queue__, ReportJob.__countdown__) == ("reports", 0)


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


_LATER = timedelta(minutes=20)
"""An ``eta`` this far ahead, taken from the clock when the test runs."""


@pytest.mark.parametrize(
    ("job", "options", "code"),
    [
        (EmailJob, {"on_success": object}, "sqs_callbacks"),
        (EmailJob, {"priority": 5}, "sqs_priority"),
        (EmailJob, {"queue": "reports"}, "sqs_queue_unknown"),
        (EmailJob, {"countdown": 901}, "sqs_delay_too_long"),
        (EmailJob, {"eta": _LATER}, "sqs_delay_too_long"),
        (OrderJob, {"payload": {"customer": 1}, "countdown": 5}, "sqs_fifo_delay"),
        (OrderJob, {"payload": {}}, "sqs_group_missing"),
        (OrderJob, {"payload": {"customer": None}}, "sqs_group_missing"),
        (EmailJob, {"payload": {"note": "\ufffe"}}, "sqs_invalid_character"),
        (EmailJob, {"payload": {"note": "\uffff"}}, "sqs_invalid_character"),
        (EmailJob, {"payload": {"blob": "x" * MAX_MESSAGE_BYTES}}, "sqs_message_too_large"),
    ],
)
async def test_dispatch_refuses_what_sqs_cannot_carry_and_sends_nothing(
    service: JobService,
    client: SQSClient,
    job: type[Job[Any]],
    options: dict[str, Any],
    code: str,
) -> None:
    if "eta" in options:
        options = {**options, "eta": datetime.now(UTC) + options["eta"]}
    with pytest.raises(SQSDispatchError) as raised:
        service.dispatch(job, **options)
    await flush_pending_dispatches()

    assert (raised.value.code, raised.value.retryable) == (code, False)
    assert client.requests == []


def test_dispatch_parallel_refuses_callbacks(service: JobService) -> None:
    jobs = [(EmailJob, {})]
    callback = cast(Any, object)
    with pytest.raises(SQSDispatchError, match="no callbacks"):
        service.dispatch_parallel(jobs, on_all_success=callback)


def _logged(caplog: pytest.LogCaptureFixture, event: str) -> list[dict[str, Any]]:
    return [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == event
    ]


def _failed_sends(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str, str, str]]:
    return [
        (fields["job"], fields["task_id"], fields["queue"], fields["error_code"])
        for fields in _logged(caplog, _SEND_FAILED)
    ]


async def test_a_failed_send_names_job_task_queue_and_code(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    service = _compose(tmp_path, _sqs_config(batch=False), SQSClient(_ClientError()), EmailJob)
    handle = service.dispatch(EmailJob)
    with pytest.raises(PostCommitError) as raised:
        await flush_pending_dispatches()

    (failure,) = raised.value.failures
    assert isinstance(failure, SQSDispatchError)
    assert (failure.code, failure.__cause__, failure.__suppress_context__) == (
        "sqs_send_failed",
        None,
        True,
    )
    assert _failed_sends(caplog) == [
        ("loom.job.EmailJob", handle.job_id, "emails", "InvalidMessageContents")
    ]


@pytest.mark.parametrize(
    ("batch", "error", "refusals"),
    [(False, True, 0), (True, False, 2), (True, True, 0)],
    ids=["single send fails", "entry refused twice", "batch call fails"],
)
async def test_a_failed_send_never_logs_or_raises_the_payload(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, batch: bool, error: bool, refusals: int
) -> None:
    client = SQSClient(_ClientError() if error else None)
    service = _compose(tmp_path, _sqs_config(batch=batch), client, EmailJob)
    client.refuse[service.dispatch(EmailJob, payload={"secret": "hunter2"}).job_id] = refusals
    with pytest.raises(PostCommitError) as raised:
        await flush_pending_dispatches()

    assert "hunter2" not in f"{raised.value.failures!r} {[r.msg for r in caplog.records]}"


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


@pytest.mark.parametrize("batch", [True, False], ids=["batch", "one send each"])
async def test_a_send_committed_before_the_cancellation_still_reaches_sqs(
    client: SQSClient, batch: bool
) -> None:
    log = Log()
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=StubUnitOfWorkFactory(log))
    service = sqs_service(client, batch=batch)
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


async def test_one_execution_sends_one_batch_per_queue_in_dispatch_order(
    service: JobService, client: SQSClient
) -> None:
    ids = [
        service.dispatch(job, payload={"customer": n}).job_id
        for n, job in enumerate([EmailJob, OrderJob, EmailJob, OrderJob, EmailJob])
    ]
    await flush_pending_dispatches()

    assert client.batches == [(_URL, ids[0::2]), (_FIFO_URL, ids[1::2])]
    fifo = [request for request in client.requests if request["QueueUrl"] == _FIFO_URL]
    assert [(r["MessageGroupId"], r["MessageDeduplicationId"]) for r in fifo] == [
        (hashlib.sha256(str(n).encode()).hexdigest(), ids[n]) for n in (1, 3)
    ]


@pytest.mark.parametrize(
    ("knobs", "blob", "sizes"),
    [
        ({"batch_size": 3}, "", [3, 3, 1]),
        ({}, "", [7]),
        ({}, "x" * 250_000, [4, 3]),
    ],
    ids=["by batch_size", "up to ten", "by total bytes"],
)
async def test_a_queue_is_sent_in_chunks_bounded_by_count_and_bytes(
    tmp_path: Path, client: SQSClient, knobs: dict[str, Any], blob: str, sizes: list[int]
) -> None:
    service = _compose(tmp_path, _sqs_config(**knobs), client, EmailJob)
    ids = [service.dispatch(EmailJob, payload={"blob": blob}).job_id for _ in range(7)]
    await flush_pending_dispatches()

    assert [len(chunk) for _, chunk in client.batches] == sizes
    assert [task for _, chunk in client.batches for task in chunk] == ids


async def test_batch_false_sends_each_job_with_its_own_send_message(
    tmp_path: Path, client: SQSClient
) -> None:
    service = _compose(tmp_path, _sqs_config(batch=False), client, EmailJob)
    service.dispatch(EmailJob)
    service.dispatch(EmailJob)
    await flush_pending_dispatches()

    assert (len(client.requests), client.batches) == (2, [])
    assert all("Id" not in request for request in client.requests)


@pytest.mark.parametrize("concurrent", [1, 2])
async def test_max_concurrent_sends_bounds_the_queues_sent_at_once(
    tmp_path: Path, concurrent: int
) -> None:
    client = SQSClient(overlap=0.5)
    service = _compose(
        tmp_path, _sqs_config(max_concurrent_sends=concurrent), client, EmailJob, OrderJob
    )
    service.dispatch(EmailJob)
    service.dispatch(OrderJob, payload={"customer": 1})
    await flush_pending_dispatches()

    assert (client.peak, len(client.requests)) == (concurrent, 2)


@pytest.mark.parametrize(
    "knobs",
    [{"batch_size": 0}, {"batch_size": 11}, {"max_concurrent_sends": 0}, {"batch": "yes"}],
)
def test_startup_refuses_an_invalid_batch_knob(tmp_path: Path, knobs: dict[str, Any]) -> None:
    source = job_source(tmp_path, _sqs_config(**knobs), "sqs")
    adapter = JobsAdapter(EmailJob)
    with patched_aws_clients(), pytest.raises(ConfigError, match=next(iter(knobs))):
        compose_application(source, adapter)


async def test_an_entry_sqs_refuses_once_is_sent_again(
    service: JobService, client: SQSClient
) -> None:
    first, second = (service.dispatch(EmailJob).job_id for _ in range(2))
    client.refuse[first] = 1
    await flush_pending_dispatches()

    assert client.batches == [(_URL, [first, second]), (_URL, [first])]
    assert len(client.requests) == 2


async def test_an_entry_refused_twice_fails_the_batch_naming_job_task_and_code(
    service: JobService, client: SQSClient, caplog: pytest.LogCaptureFixture
) -> None:
    refused = service.dispatch(EmailJob).job_id
    service.dispatch(EmailJob)
    client.refuse[refused] = 2
    with pytest.raises(PostCommitError) as raised:
        await flush_pending_dispatches()

    (failure,) = raised.value.failures
    assert isinstance(failure, SQSDispatchError)
    assert failure.code == "sqs_batch_partial_failure"
    assert failure.message == "1 of 2 SQS sends failed: InternalError"
    assert _failed_sends(caplog) == [("loom.job.EmailJob", refused, "emails", "InternalError")]


async def test_a_fifo_queue_resends_nothing_and_holds_back_what_would_overtake(
    tmp_path: Path, client: SQSClient, caplog: pytest.LogCaptureFixture
) -> None:
    service = _compose(tmp_path, _sqs_config(batch_size=2), client, OrderJob)
    ids = [service.dispatch(OrderJob, payload={"customer": "c"}).job_id for _ in range(5)]
    client.refuse[ids[1]] = 1
    with pytest.raises(PostCommitError) as raised:
        await flush_pending_dispatches()

    (failure,) = raised.value.failures
    assert isinstance(failure, SQSDispatchError)
    assert failure.message == "4 of 5 SQS sends failed: InternalError, NotSent"
    assert client.batches == [(_FIFO_URL, ids[:2])]
    assert [(task, code) for _, task, _, code in _failed_sends(caplog)] == [
        (ids[1], "InternalError"),
        *((task, "NotSent") for task in ids[2:]),
    ]


async def test_a_failed_batch_call_is_not_sent_again(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    client = SQSClient(_ClientError())
    service = _compose(tmp_path, _CONFIG, client, EmailJob)
    ids = [service.dispatch(EmailJob).job_id for _ in range(2)]
    with pytest.raises(PostCommitError) as raised:
        await flush_pending_dispatches()

    (failure,) = raised.value.failures
    assert isinstance(failure, SQSDispatchError)
    assert (failure.code, client.batches) == ("sqs_batch_partial_failure", [(_URL, ids)])
    assert [task for _, task, _, _ in _failed_sends(caplog)] == ids


async def test_a_rolled_back_dispatch_leaves_no_batch_behind(
    service: JobService, client: SQSClient
) -> None:
    service.dispatch(EmailJob)
    clear_pending_dispatches()
    kept = service.dispatch(EmailJob).job_id
    await flush_pending_dispatches()

    assert client.batches == [(_URL, [kept])]


def _queued(service: SQSJobService, channel: PostCommitChannel, *jobs: type[Job[Any]]) -> list[str]:
    token = bind_channel(channel)
    try:
        payload = {"customer": "hunter2", "secret": "hunter2"}
        return [service.dispatch(job, payload=payload).job_id for job in jobs]
    finally:
        reset_channel(token)


async def _kill_the_shield(drain: asyncio.Task[None]) -> list[asyncio.Task[None]]:
    """Cancel the drain, then its shielded tasks (what closing the loop does); return them."""
    drain.cancel()
    with pytest.raises(asyncio.CancelledError):
        await drain
    loop = asyncio.get_running_loop()
    shielded = [task for task in post_commit._shielded_tasks if task.get_loop() is loop]
    for task in shielded:
        task.cancel()
    return shielded


async def _until_logged(caplog: pytest.LogCaptureFixture, event: str) -> None:
    while event not in caplog.text:
        await asyncio.sleep(0.01)


async def test_a_batch_failing_after_the_cancellation_logs_each_job_and_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    channel, bump = PostCommitChannel(), _Gate()
    ids = _queued(sqs_service(SQSClient(_ClientError())), channel, EmailJob, EmailJob)
    channel.enqueue_priority(bump)
    task = asyncio.create_task(channel.drain(committed=True))
    await bump.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    bump.release.set()
    await _until_logged(caplog, _UNREPORTED)

    logged = [(f["job_name"], f["task_id"]) for f in _logged(caplog, _UNREPORTED)]
    assert logged == [("loom.job.EmailJob", task) for task in ids]
    assert "hunter2" not in caplog.text


async def test_a_dead_batch_holds_its_slot_until_the_running_call_returns(
    client: SQSClient, caplog: pytest.LogCaptureFixture
) -> None:
    client.hold = threading.Event()
    channel = PostCommitChannel()
    sent, unsent = _queued(sqs_service(client), channel, EmailJob, OrderJob)
    drain = asyncio.create_task(channel.drain(committed=True))
    assert await asyncio.to_thread(client.entered.wait, 5)
    await _kill_the_shield(drain)
    await asyncio.sleep(0.1)

    assert (post_commit._shielded_tasks != set(), _NEVER_RAN in caplog.text) == (True, False)
    client.hold.set()
    await _until_logged(caplog, _NEVER_RAN)

    assert [fields["task_id"] for fields in _logged(caplog, _NEVER_RAN)] == [unsent]
    assert client.batches == [(_URL, [sent])]
    assert "hunter2" not in caplog.text


async def test_a_call_still_waiting_for_a_thread_when_the_batch_dies_sends_nothing(
    client: SQSClient, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    waiting, thread_free = asyncio.Event(), asyncio.Event()

    async def to_thread(func: Callable[..., Any], /, *args: Any) -> Any:
        """A thread pool whose only thread is busy until ``thread_free``."""
        waiting.set()
        await thread_free.wait()
        return func(*args)

    monkeypatch.setattr(asyncio, "to_thread", to_thread)
    channel = PostCommitChannel()
    (queued,) = _queued(sqs_service(client), channel, EmailJob)
    drain = asyncio.create_task(channel.drain(committed=True))
    await waiting.wait()
    shielded = await _kill_the_shield(drain)
    thread_free.set()
    await asyncio.gather(*shielded, return_exceptions=True)

    assert [fields["task_id"] for fields in _logged(caplog, _NEVER_RAN)] == [queued]
    assert (client.entered.is_set(), client.batches) == (False, [])
