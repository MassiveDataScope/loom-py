"""``app.job_service: sqs``: each job is one SQS message whose body is a ``JobEnvelope``."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import math
import re
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, TypeVar

import msgspec

from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClientConfig, aws_error_code
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.post_commit import log_never_ran
from loom.core.errors import LoomError
from loom.core.identity import current_identity
from loom.core.identity.wire import encode_identity
from loom.core.job.context import add_pending_publish, pending_publish_group
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.logger import get_logger
from loom.core.model.types import PositiveInt
from loom.core.tracing import get_trace_id

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.callback import JobCallback
    from loom.core.use_case.factory import UseCaseFactory

ResultT = TypeVar("ResultT")

MAX_DELAY_SECONDS = 900
MAX_MESSAGE_BYTES = 256 * 1024
MAX_BATCH_ENTRIES = 10
MAX_BATCH_BYTES = 1024 * 1024
"""What one ``SendMessageBatch`` call carries at most, in entries and in body bytes."""
_INVALID_CHARACTER = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]")
"""A character SQS refuses in a message body."""

_log = get_logger(__name__)


class SQSConfig(AwsClientConfig, frozen=True, kw_only=True):
    """The ``sqs`` section: ``aws:`` client keys plus each logical queue's URL (``.fifo``: FIFO).

    ``batch`` sends an execution's jobs in ``SendMessageBatch`` calls of at most
    ``batch_size`` entries, ``max_concurrent_sends`` queues at a time;
    ``batch: false`` sends each with its own ``SendMessage``.
    """

    queues: dict[str, str] = {}
    batch: bool = True
    batch_size: Annotated[int, msgspec.Meta(ge=1, le=MAX_BATCH_ENTRIES)] = MAX_BATCH_ENTRIES
    max_concurrent_sends: PositiveInt = 1


class SQSQueueConfigError(ConfigError):
    """A job's queue has no URL in ``sqs.queues``, or its FIFO queue no ``__group_by__``."""


class SQSDispatchError(LoomError):
    """A message SQS cannot carry, or a send that failed; ``code`` names which."""

    retryable = False


def check_queues(queues: Mapping[str, str], jobs: Sequence[type[Job[Any]]]) -> None:
    """Fail at startup unless every job's queue has a URL and a FIFO job names its group."""
    for job_type in jobs:
        url = queues.get(job_type.__queue__)
        if url is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses queue {job_type.__queue__!r}, which "
                f"sqs.queues does not map to a URL"
            )
        if url.endswith(".fifo") and job_type.__group_by__ is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses FIFO queue {job_type.__queue__!r} but "
                "declares no __group_by__ payload field"
            )


@dataclass(frozen=True)
class _PendingSQSSend:
    """One ``SendMessage`` on the publish lane; ``task_name``/``task_id`` name it in logs."""

    client: Any
    task_name: str
    task_id: str
    queue: str
    request: dict[str, Any] = field(repr=False)

    async def __call__(self) -> None:
        try:
            await asyncio.to_thread(self.client.send_message, **self.request)
        except Exception as exc:
            # botocore's message may echo the body: neither it nor the cause leaves here.
            code = aws_error_code(exc)
            self.log_failure(code)
            raise SQSDispatchError(
                f"sending {self.task_name} to queue {self.queue!r} failed: {code}",
                code="sqs_send_failed",
            ) from None

    def log_failure(self, code: str) -> None:
        _log.error(
            "SQS send failed",
            job=self.task_name,
            task_id=self.task_id,
            queue=self.queue,
            error_code=code,
        )

    def batch_entry(self) -> dict[str, Any]:
        return {"Id": self.task_id} | {
            key: value for key, value in self.request.items() if key != "QueueUrl"
        }


_Failure = tuple[_PendingSQSSend, str]
_NOT_SENT = "NotSent"
"""The code of a FIFO entry held back because an earlier one of its queue failed."""


@dataclass(frozen=True)
class _PendingSQSBatch:
    """Every send of one post-commit drain, as ``SendMessageBatch`` calls per queue, in order."""

    client: Any
    batch_size: int
    max_concurrent_sends: int
    sends: list[_PendingSQSSend] = field(default_factory=list, repr=False)
    handed: set[str] = field(default_factory=set, repr=False)
    """Task ids already handed to an SQS call."""

    @property
    def task_name(self) -> str:
        return ",".join(send.task_name for send in self.sends)

    @property
    def task_id(self) -> str:
        return ",".join(send.task_id for send in self.sends)

    async def __call__(self) -> None:
        by_queue: dict[str, list[_PendingSQSSend]] = {}
        for send in self.sends:
            by_queue.setdefault(send.request["QueueUrl"], []).append(send)
        limit = asyncio.Semaphore(self.max_concurrent_sends)
        try:
            results = await asyncio.gather(
                *(self._send_queue(url, sends, limit) for url, sends in by_queue.items())
            )
        except BaseException:
            for send in self.sends:
                if send.task_id not in self.handed:
                    log_never_ran(send)
            raise
        failed = [failure for failures in results for failure in failures]
        if not failed:
            return
        for send, code in failed:
            send.log_failure(code)
        codes = ", ".join(sorted({code for _, code in failed}))
        raise SQSDispatchError(
            f"{len(failed)} of {len(self.sends)} SQS sends failed: {codes}",
            code="sqs_batch_partial_failure",
        )

    async def _send_queue(
        self, url: str, sends: list[_PendingSQSSend], limit: asyncio.Semaphore
    ) -> list[_Failure]:
        """One chunk after the other, so a FIFO queue receives the jobs in dispatch order.

        A FIFO queue resends nothing and stops at its first failure: a later
        message would overtake the failed one.
        """
        fifo = url.endswith(".fifo")
        failed: list[_Failure] = []
        async with limit:
            for chunk in _chunks(sends, self.batch_size):
                if fifo and failed:
                    failed += [(send, _NOT_SENT) for send in chunk]
                else:
                    failed += await self._send_chunk(url, chunk, retry=not fifo)
        return failed

    async def _send_chunk(
        self, url: str, chunk: list[_PendingSQSSend], *, retry: bool
    ) -> list[_Failure]:
        """Send ``chunk``; the entries SQS refuses are sent once more when ``retry``."""
        entries = [send.batch_entry() for send in chunk]
        self.handed.update(send.task_id for send in chunk)
        try:
            response = await asyncio.to_thread(
                self.client.send_message_batch, QueueUrl=url, Entries=entries
            )
        except Exception as exc:
            # botocore already retried the call, and its message may echo a body.
            code = aws_error_code(exc)
            return [(send, code) for send in chunk]
        by_id = {send.task_id: send for send in chunk}
        failed = [(by_id[entry["Id"]], entry["Code"]) for entry in response.get("Failed", [])]
        if failed and retry:
            return await self._send_chunk(url, [send for send, _ in failed], retry=False)
        return failed


def _chunks(sends: list[_PendingSQSSend], size: int) -> Iterator[list[_PendingSQSSend]]:
    """``sends`` in order, at most ``size`` entries and ``MAX_BATCH_BYTES`` of bodies a chunk."""
    chunk: list[_PendingSQSSend] = []
    total = 0
    for send in sends:
        body = len(send.request["MessageBody"].encode())
        if chunk and (len(chunk) == size or total + body > MAX_BATCH_BYTES):
            yield chunk
            chunk, total = [], 0
        chunk.append(send)
        total += body
    if chunk:
        yield chunk


class SQSJobService:
    """``JobService`` sending each job to SQS after the commit; :meth:`run` is in-process."""

    def __init__(
        self,
        client: Any,
        queues: Mapping[str, str],
        *,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        metrics: MetricsAdapter | None = None,
        batch_size: int | None = MAX_BATCH_ENTRIES,
        max_concurrent_sends: int = 1,
    ) -> None:
        """``batch_size=None`` sends each job with its own ``SendMessage``."""
        self._client = client
        self._queues = dict(queues)
        self._batch_size = batch_size
        self._max_concurrent_sends = max_concurrent_sends
        self._factory = factory
        self._executor = executor
        self._metrics = metrics

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        instance = self._factory.build(job_type)
        return await self._executor.execute(
            instance, params=params, payload=payload, identity=current_identity()
        )

    def dispatch(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        on_success: type[JobCallback] | None = None,
        on_failure: type[JobCallback] | None = None,
        queue: str | None = None,
        countdown: int | None = None,
        priority: int | None = None,
        eta: datetime | None = None,
    ) -> JobHandle[ResultT]:
        """Validate and encode now, so a refusal comes before the commit; send after it."""
        if on_success is not None or on_failure is not None:
            raise SQSDispatchError("SQS jobs take no callbacks", code="sqs_callbacks")
        if priority is not None and priority != job_type.__priority__:
            raise SQSDispatchError("SQS queues have no priority", code="sqs_priority")
        eff_queue = queue or job_type.__queue__
        url = self._queues.get(eff_queue)
        if url is None:
            raise SQSDispatchError(
                f"sqs.queues maps no URL for queue {eff_queue!r}", code="sqs_queue_unknown"
            )
        task_id = str(uuid.uuid4())
        task_name = wire_job_name(job_type.__qualname__)
        trace_id = get_trace_id()
        body = msgspec.json.encode(
            JobEnvelope(
                job=task_name,
                payload=payload or {},
                params=params,
                trace_id=trace_id,
                identity=encode_identity(current_identity()),
            )
        )
        if _INVALID_CHARACTER.search(body.decode()):
            raise SQSDispatchError(
                f"{job_type.__qualname__} message holds a character SQS refuses",
                code="sqs_invalid_character",
            )
        if len(body) > MAX_MESSAGE_BYTES:
            raise SQSDispatchError(
                f"{job_type.__qualname__} message is {len(body)} bytes; SQS jobs take "
                f"at most {MAX_MESSAGE_BYTES}",
                code="sqs_message_too_large",
            )
        delay = _delay(job_type, countdown, eta)
        request = {"QueueUrl": url, "MessageBody": body.decode()}
        request |= _placement(job_type, url, payload or {}, task_id, delay)
        send = _PendingSQSSend(self._client, task_name, task_id, eff_queue, request)
        size = self._batch_size
        if size is None:
            add_pending_publish(send)
        else:
            batch = functools.partial(
                _PendingSQSBatch, self._client, size, self._max_concurrent_sends
            )
            pending_publish_group(self, batch).sends.append(send)
        if self._metrics is not None:
            self._metrics.on_event(
                RuntimeEvent(
                    kind=EventKind.JOB_DISPATCHED,
                    use_case_name=job_type.__qualname__,
                    trace_id=trace_id,
                )
            )
        return JobHandle(job_id=task_id, queue=eff_queue, dispatched_at=datetime.now(UTC))

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        if on_all_success is not None or on_any_failure is not None:
            raise SQSDispatchError("SQS jobs take no callbacks", code="sqs_callbacks")
        return JobGroup(
            handles=tuple(self.dispatch(job_type, payload=body) for job_type, body in jobs)
        )


def _delay(job_type: type[Job[Any]], countdown: int | None, eta: datetime | None) -> int:
    if eta is not None:
        seconds = math.ceil((eta.astimezone(UTC) - datetime.now(UTC)).total_seconds())
    else:
        seconds = job_type.__countdown__ if countdown is None else countdown
    if seconds > MAX_DELAY_SECONDS:
        raise SQSDispatchError(
            f"SQS delays a message at most {MAX_DELAY_SECONDS} s, not {seconds}",
            code="sqs_delay_too_long",
        )
    return max(seconds, 0)


def _placement(
    job_type: type[Job[Any]], url: str, payload: Mapping[str, Any], task_id: str, delay: int
) -> dict[str, Any]:
    """``DelaySeconds`` on a standard queue; hashed group and deduplication ids on a FIFO one."""
    if not url.endswith(".fifo"):
        return {"DelaySeconds": delay} if delay else {}
    if delay:
        raise SQSDispatchError("a FIFO queue takes no per-message delay", code="sqs_fifo_delay")
    group_by = job_type.__group_by__
    group = None if group_by is None else payload.get(group_by)
    if group is None:
        raise SQSDispatchError(
            f"{job_type.__qualname__} payload has no {group_by!r} value for its "
            "FIFO MessageGroupId",
            code="sqs_group_missing",
        )
    # A hash meets SQS's 128-character ASCII limit and keeps the value out of SQS metadata.
    group_id = hashlib.sha256(str(group).encode()).hexdigest()
    return {"MessageGroupId": group_id, "MessageDeduplicationId": task_id}


__all__ = [
    "SQSConfig",
    "SQSDispatchError",
    "SQSJobService",
    "SQSQueueConfigError",
    "check_queues",
]
