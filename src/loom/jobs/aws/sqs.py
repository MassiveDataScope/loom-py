"""``app.job_service: sqs``: each job is one SQS message whose body is a ``JobEnvelope``."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import math
import threading
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, TypeVar

import msgspec

from loom.core.config import ConfigError, ConfigKey
from loom.core.config.aws import decode_aws, written_aws
from loom.core.connections.aws import AwsClientConfig, aws_error_code, aws_island
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.post_commit import log_never_ran
from loom.core.errors import LoomError
from loom.core.identity import current_identity
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish, pending_publish_group
from loom.core.job.envelope import encode_job_message, wire_job_name
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService
from loom.core.logger import get_logger
from loom.core.model.types import PositiveInt
from loom.core.tracing import get_trace_id
from loom.jobs.aws import FIFO_SUFFIX

if TYPE_CHECKING:
    from loom.composition import JobBackendContext
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
# U+FFFE, U+FFFF: what SQS refuses that JSON leaves (control characters escaped, no surrogate).
_REFUSED_CHARACTERS = (b"\xef\xbf\xbe", b"\xef\xbf\xbf")

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


def check_queues(
    queues: Mapping[str, str], jobs: Sequence[type[Job[Any]]], settings: JobSettings
) -> None:
    """Fail at startup unless every job's queue has a URL and a FIFO job names its group."""
    for job_type in jobs:
        queue = settings.of(job_type).queue
        url = queues.get(queue)
        if url is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses queue {queue!r}, which "
                f"sqs.queues does not map to a URL"
            )
        if url.endswith(FIFO_SUFFIX) and job_type.__group_by__ is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses FIFO queue {queue!r} but "
                "declares no __group_by__ payload field"
            )


def build_backend(context: JobBackendContext) -> SQSJobService:
    """``app.job_service: sqs``; ``MissingExtraError`` without the ``loom[sqs]`` extra."""
    ctx = context.config
    written = ctx.section(ConfigKey.SQS, dict) if ctx.has(ConfigKey.SQS) else {}
    config = decode_aws(written_aws(ctx), written, ConfigKey.SQS, SQSConfig)
    check_queues(config.queues, context.jobs, context.settings)
    client = aws_island(extra="sqs").build_aws_client("sqs", config)
    return SQSJobService(
        client,
        config,
        factory=context.factory,
        executor=context.executor,
        metrics=context.metrics,
        settings=context.settings,
    )


@dataclass(frozen=True)
class _PendingSQSSend:
    """One ``SendMessage`` on the publish lane; ``task_name``/``task_id`` name it in logs."""

    client: Any
    task_name: str
    task_id: str
    queue: str
    size: int  # of the body, in bytes
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

    def log_fields(self) -> tuple[dict[str, object]]:
        return ({"job_name": self.task_name, "task_id": self.task_id},)

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
    config: SQSConfig
    sends: list[_PendingSQSSend] = field(default_factory=list, repr=False)
    handed: set[str] = field(default_factory=set, repr=False)
    """Task ids a running thread has handed to an SQS call."""
    stopped: threading.Event = field(default_factory=threading.Event, repr=False)

    async def __call__(self) -> None:
        by_queue: dict[str, list[_PendingSQSSend]] = {}
        for send in self.sends:
            by_queue.setdefault(send.request["QueueUrl"], []).append(send)
        limit = asyncio.Semaphore(self.config.max_concurrent_sends)
        queues = [
            asyncio.ensure_future(self._send_queue(url, sends, limit))
            for url, sends in by_queue.items()
        ]
        try:
            results = await asyncio.gather(*queues)
        except BaseException:
            await asyncio.wait(queues)  # gather cancelled them; each waits for its running call
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

    def log_fields(self) -> list[dict[str, object]]:
        return [fields for send in self.sends for fields in send.log_fields()]

    async def _send_queue(
        self, url: str, sends: list[_PendingSQSSend], limit: asyncio.Semaphore
    ) -> list[_Failure]:
        """One chunk after the other, so a FIFO queue receives the jobs in dispatch order.

        A FIFO queue resends nothing and stops at its first failure: a later
        message would overtake the failed one.
        """
        fifo = url.endswith(FIFO_SUFFIX)
        failed: list[_Failure] = []
        async with limit:
            for chunk in _chunks(sends, self.config.batch_size):
                if fifo and failed:
                    failed += [(send, _NOT_SENT) for send in chunk]
                else:
                    failed += await self._send_chunk(url, chunk, retry=not fifo)
        return failed

    async def _send_chunk(
        self, url: str, chunk: list[_PendingSQSSend], *, retry: bool
    ) -> list[_Failure]:
        """Send ``chunk``; the entries SQS refuses are sent once more when ``retry``."""
        call = asyncio.ensure_future(asyncio.to_thread(self._call, url, chunk))
        try:
            response = await asyncio.shield(call)
        except asyncio.CancelledError:
            # A call still waiting for a thread sends nothing; one running keeps
            # its max_concurrent_sends slot until it returns.
            self.stopped.set()
            await asyncio.wait([call])
            raise
        except Exception as exc:
            # botocore already retried the call, and its message may echo a body.
            code = aws_error_code(exc)
            return [(send, code) for send in chunk]
        by_id = {send.task_id: send for send in chunk}
        failed = [(by_id[entry["Id"]], entry["Code"]) for entry in response.get("Failed", [])]
        if failed and retry:
            return await self._send_chunk(url, [send for send, _ in failed], retry=False)
        return failed

    def _call(self, url: str, chunk: list[_PendingSQSSend]) -> dict[str, Any]:
        """In the thread: hand ``chunk`` to SQS, unless the batch stopped before it started."""
        if self.stopped.is_set():
            raise asyncio.CancelledError
        self.handed.update(send.task_id for send in chunk)
        entries = [send.batch_entry() for send in chunk]
        response: dict[str, Any] = self.client.send_message_batch(QueueUrl=url, Entries=entries)
        return response


def _chunks(sends: list[_PendingSQSSend], size: int) -> Iterator[list[_PendingSQSSend]]:
    """``sends`` in order, at most ``size`` entries and ``MAX_BATCH_BYTES`` of bodies a chunk."""
    chunk: list[_PendingSQSSend] = []
    total = 0
    for send in sends:
        if chunk and (len(chunk) == size or total + send.size > MAX_BATCH_BYTES):
            yield chunk
            chunk, total = [], 0
        chunk.append(send)
        total += send.size
    if chunk:
        yield chunk


class SQSJobService:
    """``JobService`` sending each job to SQS after the commit; :meth:`run` is in-process.

    *settings* give each job's queue, countdown and priority; the ClassVars
    when omitted.
    """

    def __init__(
        self,
        client: Any,
        config: SQSConfig,
        *,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        metrics: MetricsAdapter | None = None,
        settings: JobSettings | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._settings = settings or JobSettings()
        self._new_batch = functools.partial(_PendingSQSBatch, client, config)
        self._inline = InlineJobService(factory, executor, settings=self._settings)
        self._metrics = metrics

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        return await self._inline.run(job_type, params=params, payload=payload)

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
        options = self._settings.of(job_type)
        if priority is not None and priority != options.priority:
            raise SQSDispatchError("SQS queues have no priority", code="sqs_priority")
        eff_queue = queue or options.queue
        url = self._config.queues.get(eff_queue)
        if url is None:
            raise SQSDispatchError(
                f"sqs.queues maps no URL for queue {eff_queue!r}", code="sqs_queue_unknown"
            )
        task_id = str(uuid.uuid4())
        trace_id = get_trace_id()
        payload = payload or {}
        body = encode_job_message(
            job_type,
            payload=payload,
            params=params,
            identity=current_identity(),
            trace_id=trace_id,
        )
        if any(refused in body for refused in _REFUSED_CHARACTERS):
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
        delay = _delay(options.countdown if countdown is None else countdown, eta)
        request = {"QueueUrl": url, "MessageBody": body.decode()}
        request |= _placement(job_type, url, payload, task_id, delay)
        task_name = wire_job_name(job_type.__qualname__)
        send = _PendingSQSSend(self._client, task_name, task_id, eff_queue, len(body), request)
        if self._config.batch:
            pending_publish_group(self, self._new_batch).sends.append(send)
        else:
            add_pending_publish(send)
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


def _delay(countdown: int, eta: datetime | None) -> int:
    if eta is not None:
        seconds = math.ceil((eta.astimezone(UTC) - datetime.now(UTC)).total_seconds())
    else:
        seconds = countdown
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
    if not url.endswith(FIFO_SUFFIX):
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
    "build_backend",
    "check_queues",
]
