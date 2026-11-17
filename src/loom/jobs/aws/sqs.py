"""``app.job_service: sqs``: each job is one SQS message whose body is a ``JobEnvelope``."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import threading
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Annotated, Any, TypeVar

import msgspec

from loom.core.config import ConfigKey
from loom.core.connections.aws import AwsClientConfig, aws_error_code
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import NoMetrics
from loom.core.engine.post_commit import log_never_ran
from loom.core.identity import captured_delegation
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish, pending_publish_group
from loom.core.job.envelope import encode_job_message, wire_job_name
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.job.policy import Countdown
from loom.core.job.service import InlineJobService
from loom.core.logger import get_logger
from loom.core.model.types import PositiveInt
from loom.core.tracing import get_trace_id
from loom.jobs.aws.errors import SQSDispatchError, SQSErrorCode, SQSQueueConfigError
from loom.jobs.aws.queue import (
    MAX_BATCH_ENTRIES,
    BatchResult,
    OutgoingMessage,
    QueueConfig,
    SqsQueue,
    SqsQueues,
)

if TYPE_CHECKING:
    from loom.composition import JobBackendContext
    from loom.core.config import ConfigContext
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.callback import JobCallback
    from loom.core.use_case.factory import UseCaseFactory

ResultT = TypeVar("ResultT")

MAX_MESSAGE_BYTES = 256 * 1024
_REFUSED_CHARACTERS = (b"\xef\xbf\xbe", b"\xef\xbf\xbf")

_log = get_logger(__name__)


class SQSConfig(AwsClientConfig, frozen=True, kw_only=True):
    """The ``sqs`` section: ``aws:`` client keys plus each logical queue.

    A queue is its URL (``.fifo``: FIFO) and its dead-letter policy.
    ``batch`` sends an execution's jobs in ``SendMessageBatch`` calls of at most
    ``batch_size`` entries, ``max_concurrent_sends`` queues at a time;
    ``batch: false`` sends each with its own ``SendMessage``.
    """

    queues: dict[str, QueueConfig] = {}
    batch: bool = True
    batch_size: Annotated[int, msgspec.Meta(ge=1, le=MAX_BATCH_ENTRIES)] = MAX_BATCH_ENTRIES
    max_concurrent_sends: PositiveInt = 1

    @classmethod
    def load(cls, ctx: ConfigContext) -> SQSConfig:
        """The ``sqs`` section of *ctx*, inheriting the ``aws`` one.

        Raises:
            ConfigError: When the section is invalid, or a queue is written as a bare URL.
        """
        written = ctx.section(ConfigKey.SQS, dict) if ctx.has(ConfigKey.SQS) else {}
        QueueConfig.refuse_misshapen(written.get("queues", {}), "sqs.queues")
        return cls.from_config(ctx, ConfigKey.SQS, own=written)


def check_queues(
    queues: Mapping[str, QueueConfig], jobs: Sequence[type[Job[Any]]], settings: JobSettings
) -> None:
    """Fail at startup unless every job's queue has a URL and a FIFO job names its group."""
    for job_type in jobs:
        queue = settings.of(job_type).queue
        config = queues.get(queue)
        if config is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses queue {queue!r}, which "
                f"sqs.queues does not map to a URL"
            )
        if config.is_fifo and job_type.__group_by__ is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses FIFO queue {queue!r} but "
                "declares no __group_by__ payload field"
            )


def build_backend(context: JobBackendContext) -> SQSJobService:
    """``app.job_service: sqs``; ``MissingExtraError`` without the ``loom[aws]`` extra."""
    config = SQSConfig.load(context.config)
    check_queues(config.queues, context.jobs, context.settings)
    client = context.aws.client("sqs", config)
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

    queue: SqsQueue
    task_name: str
    task_id: str
    message: OutgoingMessage = field(repr=False)

    @property
    def size(self) -> int:
        return self.message.size

    async def __call__(self) -> None:
        try:
            await asyncio.to_thread(self.queue.send, self.message)
        except Exception as exc:
            code = aws_error_code(exc)
            self.log_failure(code)
            raise SQSDispatchError(
                f"sending {self.task_name} to queue {self.queue.name!r} failed: {code}",
                code=SQSErrorCode.SEND_FAILED,
            ) from None

    def log_fields(self) -> tuple[dict[str, object]]:
        return ({"job_name": self.task_name, "task_id": self.task_id},)

    def log_failure(self, code: str) -> None:
        _log.error(
            "SQS send failed",
            job=self.task_name,
            task_id=self.task_id,
            queue=self.queue.name,
            error_code=code,
        )


_Failure = tuple[_PendingSQSSend, str]
_NOT_SENT = "NotSent"
"""The code of a FIFO entry held back because an earlier one of its queue failed."""


@dataclass(frozen=True)
class _PendingSQSBatch:
    """Every send of one post-commit drain, as ``SendMessageBatch`` calls per queue, in order."""

    config: SQSConfig
    sends: list[_PendingSQSSend] = field(default_factory=list, repr=False)
    handed: set[str] = field(default_factory=set, repr=False)
    """Task ids a running thread has handed to an SQS call."""
    stopped: threading.Event = field(default_factory=threading.Event, repr=False)

    async def __call__(self) -> None:
        by_queue: dict[str, list[_PendingSQSSend]] = {}
        for send in self.sends:
            by_queue.setdefault(send.queue.url, []).append(send)
        limit = asyncio.Semaphore(self.config.max_concurrent_sends)
        queues = [
            asyncio.ensure_future(self._send_queue(sends, limit)) for sends in by_queue.values()
        ]
        try:
            results = await asyncio.gather(*queues)
        except BaseException:
            await asyncio.wait(queues)
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
            code=SQSErrorCode.BATCH_PARTIAL_FAILURE,
        )

    def log_fields(self) -> list[dict[str, object]]:
        return [fields for send in self.sends for fields in send.log_fields()]

    async def _send_queue(
        self, sends: list[_PendingSQSSend], limit: asyncio.Semaphore
    ) -> list[_Failure]:
        """One chunk after the other, so a FIFO queue receives the jobs in dispatch order.

        A FIFO queue resends nothing and stops at its first failure: a later
        message would overtake the failed one.
        """
        queue = sends[0].queue
        failed: list[_Failure] = []
        async with limit:
            for chunk in SqsQueue.chunks(
                sends, self.config.batch_size, weight=lambda send: send.size
            ):
                if queue.is_fifo and failed:
                    failed += [(send, _NOT_SENT) for send in chunk]
                else:
                    failed += await self._send_chunk(queue, chunk, retry=not queue.is_fifo)
        return failed

    async def _send_chunk(
        self, queue: SqsQueue, chunk: list[_PendingSQSSend], *, retry: bool
    ) -> list[_Failure]:
        """Send ``chunk``; the entries SQS refuses are sent once more when ``retry``."""
        call = asyncio.ensure_future(asyncio.to_thread(self._call, queue, chunk))
        try:
            result = await asyncio.shield(call)
        except asyncio.CancelledError:
            self.stopped.set()
            await asyncio.wait([call])
            raise
        by_id = {send.task_id: send for send in chunk}
        if result.refused and retry:
            refused = [by_id[task_id] for task_id in result.refused]
            return await self._send_chunk(queue, refused, retry=False)
        return [(by_id[task_id], code) for task_id, code in result.failed.items()]

    def _call(self, queue: SqsQueue, chunk: list[_PendingSQSSend]) -> BatchResult:
        """In the thread: hand ``chunk`` to SQS, unless the batch stopped before it started."""
        if self.stopped.is_set():
            raise asyncio.CancelledError
        self.handed.update(send.task_id for send in chunk)
        return queue.send_batch([send.message for send in chunk])


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
        self._config = config
        self._settings = settings or JobSettings()
        self._queues = SqsQueues(client, config.queues)
        self._new_batch = functools.partial(_PendingSQSBatch, config)
        self._inline = InlineJobService(factory, executor, settings=self._settings)
        self._metrics = NoMetrics() if metrics is None else metrics

    async def run(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ResultT:
        return await self._inline.run(job_type, params=params, payload=payload)

    def dispatch_after_commit(
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
    ) -> JobHandle[ResultT]:
        """Validate and encode now, so a refusal comes before the commit; send after it."""
        send, handle = self._message(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        if self._config.batch:
            pending_publish_group(self, self._new_batch).sends.append(send)
        else:
            add_pending_publish(send)
        return handle

    async def dispatch_now(
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
    ) -> JobHandle[ResultT]:
        """Validate, encode and send the job now, even inside a use case.

        The job can run before the caller's writes commit and is not undone
        if they roll back. Work that depends on the caller's writes uses
        :meth:`dispatch_after_commit`.
        """
        send, handle = self._message(
            job_type,
            params=params,
            payload=payload,
            on_success=on_success,
            on_failure=on_failure,
            queue=queue,
            countdown=countdown,
            priority=priority,
        )
        await send()
        return handle

    def _message(
        self,
        job_type: type[Job[ResultT]],
        *,
        params: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        on_success: type[JobCallback] | None,
        on_failure: type[JobCallback] | None,
        queue: str | None,
        countdown: int | None,
        priority: int | None,
    ) -> tuple[_PendingSQSSend, JobHandle[ResultT]]:
        """Validate, encode and place the message.

        Args:
            job_type: Concrete Job subclass to dispatch.
            params: Primitive params for the execute signature.
            payload: Raw dict for ``Input()`` command construction.
            on_success: Must be ``None``; SQS jobs take no callbacks.
            on_failure: Must be ``None``; SQS jobs take no callbacks.
            queue: Override the job's queue for this dispatch only.
            countdown: Delay in seconds, 0 through 900 inclusive.
            priority: Must match the job's priority; SQS queues have none.

        Returns:
            The pending send and a handle sharing its task id.

        Raises:
            ValueError: When *countdown* is outside 0..900 inclusive.
            SQSDispatchError: When SQS cannot carry the message.
        """
        if on_success is not None or on_failure is not None:
            raise SQSDispatchError("SQS jobs take no callbacks", code=SQSErrorCode.CALLBACKS)
        options = self._settings.of(job_type)
        if priority is not None and priority != options.priority:
            raise SQSDispatchError("SQS queues have no priority", code=SQSErrorCode.PRIORITY)
        eff_queue = queue or options.queue
        target = self._queues.named(eff_queue)
        task_id = str(uuid.uuid4())
        trace_id = get_trace_id()
        payload = payload or {}
        body = encode_job_message(
            job_type,
            payload=payload,
            params=params,
            identity=captured_delegation(),
            trace_id=trace_id,
            task_id=task_id,
        )
        if any(refused in body for refused in _REFUSED_CHARACTERS):
            raise SQSDispatchError(
                f"{job_type.__qualname__} message holds a character SQS refuses",
                code=SQSErrorCode.INVALID_CHARACTER,
            )
        if len(body) > MAX_MESSAGE_BYTES:
            raise SQSDispatchError(
                f"{job_type.__qualname__} message is {len(body)} bytes; SQS jobs take "
                f"at most {MAX_MESSAGE_BYTES}",
                code=SQSErrorCode.MESSAGE_TOO_LARGE,
            )
        delay = Countdown(options.countdown if countdown is None else countdown)
        message = OutgoingMessage(
            id=task_id,
            body=body.decode(),
            **_placement(job_type, target, payload, task_id, delay),
        )
        task_name = wire_job_name(job_type.__qualname__)
        send = _PendingSQSSend(target, task_name, task_id, message)
        self._metrics.on_event(
            RuntimeEvent(
                kind=EventKind.JOB_DISPATCHED,
                use_case_name=job_type.__qualname__,
                trace_id=trace_id,
            )
        )
        handle: JobHandle[ResultT] = JobHandle(
            job_id=task_id, queue=eff_queue, dispatched_at=datetime.now(UTC)
        )
        return send, handle

    def dispatch_parallel(
        self,
        jobs: Sequence[tuple[type[Job[Any]], dict[str, Any]]],
        *,
        on_all_success: type[JobCallback] | None = None,
        on_any_failure: type[JobCallback] | None = None,
    ) -> JobGroup:
        if on_all_success is not None or on_any_failure is not None:
            raise SQSDispatchError("SQS jobs take no callbacks", code=SQSErrorCode.CALLBACKS)
        return JobGroup(
            handles=tuple(
                self.dispatch_after_commit(job_type, payload=body) for job_type, body in jobs
            )
        )


def _placement(
    job_type: type[Job[Any]],
    queue: SqsQueue,
    payload: Mapping[str, Any],
    task_id: str,
    delay: Countdown,
) -> dict[str, Any]:
    """The delay on a standard queue; hashed group and deduplication ids on a FIFO one."""
    if not queue.is_fifo:
        return {"delay": delay}
    if delay.seconds:
        raise SQSDispatchError(
            "a FIFO queue takes no per-message delay", code=SQSErrorCode.FIFO_DELAY
        )
    group_by = job_type.__group_by__
    group = None if group_by is None else payload.get(group_by)
    if group is None:
        raise SQSDispatchError(
            f"{job_type.__qualname__} payload has no {group_by!r} value for its "
            "FIFO MessageGroupId",
            code=SQSErrorCode.GROUP_MISSING,
        )
    group_id = hashlib.sha256(str(group).encode()).hexdigest()
    return {"group_id": group_id, "dedup_id": task_id}


__all__ = [
    "SQSConfig",
    "SQSDispatchError",
    "SQSJobService",
    "build_backend",
    "check_queues",
]
