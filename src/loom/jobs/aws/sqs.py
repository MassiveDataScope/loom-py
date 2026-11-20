"""``app.job_service: sqs``: each job is one SQS message whose body is a ``JobEnvelope``."""

from __future__ import annotations

import asyncio
import functools
import threading
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
from loom.core.job.config import JobSettings
from loom.core.job.context import add_pending_publish, pending_publish_group
from loom.core.job.dispatch import JobDispatch
from loom.core.job.handle import JobHandle
from loom.core.job.job import Job
from loom.core.logger import get_logger
from loom.core.model.types import PositiveInt
from loom.jobs.aws.errors import SQSDispatchError, SQSErrorCode, SQSQueueConfigError
from loom.jobs.aws.queue import (
    MAX_BATCH_ENTRIES,
    BatchResult,
    OutgoingMessage,
    QueueConfig,
    SQSQueue,
    SQSQueues,
)

if TYPE_CHECKING:
    from loom.composition import JobTransportContext
    from loom.core.config import ConfigContext
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.callback import JobCallback

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
        QueueConfig.check_written_shape(written.get("queues", {}), "sqs.queues")
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


def build_transport(context: JobTransportContext) -> SQSJobTransport:
    """``app.job_service: sqs``; ``MissingExtraError`` without the ``loom[aws]`` extra."""
    config = SQSConfig.load(context.config)
    check_queues(config.queues, context.jobs, context.settings)
    client = context.aws.client("sqs", config)
    return SQSJobTransport(client, config, metrics=context.metrics)


@dataclass(frozen=True)
class _PendingSQSSend:
    """One ``SendMessage`` on the publish lane; ``task_name``/``task_id`` name it in logs."""

    queue: SQSQueue
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
            for chunk in SQSQueue.chunks(
                sends, self.config.batch_size, weight=lambda send: send.size
            ):
                if queue.is_fifo and failed:
                    failed += [(send, _NOT_SENT) for send in chunk]
                else:
                    failed += await self._send_chunk(queue, chunk, retry=not queue.is_fifo)
        return failed

    async def _send_chunk(
        self, queue: SQSQueue, chunk: list[_PendingSQSSend], *, retry: bool
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

    def _call(self, queue: SQSQueue, chunk: list[_PendingSQSSend]) -> BatchResult:
        """In the thread: hand ``chunk`` to SQS, unless the batch stopped before it started."""
        if self.stopped.is_set():
            raise asyncio.CancelledError
        self.handed.update(send.task_id for send in chunk)
        return queue.send_batch([send.message for send in chunk])


class SQSJobTransport:
    """Send validated jobs to SQS, batching the post-commit channel when configured."""

    def __init__(
        self, client: Any, config: SQSConfig, *, metrics: MetricsAdapter | None = None
    ) -> None:
        self._config = config
        self._queues = SQSQueues(client, config.queues)
        self._new_batch = functools.partial(_PendingSQSBatch, config)
        self._metrics = NoMetrics() if metrics is None else metrics

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Validate SQS placement now and publish after a successful commit."""
        send, handle = self._build_send(dispatch)
        if self._config.batch:
            pending_publish_group(self, self._new_batch).sends.append(send)
        else:
            add_pending_publish(send)
        return handle

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Validate placement and publish the SQS message immediately."""
        send, handle = self._build_send(dispatch)
        await send()
        return handle

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Refuse callbacks before callback discovery or other dispatch checks."""
        if on_success is not None or on_failure is not None:
            raise SQSDispatchError("SQS jobs take no callbacks", code=SQSErrorCode.CALLBACKS)

    def check_parallel_callbacks(
        self, on_all_success: type[JobCallback] | None, on_any_failure: type[JobCallback] | None
    ) -> None:
        """Refuse callbacks unsupported by SQS.

        Raises:
            SQSDispatchError: Either callback is supplied.
        """
        self.check_callbacks(on_all_success, on_any_failure)

    def _build_send(self, dispatch: JobDispatch) -> tuple[_PendingSQSSend, JobHandle[Any]]:
        self.check_callbacks(dispatch.on_success, dispatch.on_failure)
        if dispatch.priority != dispatch.options.priority:
            raise SQSDispatchError("SQS queues have no priority", code=SQSErrorCode.PRIORITY)
        target = self._queues.named(dispatch.queue)
        envelope = dispatch.envelope
        body = msgspec.json.encode(envelope)
        if any(refused in body for refused in _REFUSED_CHARACTERS):
            raise SQSDispatchError(
                f"{dispatch.job_type.__qualname__} message holds a character SQS refuses",
                code=SQSErrorCode.INVALID_CHARACTER,
            )
        if len(body) > MAX_MESSAGE_BYTES:
            raise SQSDispatchError(
                f"{dispatch.job_type.__qualname__} message is {len(body)} bytes; SQS jobs take "
                f"at most {MAX_MESSAGE_BYTES}",
                code=SQSErrorCode.MESSAGE_TOO_LARGE,
            )
        task_id = str(envelope.task_id)
        message = target.build_message(
            dispatch.job_type, envelope.payload or {}, task_id, dispatch.countdown, body.decode()
        )
        send = _PendingSQSSend(target, envelope.job, task_id, message)
        self._metrics.on_event(
            RuntimeEvent(
                kind=EventKind.JOB_DISPATCHED,
                use_case_name=dispatch.job_type.__qualname__,
                trace_id=envelope.trace_id,
            )
        )
        return send, JobHandle(
            job_id=task_id, queue=dispatch.queue, dispatched_at=datetime.now(UTC)
        )


__all__ = ["SQSConfig", "SQSDispatchError", "SQSJobTransport", "build_transport", "check_queues"]
