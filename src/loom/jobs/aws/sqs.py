"""``app.job_service: sqs``: each job is one SQS message whose body is a ``JobEnvelope``."""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

import msgspec

from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClientConfig, aws_error_code
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.errors import LoomError
from loom.core.identity import current_identity
from loom.core.identity.wire import encode_identity
from loom.core.job.context import add_pending_publish
from loom.core.job.envelope import JobEnvelope, wire_job_name
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.logger import get_logger
from loom.core.tracing import get_trace_id

if TYPE_CHECKING:
    from loom.core.engine.executor import RuntimeExecutor
    from loom.core.engine.metrics import MetricsAdapter
    from loom.core.job.callback import JobCallback
    from loom.core.use_case.factory import UseCaseFactory

ResultT = TypeVar("ResultT")

MAX_DELAY_SECONDS = 900
MAX_MESSAGE_BYTES = 256 * 1024
_INVALID_CHARACTER = re.compile(r"[^\t\n\r\x20-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]")
"""A character SQS refuses in a message body."""

_log = get_logger(__name__)


class SQSConfig(AwsClientConfig, frozen=True, kw_only=True):
    """The ``sqs`` section: ``aws:`` client keys plus each logical queue's URL (``.fifo``: FIFO)."""

    queues: dict[str, str] = {}


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
            _log.error(
                "SQS send failed",
                job=self.task_name,
                task_id=self.task_id,
                queue=self.queue,
                error_code=code,
            )
            raise SQSDispatchError(
                f"sending {self.task_name} to queue {self.queue!r} failed: {code}",
                code="sqs_send_failed",
            ) from None


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
    ) -> None:
        self._client = client
        self._queues = dict(queues)
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
        add_pending_publish(_PendingSQSSend(self._client, task_name, task_id, eff_queue, request))
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
