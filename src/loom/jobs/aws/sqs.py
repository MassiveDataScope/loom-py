"""SQSJobService: dispatches each job as one Amazon SQS message.

Selected by ``app.job_service: sqs``. The body is the
:class:`~loom.core.job.envelope.JobEnvelope` the Lambda SQS adapter decodes.
:meth:`SQSJobService.dispatch` validates and encodes; the send runs, shielded,
on the post-commit publish lane, after the unit of work commits.
"""

from __future__ import annotations

import asyncio
import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

import msgspec

from loom.core.config import ConfigError
from loom.core.connections.aws import AwsClientConfig
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
"""Longest per-message delay SQS accepts (15 minutes)."""
MAX_MESSAGE_BYTES = 256 * 1024
"""Largest body this service sends."""

_log = get_logger(__name__)


class SQSConfig(AwsClientConfig, frozen=True, kw_only=True):
    """The ``sqs`` section: the AWS client, inheriting ``aws:``, and the URL of each queue.

    Args:
        queues: Queue URL by logical queue name; a URL ending in ``.fifo``
            is a FIFO queue.
    """

    queues: dict[str, str] = {}


class SQSQueueConfigError(ConfigError):
    """A job's queue has no URL in ``sqs.queues``, or its FIFO queue no ``__group_by__``."""


class SQSDispatchError(LoomError):
    """A dispatch SQS cannot carry; ``code`` names which. Raised before anything is sent."""

    retryable = False


def check_queues(queues: Mapping[str, str], jobs: Sequence[type[Job[Any]]]) -> None:
    """Fail at startup unless every job's queue has a URL and a FIFO job names its group.

    Raises:
        SQSQueueConfigError: Naming the first job that does not.
    """
    for job_type in jobs:
        url = queues.get(job_type.__queue__)
        if url is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses queue {job_type.__queue__!r}, which "
                f"sqs.queues does not map to a URL"
            )
        if _is_fifo(url) and job_type.__group_by__ is None:
            raise SQSQueueConfigError(
                f"{job_type.__qualname__} uses FIFO queue {job_type.__queue__!r} but "
                "declares no __group_by__ payload field"
            )


@dataclass(frozen=True)
class _PendingSQSSend:
    """One ``SendMessage``, run on the post-commit publish lane.

    ``task_name`` and ``task_id`` are what the post-commit logs name; the
    request holds the body and is left out of the repr.
    """

    client: Any
    task_name: str
    task_id: str
    queue: str
    request: dict[str, Any] = field(repr=False)

    async def __call__(self) -> None:
        try:
            await asyncio.to_thread(self.client.send_message, **self.request)
        except Exception as exc:
            _log.error(
                "SQS send failed",
                job=self.task_name,
                task_id=self.task_id,
                queue=self.queue,
                error_class=type(exc).__name__,
            )
            raise


class SQSJobService:
    """``JobService`` that sends each dispatched job to its SQS queue after the commit.

    :meth:`run` executes in-process, as
    :class:`~loom.core.job.service.InlineJobService` does.

    Args:
        client: boto3 ``sqs`` client.
        queues: Queue URL by logical queue name.
        factory: Builds the job :meth:`run` executes.
        executor: Executes it.
        metrics: Receives ``JOB_DISPATCHED`` for each dispatch.
    """

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
        """Execute the job in-process and return its result."""
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
        """Validate and encode the message now; send it once the unit of work commits.

        Raises:
            SQSDispatchError: For a callback, a priority other than the
                job's, a queue without URL, a delay over 900 s or any delay
                on a FIFO queue, a FIFO job whose payload lacks its group
                field, or a body over 256 KiB.
        """
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
        """Dispatch each job independently.

        Raises:
            SQSDispatchError: For a callback, or as :meth:`dispatch`.
        """
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
    """``DelaySeconds`` on a standard queue; group and deduplication ids on a FIFO one."""
    if not _is_fifo(url):
        return {"DelaySeconds": delay} if delay else {}
    if delay:
        raise SQSDispatchError("a FIFO queue takes no per-message delay", code="sqs_fifo_delay")
    group_by = job_type.__group_by__
    if group_by is None or group_by not in payload:
        raise SQSDispatchError(
            f"{job_type.__qualname__} payload has no {group_by!r} field for its "
            "FIFO MessageGroupId",
            code="sqs_group_missing",
        )
    return {"MessageGroupId": str(payload[group_by]), "MessageDeduplicationId": task_id}


def _is_fifo(url: str) -> bool:
    return url.endswith(".fifo")


__all__ = [
    "SQSConfig",
    "SQSDispatchError",
    "SQSJobService",
    "SQSQueueConfigError",
    "check_queues",
]
