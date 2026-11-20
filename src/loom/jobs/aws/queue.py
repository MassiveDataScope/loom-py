"""The SQS queues of an application: sending, and settling the messages a consumer received."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, TypeVar

import msgspec

from loom.core.config import ConfigError
from loom.core.connections.aws import aws_error_code
from loom.core.job.config import ResolvedRetry, RetrySource
from loom.core.job.job import Job
from loom.core.job.policy import DEFAULT_RETRY, Countdown, RetryPolicy
from loom.jobs.aws import FIFO_SUFFIX
from loom.jobs.aws.errors import SQSDispatchError, SQSErrorCode, SQSQueueConfigError

_T = TypeVar("_T")

MAX_BATCH_ENTRIES = 10
MAX_BATCH_BYTES = 1024 * 1024
"""What one batch call carries at most, in entries and in body bytes."""
POSTPONEMENTS_ATTRIBUTE = "loom.postponements"
"""The message attribute counting how many times a job asked to run again."""
ABANDONMENT_REASON_ATTRIBUTE = "loom.abandonment_reason"
"""The message attribute holding why a message was sent to the dead-letter queue."""
REASON_MAX_CHARS = 256
"""The longest abandonment reason kept; a longer one is cut."""

_URL_REGION = re.compile(r"https?://sqs\.([a-z0-9-]+)\.")


class DeadLetterPolicy(StrEnum):
    """What becomes of a message whose job is abandoned for good."""

    REDRIVE = "redrive"
    DISCARD = "discard"


class QueueConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One entry of ``sqs.queues``.

    Attributes:
        url: The queue URL; ``.fifo`` at its end marks a FIFO queue.
        dead_letter: ``redrive`` sends an abandoned message to the queue's
            dead-letter queue and leaves the attempt limit to its
            ``RedrivePolicy``; ``discard`` drops it and ``max_attempts`` decides.
    """

    url: str
    dead_letter: DeadLetterPolicy = DeadLetterPolicy.REDRIVE

    @property
    def is_fifo(self) -> bool:
        """Whether the URL names a FIFO queue."""
        return self.url.endswith(FIFO_SUFFIX)

    @property
    def region(self) -> str | None:
        """The region in the URL's host; ``None`` for an endpoint that names none."""
        found = _URL_REGION.match(self.url)
        return None if found is None else found.group(1)

    @property
    def account_id(self) -> str:
        """The AWS account owning the queue, the first segment of its URL path."""
        return self.url.rstrip("/").split("/")[-2]

    @property
    def queue_name(self) -> str:
        """The queue's name in AWS, the last segment of its URL path."""
        return self.url.rstrip("/").split("/")[-1]

    @classmethod
    def check_written_shape(cls, written: Any, path: str) -> None:
        """Refuse ``sqs.queues`` that is no mapping, or an entry written as a bare URL.

        Args:
            written: ``sqs.queues`` as written.
            path: Its dotted path, for the message.

        Raises:
            ConfigError: Naming the mapping the section and each entry are written as.
        """
        if not isinstance(written, Mapping):
            raise ConfigError(
                f"{path} must be a mapping of queue name to {{url, dead_letter: redrive|discard}}, "
                f"not {type(written).__name__}"
            )
        for name, entry in written.items():
            if isinstance(entry, str):
                raise ConfigError(
                    f"{path}.{name} is a URL; write it as a mapping: "
                    f"{{url: {entry}, dead_letter: redrive|discard}}"
                )


@dataclass(frozen=True, slots=True)
class BatchResult:
    """What one batch operation came to, entry by entry; a failed entry never raises.

    Attributes:
        succeeded: The ids of the entries SQS accepted.
        failed: The AWS error code of each entry that did not go through, by id.
        unanswered: The ids among *failed* whose whole call raised, so SQS
            answered nothing about them.
    """

    succeeded: tuple[str, ...] = ()
    failed: Mapping[str, str] = field(default_factory=dict)
    unanswered: frozenset[str] = frozenset()

    @property
    def refused(self) -> list[str]:
        """The ids SQS answered a failure for, in a call that itself went through."""
        return [message_id for message_id in self.failed if message_id not in self.unanswered]

    def merge(self, other: BatchResult) -> BatchResult:
        """This result followed by *other*."""
        return BatchResult(
            (*self.succeeded, *other.succeeded),
            {**self.failed, **other.failed},
            self.unanswered | other.unanswered,
        )


@dataclass(frozen=True, slots=True)
class OutgoingMessage:
    """One message to send; ``id`` names its entry in a batch.

    Attributes:
        id: The entry id, unique within a batch.
        body: The message body.
        delay: Seconds before the message becomes visible; none on a FIFO queue.
        group_id: ``MessageGroupId`` of a FIFO queue.
        dedup_id: ``MessageDeduplicationId`` of a FIFO queue.
        attributes: The message attributes.
    """

    id: str
    body: str
    delay: Countdown = Countdown(0)
    group_id: str | None = None
    dedup_id: str | None = None
    attributes: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @property
    def size(self) -> int:
        """The bytes of the body."""
        return len(self.body.encode())

    def request(self) -> dict[str, Any]:
        """The ``SendMessage`` parameters but the queue URL, as the SQS API takes them."""
        request: dict[str, Any] = {"MessageBody": self.body}
        if self.delay.seconds:
            request["DelaySeconds"] = self.delay.seconds
        if self.group_id is not None:
            request["MessageGroupId"] = self.group_id
        if self.dedup_id is not None:
            request["MessageDeduplicationId"] = self.dedup_id
        if self.attributes:
            request["MessageAttributes"] = dict(self.attributes)
        return request


@dataclass(frozen=True, slots=True)
class SQSRecord:
    """A message a Lambda consumer received, as settlement needs it.

    Attributes:
        message_id: SQS's id of the message; the entry id of its settlement.
        receipt_handle: The handle of this delivery.
        body: The message body.
        group_id: ``MessageGroupId`` when the queue is FIFO.
        attributes: The message attributes, in the shape the SQS API takes.
        queue_arn: The ARN of the queue it was received from.
        receive_count: ``ApproximateReceiveCount``: how many times SQS delivered it.
        postponements: How many times the job asked to run again; zero when it never did.
        unreadable_reason: Why loom cannot read its counts; ``None`` when it can.
    """

    message_id: str
    receipt_handle: str
    body: str
    group_id: str | None = None
    attributes: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    queue_arn: str = ""
    receive_count: int = 1
    postponements: int = 0
    unreadable_reason: str | None = None

    @classmethod
    def from_event(cls, record: Any) -> SQSRecord:
        """The record of an SQS event's ``Records`` entry; what it lacks is empty."""
        if not isinstance(record, Mapping):
            return cls("", "", "")
        fields: dict[str, Any] = {
            "message_id": str(record.get("messageId", "")),
            "receipt_handle": str(record.get("receiptHandle", "")),
            "body": str(record.get("body", "")),
            "queue_arn": str(record.get("eventSourceARN", "")),
        }
        try:
            system = record.get("attributes") or {}
            fields["group_id"] = system.get("MessageGroupId")
            fields["attributes"] = {
                name: cls._attribute(value)
                for name, value in (record.get("messageAttributes") or {}).items()
            }
        except (AttributeError, KeyError, TypeError, ValueError):
            return cls(
                **fields,
                unreadable_reason="the message attributes are not readable (each needs a dataType)",
            )
        for name, written, minimum, reason in (
            (
                "receive_count",
                system.get("ApproximateReceiveCount", 1),
                1,
                "ApproximateReceiveCount is not a positive integer",
            ),
            (
                "postponements",
                fields["attributes"].get(POSTPONEMENTS_ATTRIBUTE, {}).get("StringValue", 0),
                0,
                f"{POSTPONEMENTS_ATTRIBUTE} is not a non-negative integer",
            ),
        ):
            try:
                fields[name] = int(written)
            except (TypeError, ValueError):
                return cls(**fields, unreadable_reason=reason)
            if fields[name] < minimum:
                return cls(**fields, unreadable_reason=reason)
        return cls(**fields)

    @property
    def is_settleable(self) -> bool:
        """Whether it carries the id and handle a settlement needs, and a body to run."""
        return bool(self.message_id and self.receipt_handle and self.body)

    @property
    def is_fifo(self) -> bool:
        """Whether it was received from a FIFO queue."""
        return self.queue_arn.endswith(FIFO_SUFFIX)

    @property
    def is_readable(self) -> bool:
        """Whether loom can read its attributes and counts."""
        return self.unreadable_reason is None

    @staticmethod
    def _attribute(written: Mapping[str, Any]) -> dict[str, Any]:
        attribute: dict[str, Any] = {"DataType": written["dataType"]}
        if written.get("stringValue") is not None:
            attribute["StringValue"] = written["stringValue"]
        if written.get("binaryValue") is not None:
            attribute["BinaryValue"] = base64.b64decode(written["binaryValue"])
        return attribute

    def as_postponement(self, delay: Countdown) -> OutgoingMessage:
        """The same body and attributes, to become visible after *delay*, one postponement more."""
        postponed = {
            POSTPONEMENTS_ATTRIBUTE: {
                "DataType": "Number",
                "StringValue": str(self.postponements + 1),
            }
        }
        return OutgoingMessage(
            id=self.message_id,
            body=self.body,
            delay=delay,
            attributes={**self.attributes, **postponed},
        )

    def as_abandonment(self, reason: str, *, fifo: bool) -> OutgoingMessage:
        """The same body and attributes, with *reason* cut to ``REASON_MAX_CHARS``.

        Args:
            reason: Why the message is abandoned.
            fifo: Whether the target queue is FIFO: it gets the group id and a new
                deduplication id.
        """
        abandonment = {
            ABANDONMENT_REASON_ATTRIBUTE: {
                "DataType": "String",
                "StringValue": reason[:REASON_MAX_CHARS],
            }
        }
        return OutgoingMessage(
            id=self.message_id,
            body=self.body,
            group_id=(self.group_id or self.message_id) if fifo else None,
            dedup_id=uuid.uuid4().hex if fifo else None,
            attributes={**self.attributes, **abandonment},
        )


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Where a queue's abandoned messages go and after how many receives SQS sends them there.

    Attributes:
        queue: The dead-letter queue.
        max_receive_count: The ``maxReceiveCount`` of the source queue's ``RedrivePolicy``.
    """

    queue: SQSQueue
    max_receive_count: int


class SQSQueue:
    """One SQS queue: sends, and the batched settlement of what a consumer received.

    Args:
        client: The boto3 ``sqs`` client.
        name: The queue's name in ``sqs.queues``, for messages.
        config: Its URL and dead-letter policy.
    """

    def __init__(self, client: Any, name: str, config: QueueConfig) -> None:
        self._client = client
        self._config = config
        self.name = name
        self.url = config.url
        self.is_fifo = config.is_fifo
        self.dead_letter_policy = config.dead_letter
        self._dead_letter: DeadLetter | None = None

    @staticmethod
    def chunks(
        items: Sequence[_T],
        size: int = MAX_BATCH_ENTRIES,
        weight: Callable[[_T], int] | None = None,
    ) -> Iterator[list[_T]]:
        """*items* in order, at most *size* entries and ``MAX_BATCH_BYTES`` of *weight* a chunk."""
        chunk: list[_T] = []
        total = 0
        for item in items:
            item_weight = 0 if weight is None else weight(item)
            if chunk and (len(chunk) == size or total + item_weight > MAX_BATCH_BYTES):
                yield chunk
                chunk, total = [], 0
            chunk.append(item)
            total += item_weight
        if chunk:
            yield chunk

    @property
    def region(self) -> str | None:
        """The region in the URL's host; ``None`` for an endpoint that names none."""
        return self._config.region

    @property
    def account_id(self) -> str:
        """The AWS account owning the queue, the first segment of its URL path."""
        return self._config.account_id

    @property
    def queue_name(self) -> str:
        """The queue's name in AWS, the last segment of its URL path."""
        return self._config.queue_name

    def placement(
        self,
        job_type: type[Job[Any]],
        payload: Mapping[str, Any],
        task_id: str,
        delay: Countdown,
    ) -> dict[str, Any]:
        """Where a job's message goes: the delay on a standard queue; ids on a FIFO one.

        A FIFO queue takes the hash of the payload field ``__group_by__`` names as its
        ``MessageGroupId`` and the task id as its ``MessageDeduplicationId``.

        Args:
            job_type: The job the message runs.
            payload: Its payload.
            task_id: The message's task id.
            delay: Seconds before it becomes visible.

        Returns:
            The :class:`OutgoingMessage` fields that place it.

        Raises:
            SQSDispatchError: When a FIFO queue is given a delay, or the payload has no
                value for ``__group_by__``.
        """
        if not self.is_fifo:
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

    def is_at(self, event_source_arn: str) -> bool:
        """Whether *event_source_arn* names this queue, by region, account and queue name.

        A URL that names no region matches any.
        """
        parts = event_source_arn.split(":")
        if len(parts) != 6:
            return False
        _, _, _, region, account, name = parts
        same_region = self.region in (None, region)
        return same_region and (self.account_id, self.queue_name) == (account, name)

    def send(self, message: OutgoingMessage) -> None:
        """Send *message* with ``SendMessage``."""
        self._client.send_message(QueueUrl=self.url, **message.request())

    def send_batch(self, messages: Sequence[OutgoingMessage]) -> BatchResult:
        """Send *messages* in calls of at most ten entries and one MiB of bodies."""
        return self._call_in_chunks(
            self._client.send_message_batch,
            [
                [{"Id": message.id, **message.request()} for message in chunk]
                for chunk in self.chunks(messages, weight=lambda message: message.size)
            ],
        )

    def change_visibility_batch(self, delays: Sequence[tuple[SQSRecord, Countdown]]) -> BatchResult:
        """Make each record visible again after its delay, without receiving it again before."""
        entries = [
            {
                "Id": record.message_id,
                "ReceiptHandle": record.receipt_handle,
                "VisibilityTimeout": delay.seconds,
            }
            for record, delay in delays
        ]
        return self._call_in_chunks(
            self._client.change_message_visibility_batch, list(self.chunks(entries))
        )

    def postpone_batch(self, postponements: Sequence[tuple[SQSRecord, Countdown]]) -> BatchResult:
        """Send each record again with its delay and one postponement more.

        Raises:
            SQSDispatchError: When the queue is FIFO, which takes no per-message delay.
        """
        if self.is_fifo:
            raise SQSDispatchError(
                f"queue {self.name!r} is FIFO and takes no per-message delay",
                code=SQSErrorCode.FIFO_DELAY,
            )
        return self.send_batch([record.as_postponement(delay) for record, delay in postponements])

    def abandon_batch(self, abandonments: Sequence[tuple[SQSRecord, str]]) -> BatchResult:
        """Send each record to the dead-letter queue, with its reason as a message attribute.

        Raises:
            SQSQueueConfigError: When the queue has no dead-letter queue to send to.
        """
        target = self.dead_letter().queue
        return target.send_batch(
            [record.as_abandonment(reason, fifo=target.is_fifo) for record, reason in abandonments]
        )

    def dead_letter(self) -> DeadLetter:
        """The dead-letter queue :meth:`read_dead_letter` kept.

        Raises:
            SQSQueueConfigError: When the queue is ``dead_letter: discard``, or its
                dead-letter queue was not read at start-up.
        """
        self._refuse_discard()
        if self._dead_letter is None:
            raise SQSQueueConfigError(
                f"queue {self.name!r} has not read its dead-letter queue: call "
                "read_dead_letter(client) at start-up, never during an invocation"
            )
        return self._dead_letter

    def retries_for(
        self, jobs: Iterable[type[Job[Any]]], resolved: Callable[[type[Job[Any]]], ResolvedRetry]
    ) -> dict[type[Job[Any]], ResolvedRetry]:
        """The retry policy each of *jobs* has when consumed from this queue.

        With ``discard`` it is the one resolved. With ``redrive`` its
        ``max_attempts`` is the ``maxReceiveCount`` of the ``RedrivePolicy``.

        Args:
            jobs: The jobs consumed from this queue.
            resolved: Each job's resolved retry layers.

        Returns:
            Each job's policy and where each of its fields came from.

        Raises:
            ConfigError: When a job on a ``redrive`` queue has ``max_attempts`` in its own
                ``jobs.<Job>.retry``.
            SQSQueueConfigError: When a ``redrive`` queue has no ``RedrivePolicy``.
        """
        if self.dead_letter_policy is DeadLetterPolicy.DISCARD:
            return {job: resolved(job) for job in jobs}
        limit = self.dead_letter().max_receive_count
        retries: dict[type[Job[Any]], ResolvedRetry] = {}
        for job in jobs:
            retry = resolved(job)
            if retry.sources["max_attempts"] is RetrySource.JOB_CONFIG:
                raise ConfigError(
                    f"{job.__qualname__} declares jobs.<Job>.retry.max_attempts, but queue "
                    f"{self.name!r} is dead_letter: redrive, where the attempt limit is the "
                    f"RedrivePolicy maxReceiveCount ({limit}): remove max_attempts or set "
                    "the queue to dead_letter: discard"
                )
            retries[job] = ResolvedRetry(
                replace(retry.policy, max_attempts=limit),
                {**retry.sources, "max_attempts": RetrySource.REDRIVE},
            )
        return retries

    def unknown_job_policy(self) -> RetryPolicy:
        """The policy of a message that names no job this consumer serves.

        It is the loom default; on a ``redrive`` queue its ``max_attempts`` is the
        ``maxReceiveCount`` of the ``RedrivePolicy``.

        Raises:
            SQSQueueConfigError: When a ``redrive`` queue has no ``RedrivePolicy``.
        """
        if self.dead_letter_policy is DeadLetterPolicy.DISCARD:
            return DEFAULT_RETRY
        return replace(DEFAULT_RETRY, max_attempts=self.dead_letter().max_receive_count)

    def _refuse_discard(self) -> None:
        if self.dead_letter_policy is DeadLetterPolicy.DISCARD:
            raise SQSQueueConfigError(
                f"queue {self.name!r} is dead_letter: discard: it has no dead-letter queue"
            )

    def read_dead_letter(self, client: Any) -> DeadLetter:
        """Read the ``RedrivePolicy`` and the dead-letter queue's URL through *client*.

        Args:
            client: The boto3 ``sqs`` client that makes the metadata calls.

        Raises:
            SQSQueueConfigError: When the queue is ``dead_letter: discard`` or its
                ``RedrivePolicy`` is missing.
        """
        self._refuse_discard()
        attributes = client.get_queue_attributes(
            QueueUrl=self.url, AttributeNames=["RedrivePolicy"]
        )
        written = attributes.get("Attributes", {}).get("RedrivePolicy")
        if written is None:
            raise SQSQueueConfigError(
                f"queue {self.name!r} is dead_letter: redrive but has no RedrivePolicy: attach "
                "a dead-letter queue to it, or set dead_letter: discard (then max_attempts "
                "decides when a job is abandoned)"
            )
        try:
            policy = json.loads(written)
            _, _, _, _, account, name = policy["deadLetterTargetArn"].split(":")
            limit = int(policy["maxReceiveCount"])
            if limit < 1:
                raise ValueError("maxReceiveCount must be at least 1")
        except (AttributeError, ValueError, KeyError, TypeError) as exc:
            raise SQSQueueConfigError(
                f"queue {self.name!r} has a RedrivePolicy loom cannot read "
                f"({type(exc).__name__}: {exc}): it needs deadLetterTargetArn and maxReceiveCount"
            ) from exc
        url = client.get_queue_url(QueueName=name, QueueOwnerAWSAccountId=account)["QueueUrl"]
        target = SQSQueue(
            self._client, name, QueueConfig(url=url, dead_letter=DeadLetterPolicy.DISCARD)
        )
        self._dead_letter = DeadLetter(target, limit)
        return self._dead_letter

    def _call_in_chunks(
        self, call: Callable[..., Any], chunks: Sequence[Sequence[dict[str, Any]]]
    ) -> BatchResult:
        result = BatchResult()
        for chunk in chunks:
            result = result.merge(self._call_chunk(call, chunk))
        return result

    def _call_chunk(
        self, call: Callable[..., Any], entries: Sequence[dict[str, Any]]
    ) -> BatchResult:
        ids = [entry["Id"] for entry in entries]
        try:
            response = call(QueueUrl=self.url, Entries=list(entries))
        except Exception as exc:
            return BatchResult(
                failed=dict.fromkeys(ids, aws_error_code(exc)), unanswered=frozenset(ids)
            )
        failed = {entry["Id"]: entry["Code"] for entry in response.get("Failed", [])}
        return BatchResult(tuple(id_ for id_ in ids if id_ not in failed), failed)


class SQSQueues:
    """The queues of ``sqs.queues``, each built once and calling SQS only when used.

    Building one, or asking it for a queue, makes no call to SQS.

    Args:
        client: The boto3 ``sqs`` client every queue sends and settles with.
        queues: ``sqs.queues``, by name.
    """

    def __init__(self, client: Any, queues: Mapping[str, QueueConfig]) -> None:
        self._by_name = {name: SQSQueue(client, name, config) for name, config in queues.items()}

    def named(self, name: str) -> SQSQueue:
        """The queue *name* of ``sqs.queues``.

        Raises:
            SQSDispatchError: When ``sqs.queues`` maps no URL for *name*.
        """
        queue = self._by_name.get(name)
        if queue is None:
            raise SQSDispatchError(
                f"sqs.queues maps no URL for queue {name!r}", code=SQSErrorCode.QUEUE_UNKNOWN
            )
        return queue


__all__ = [
    "BatchResult",
    "DeadLetter",
    "DeadLetterPolicy",
    "OutgoingMessage",
    "QueueConfig",
    "SQSQueue",
    "SQSQueues",
    "SQSRecord",
]
