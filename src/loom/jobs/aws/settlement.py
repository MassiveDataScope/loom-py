"""Settling the SQS messages a Lambda consumer ran: one decision per message, grouped calls."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

import msgspec

from loom.core.connections.aws import AwsClients, aws_error_code
from loom.core.job.outcome import Deferred, Failed, JobOutcome, Rejected, Succeeded
from loom.core.job.policy import Countdown
from loom.core.logger import get_logger
from loom.jobs.aws.queue import (
    BatchResult,
    DeadLetterPolicy,
    SqsQueue,
    SqsQueues,
    SqsRecord,
)

if TYPE_CHECKING:
    from loom.jobs.aws.sqs import SQSConfig

_EntryT = TypeVar("_EntryT")
_log = get_logger(__name__)

SETTLEMENT_TIMEOUT_S = 1
"""Seconds a settlement call waits to connect, and again to read."""


@dataclass(slots=True)
class _Plan:
    """What the messages of one queue need, by action."""

    rejections: list[tuple[SqsRecord, str]] = field(default_factory=list)
    discards: list[tuple[SqsRecord, str]] = field(default_factory=list)
    deferrals: list[tuple[SqsRecord, Countdown]] = field(default_factory=list)
    retries: list[tuple[SqsRecord, Countdown]] = field(default_factory=list)

    @classmethod
    def of(cls, queue: SqsQueue, settled: Sequence[tuple[SqsRecord, JobOutcome]]) -> _Plan:
        """The plan for *settled*, every message received from *queue*."""
        plan = cls()
        for record, outcome in settled:
            match outcome:
                case Succeeded():
                    continue
                case Rejected(reason=reason) if (
                    queue.dead_letter_policy is DeadLetterPolicy.REDRIVE
                ):
                    plan.rejections.append((record, reason))
                case Rejected(reason=reason):
                    plan.discards.append((record, reason))
                case Deferred(delay=delay, reason=reason) if queue.is_fifo:
                    _log.error(
                        "SQS FIFO queue takes no per-message delay: the deferral is "
                        "settled as a failed attempt",
                        queue=queue.name,
                        message_id=record.message_id,
                        reason=reason,
                    )
                    plan.retries.append((record, delay))
                case Deferred(delay=delay):
                    plan.deferrals.append((record, delay))
                case Failed(delay=delay):
                    plan.retries.append((record, delay))
        return plan


class SqsSettlement:
    """Settles Lambda-received SQS messages by what their job runs came to.

    Loom never deletes a message: Lambda deletes every one the batch does not report.

    Args:
        clients: The application's AWS clients.
        config: The ``sqs`` section.
    """

    def __init__(self, clients: AwsClients, config: SQSConfig) -> None:
        self._clients = clients
        self._config = config
        self._queues: SqsQueues | None = None

    @property
    def queues(self) -> SqsQueues:
        """The configured queues, built once.

        They send and settle through a client bound to ``SETTLEMENT_TIMEOUT_S`` and read
        queue metadata through the application's own client, with its retries.
        """
        if self._queues is None:
            bounded = msgspec.structs.replace(
                self._config,
                connect_timeout=SETTLEMENT_TIMEOUT_S,
                read_timeout=SETTLEMENT_TIMEOUT_S,
                max_attempts=1,
            )
            self._queues = SqsQueues(
                self._clients.client("sqs", bounded),
                self._config.queues,
                self._clients.client("sqs", self._config),
            )
        return self._queues

    def settle_all(self, settled: Sequence[tuple[SqsRecord, JobOutcome]]) -> list[SqsRecord]:
        """Settle every message by its outcome, one batch call per action and queue.

        Args:
            settled: Each message that ran and what its run came to.

        Returns:
            The messages to report as failed: every failed one, and every rejection
            or deferral whose send did not go through.
        """
        by_queue: dict[str, list[tuple[SqsRecord, JobOutcome]]] = {}
        for item in settled:
            if not isinstance(item[1], Succeeded):
                by_queue.setdefault(item[0].queue_arn, []).append(item)
        redelivered: list[SqsRecord] = []
        for arn, group in by_queue.items():
            redelivered += self._settle_queue(arn, group)
        return redelivered

    def _settle_queue(
        self, arn: str, group: Sequence[tuple[SqsRecord, JobOutcome]]
    ) -> list[SqsRecord]:
        try:
            queue = self.queues.at(arn)
        except Exception:
            _log.exception("SQS settlement cannot reach the queue", queue_arn=arn)
            return [record for record, _ in group]
        plan = _Plan.of(queue, group)
        for record, reason in plan.discards:
            _log.error(
                "SQS message discarded: its queue has no dead-letter queue",
                queue=queue.name,
                message_id=record.message_id,
                outcome="discarded",
                reason=reason,
            )
        self._settle(queue, "change_visibility", plan.retries, queue.change_visibility_batch)
        unsent = self._settle(queue, "reject", plan.rejections, queue.reject_batch)
        unsent += self._settle(queue, "defer", plan.deferrals, queue.defer_batch)
        return [record for record, _ in plan.retries] + unsent

    @staticmethod
    def _settle(
        queue: SqsQueue,
        action: str,
        entries: Sequence[tuple[SqsRecord, _EntryT]],
        send: Callable[[Sequence[tuple[SqsRecord, _EntryT]]], BatchResult],
    ) -> list[SqsRecord]:
        if not entries:
            return []
        try:
            result = send(entries)
        except Exception as exc:
            _log.exception("SQS settlement call raised", queue=queue.name, action=action)
            code = aws_error_code(exc)
            result = BatchResult(failed={record.message_id: code for record, _ in entries})
        else:
            if result.failed:
                _log.warning(
                    "SQS settlement entries failed",
                    queue=queue.name,
                    action=action,
                    entries=dict(result.failed),
                )
        return [record for record, _ in entries if record.message_id in result.failed]


__all__ = ["SqsSettlement"]
