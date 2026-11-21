"""Settling the SQS messages a Lambda consumer ran: one decision per message, grouped calls."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar

import msgspec

from loom.connections.aws import AwsClients
from loom.core.connections.aws import aws_error_code
from loom.core.job.outcome import Abandoned, Failed, JobOutcome, Postponed, Succeeded
from loom.core.job.policy import Countdown
from loom.jobs.aws.queue import AbandonedMessage, BatchResult, SQSQueue, SQSRecord

if TYPE_CHECKING:
    from loom.jobs.aws.sqs import SQSConfig

_EntryT = TypeVar("_EntryT")
_log = logging.getLogger(__name__)

SETTLEMENT_TIMEOUT_S = 1
"""Seconds a settlement call waits to connect, and again to read."""


@dataclass(slots=True)
class _Plan:
    """What the messages of one queue need, by action."""

    abandonments: list[tuple[SQSRecord, str]] = field(default_factory=list)
    discards: list[tuple[SQSRecord, str]] = field(default_factory=list)
    postponements: list[tuple[SQSRecord, Countdown]] = field(default_factory=list)
    retries: list[tuple[SQSRecord, Countdown]] = field(default_factory=list)

    @classmethod
    def from_outcomes(
        cls, queue: SQSQueue, settled: Sequence[tuple[SQSRecord, JobOutcome]]
    ) -> _Plan:
        """The plan for *settled*, every message received from *queue*."""
        plan = cls()
        for record, outcome in settled:
            match outcome:
                case Succeeded():
                    continue
                case Abandoned() if queue.abandoned is AbandonedMessage.DEAD_LETTER:
                    plan.abandonments.append((record, outcome.statement))
                case Abandoned():
                    plan.discards.append((record, outcome.statement))
                case Postponed(delay=delay):
                    plan.postponements.append((record, delay))
                case Failed(delay=delay):
                    plan.retries.append((record, delay))
        return plan


class SQSJobSettlement:
    """Settles the messages a Lambda function received from the queue it consumes.

    Loom never deletes a message: Lambda deletes every one the batch does not report.

    Args:
        clients: The application's AWS clients.
        config: The ``sqs`` section.
        queue_name: The key of ``sqs.queues`` the function consumes.
    """

    def __init__(self, clients: AwsClients, config: SQSConfig, queue_name: str) -> None:
        self._clients = clients
        self._config = config
        self._queue_name = queue_name
        self._queue: SQSQueue | None = None

    @property
    def queue(self) -> SQSQueue:
        """The consumed queue, over a client bound to ``SETTLEMENT_TIMEOUT_S``; built once."""
        if self._queue is None:
            bounded = msgspec.structs.replace(
                self._config,
                connect_timeout=SETTLEMENT_TIMEOUT_S,
                read_timeout=SETTLEMENT_TIMEOUT_S,
                max_attempts=1,
            )
            self._queue = SQSQueue(
                self._clients.client("sqs", bounded),
                self._queue_name,
                self._config.queues[self._queue_name],
            )
        return self._queue

    def settle_all(self, settled: Sequence[tuple[SQSRecord, JobOutcome]]) -> list[SQSRecord]:
        """Settle every message by its outcome, one batch call per action.

        Args:
            settled: Each message that ran and what its run came to.

        Returns:
            The messages to report as failed: every failed one, a message of another
            queue, and every abandonment or postponement whose send did not go through.
        """
        group = [item for item in settled if not isinstance(item[1], Succeeded)]
        if not group:
            return []
        queue = self.queue
        consumed: list[tuple[SQSRecord, JobOutcome]] = []
        strays: list[SQSRecord] = []
        for item in group:
            if queue.is_at(item[0].queue_arn):
                consumed.append(item)
            else:
                strays.append(item[0])
        for arn in {record.queue_arn for record in strays}:
            _log.error(
                "SQS settlement cannot reach the queue: it is not the one this function consumes",
                extra={"queue_arn": arn, "queue": queue.name},
            )
        return strays + self._settle_queue(queue, consumed)

    def _settle_queue(
        self, queue: SQSQueue, group: Sequence[tuple[SQSRecord, JobOutcome]]
    ) -> list[SQSRecord]:
        plan = _Plan.from_outcomes(queue, group)
        for record, reason in plan.discards:
            _log.error(
                "SQS message discarded: its queue has no dead-letter queue",
                extra={
                    "queue": queue.name,
                    "message_id": record.message_id,
                    "outcome": "discarded",
                    "reason": reason,
                },
            )
        self._settle(queue, "change_visibility", plan.retries, queue.change_visibility_batch)
        unsent = self._settle(queue, "abandon", plan.abandonments, queue.abandon_batch)
        unsent += self._settle(queue, "postpone", plan.postponements, queue.postpone_batch)
        return [record for record, _ in plan.retries] + unsent

    @staticmethod
    def _settle(
        queue: SQSQueue,
        action: str,
        entries: Sequence[tuple[SQSRecord, _EntryT]],
        send: Callable[[Sequence[tuple[SQSRecord, _EntryT]]], BatchResult],
    ) -> list[SQSRecord]:
        if not entries:
            return []
        try:
            result = send(entries)
        except Exception as exc:
            _log.exception(
                "SQS settlement call raised", extra={"queue": queue.name, "action": action}
            )
            code = aws_error_code(exc)
            result = BatchResult(failed={record.message_id: code for record, _ in entries})
        else:
            if result.failed:
                _log.warning(
                    "SQS settlement entries failed",
                    extra={"queue": queue.name, "action": action, "entries": dict(result.failed)},
                )
        return [record for record, _ in entries if record.message_id in result.failed]


__all__ = ["SQSJobSettlement"]
