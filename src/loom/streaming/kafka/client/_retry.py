"""Bounded retry for transient Kafka group-coordinator errors.

Every consumer-group offset operation — ``OffsetFetch`` and ``OffsetCommit`` —
is answered by the broker that coordinates the group. That coordinator is not
always available: it is elected lazily, its ``__consumer_offsets`` partition has
to be loaded into memory first, and it moves on broker restart or partition
reassignment. While that is happening the broker answers with a *retriable*
protocol error, and the correct client behaviour is to back off and ask again —
not to fail the caller.

Without this, a flow reading from a cluster whose coordinator is still settling
dies at startup with an opaque ``NOT_COORDINATOR``, which is the single most
likely way a healthy deployment fails on its first run.

Note on classification: ``KafkaError.retriable()`` is deliberately **not** used.
It reports whether librdkafka will retry the request internally, which is a
different question from whether the Kafka protocol considers the error
transient — it is ``False`` for ``NOT_COORDINATOR``. Only the explicit code set
below decides, so adding a code is a visible, reviewed change.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from confluent_kafka import KafkaError, KafkaException

from loom.streaming.kafka.client._failure import broker_error

T = TypeVar("T")

_COORDINATOR_RETRIABLE_CODES: frozenset[int] = frozenset(
    {
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
        KafkaError.NOT_COORDINATOR,
    }
)


@dataclass(frozen=True)
class CoordinatorRetryPolicy:
    """Bounded exponential backoff for group-coordinator operations.

    Attributes:
        attempts: Total number of attempts, including the first. Must be at
            least 1; ``1`` disables retrying without special-casing it.
        initial_backoff_s: Delay before the second attempt.
        backoff_multiplier: Factor applied to the delay after each retry.
    """

    attempts: int = 4
    initial_backoff_s: float = 0.25
    backoff_multiplier: float = 2.0

    def __post_init__(self) -> None:
        """Validate the policy.

        Raises:
            ValueError: If any field is outside its permitted range.
        """
        if self.attempts < 1:
            raise ValueError("CoordinatorRetryPolicy.attempts must be at least 1.")
        if self.initial_backoff_s <= 0:
            raise ValueError("CoordinatorRetryPolicy.initial_backoff_s must be positive.")
        if self.backoff_multiplier < 1:
            raise ValueError("CoordinatorRetryPolicy.backoff_multiplier must be at least 1.")

    def delay_before(self, retry_index: int) -> float:
        """Return the backoff delay preceding one zero-based retry."""
        return self.initial_backoff_s * self.backoff_multiplier**retry_index


DEFAULT_COORDINATOR_RETRY = CoordinatorRetryPolicy()


def is_coordinator_error(exc: KafkaException) -> bool:
    """Report whether a Kafka exception is a transient coordinator error."""
    error = broker_error(exc)
    return error is not None and error.code() in _COORDINATOR_RETRIABLE_CODES


def with_coordinator_retry(
    operation: Callable[[], T],
    *,
    policy: CoordinatorRetryPolicy = DEFAULT_COORDINATOR_RETRY,
) -> T:
    """Run a group-coordinator call, retrying only transient coordinator errors.

    Blocking by design: the callers are synchronous broker round-trips made
    while a Bytewax partition is being built, where the alternative to waiting
    is failing the flow.

    Args:
        operation: Zero-argument call performing one coordinator round-trip.
        policy: Attempt count and backoff schedule.

    Returns:
        Whatever ``operation`` returns.

    Raises:
        KafkaException: The last error, once attempts are exhausted or the
            error is not a transient coordinator error. Any other exception
            from ``operation`` propagates untouched on the first attempt.

    Example:
        >>> with_coordinator_retry(lambda: consumer.committed(partitions, timeout=10))
        [TopicPartition{topic=orders,partition=0,offset=42}]
    """
    for retry_index in range(policy.attempts - 1):
        try:
            return operation()
        except KafkaException as exc:
            if not is_coordinator_error(exc):
                raise
            time.sleep(policy.delay_before(retry_index))
    return operation()


__all__ = [
    "DEFAULT_COORDINATOR_RETRY",
    "CoordinatorRetryPolicy",
    "is_coordinator_error",
    "with_coordinator_retry",
]
