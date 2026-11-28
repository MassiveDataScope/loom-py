"""How loom names a confluent-kafka failure without its text."""

from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException

from loom.core.errors.summary import ErrorSummary


def broker_error(exc: BaseException) -> KafkaError | None:
    """Return the broker error a ``KafkaException`` wraps, if it wraps one."""
    if isinstance(exc, KafkaException) and exc.args and isinstance(exc.args[0], KafkaError):
        return exc.args[0]
    return None


def failure_message(failure: KafkaError | BaseException) -> str:
    """The broker error name, else ``ErrorSummary.message_for``."""
    if isinstance(failure, KafkaError):
        return failure.name()
    error = broker_error(failure)
    return ErrorSummary.message_for(failure) if error is None else error.name()
