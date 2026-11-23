"""How loom names a confluent-kafka failure without its text."""

from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException

from loom.core.logger.field import ErrorSummary


def broker_error(exc: BaseException) -> KafkaError | None:
    """Return the broker error a ``KafkaException`` wraps, if it wraps one."""
    if isinstance(exc, KafkaException) and exc.args and isinstance(exc.args[0], KafkaError):
        return exc.args[0]
    return None


def cite_failure(failure: KafkaError | BaseException) -> str:
    """Name a failure for loom's own text: the broker error name, else ``ErrorSummary.cite``."""
    error = failure if isinstance(failure, KafkaError) else broker_error(failure)
    if error is not None:
        return error.name()
    return ErrorSummary.cite(failure)
