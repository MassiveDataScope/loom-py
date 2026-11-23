"""Unit contract for the group-coordinator retry pattern."""

from __future__ import annotations

import pytest
from confluent_kafka import KafkaError, KafkaException

from loom.streaming.kafka.client._retry import (
    CoordinatorRetryPolicy,
    is_coordinator_error,
    with_coordinator_retry,
)

pytestmark = pytest.mark.kafka

FAST = CoordinatorRetryPolicy(attempts=4, initial_backoff_s=0.001, backoff_multiplier=1.0)


def _kafka_error(code: int) -> KafkaException:
    return KafkaException(KafkaError(code))


class _FailingOperation:
    """Fail with one error code a fixed number of times, then succeed."""

    def __init__(self, failures: int, code: int) -> None:
        self._remaining = failures
        self._code = code
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise _kafka_error(self._code)
        return "ok"


class TestClassification:
    """Only the explicit coordinator codes are treated as transient."""

    @pytest.mark.parametrize(
        "code",
        [
            KafkaError.NOT_COORDINATOR,
            KafkaError.COORDINATOR_NOT_AVAILABLE,
            KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
        ],
    )
    def test_coordinator_codes_are_transient(self, code: int) -> None:
        assert is_coordinator_error(_kafka_error(code)) is True

    @pytest.mark.parametrize(
        "code",
        [KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError.TOPIC_AUTHORIZATION_FAILED],
    )
    def test_other_codes_are_not_transient(self, code: int) -> None:
        assert is_coordinator_error(_kafka_error(code)) is False

    def test_classification_does_not_rely_on_librdkafka_retriable_flag(self) -> None:
        """``KafkaError.retriable()`` answers a different question and says False here.

        Pinned deliberately: if a future refactor swaps the explicit code set for
        ``error.retriable()``, the coordinator bug silently comes back.
        """
        error = KafkaError(KafkaError.NOT_COORDINATOR)

        assert error.retriable() is False
        assert is_coordinator_error(KafkaException(error)) is True

    def test_non_kafka_payload_is_not_transient(self) -> None:
        assert is_coordinator_error(KafkaException("plain message")) is False

    def test_an_argument_less_exception_is_not_transient(self) -> None:
        assert is_coordinator_error(KafkaException()) is False


class TestRetryLoop:
    """Bounded retries, and nothing retried that should not be."""

    def test_returns_the_result_without_retrying_on_success(self) -> None:
        operation = _FailingOperation(failures=0, code=KafkaError.NOT_COORDINATOR)

        assert with_coordinator_retry(operation, policy=FAST) == "ok"
        assert operation.calls == 1

    def test_recovers_once_the_coordinator_settles(self) -> None:
        operation = _FailingOperation(failures=2, code=KafkaError.NOT_COORDINATOR)

        assert with_coordinator_retry(operation, policy=FAST) == "ok"
        assert operation.calls == 3

    def test_reraises_after_exhausting_attempts(self) -> None:
        operation = _FailingOperation(failures=99, code=KafkaError.NOT_COORDINATOR)

        with pytest.raises(KafkaException):
            with_coordinator_retry(operation, policy=FAST)
        assert operation.calls == FAST.attempts

    def test_non_coordinator_errors_fail_on_the_first_attempt(self) -> None:
        """A genuine failure must surface immediately, never after a backoff."""
        operation = _FailingOperation(failures=99, code=KafkaError.TOPIC_AUTHORIZATION_FAILED)

        with pytest.raises(KafkaException):
            with_coordinator_retry(operation, policy=FAST)
        assert operation.calls == 1

    def test_single_attempt_policy_disables_retrying(self) -> None:
        operation = _FailingOperation(failures=99, code=KafkaError.NOT_COORDINATOR)
        policy = CoordinatorRetryPolicy(attempts=1, initial_backoff_s=0.001)

        with pytest.raises(KafkaException):
            with_coordinator_retry(operation, policy=policy)
        assert operation.calls == 1

    def test_unexpected_exception_types_propagate_untouched(self) -> None:
        def operation() -> str:
            raise RuntimeError("not a kafka error")

        with pytest.raises(RuntimeError, match="not a kafka error"):
            with_coordinator_retry(operation, policy=FAST)


class TestPolicy:
    """The backoff schedule is explicit and validated."""

    def test_delay_grows_exponentially(self) -> None:
        policy = CoordinatorRetryPolicy(attempts=4, initial_backoff_s=0.25, backoff_multiplier=2.0)

        delays = [policy.delay_before(index) for index in range(3)]

        assert delays == pytest.approx([0.25, 0.5, 1.0])

    @pytest.mark.parametrize(
        ("field", "value"),
        [("attempts", 0), ("initial_backoff_s", 0.0), ("backoff_multiplier", 0.5)],
    )
    def test_invalid_policies_are_rejected(self, field: str, value: float) -> None:
        with pytest.raises(ValueError, match=field):
            CoordinatorRetryPolicy(**{field: value})  # type: ignore[arg-type]


class TestAsyncCommitVisibility:
    """A commit that fails after the call returned must still be reported."""

    def test_failed_async_commit_is_logged_with_its_partitions(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Without this the only symptom was unexplained consumer lag."""
        import logging

        from confluent_kafka import TopicPartition

        from loom.streaming.kafka._config import ConsumerSettings
        from loom.streaming.kafka.client._consumer import KafkaConsumerClient

        settings = ConsumerSettings(
            brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_least_once"
        )
        client = KafkaConsumerClient.unassigned(settings)

        with caplog.at_level(logging.ERROR):
            client._on_commit(
                KafkaError(KafkaError.REQUEST_TIMED_OUT, "broker b1.internal said no"),
                [TopicPartition("orders", 3, 91)],
            )

        [message] = [record.getMessage() for record in caplog.records]
        assert ("orders:3@91" in message, "REQUEST_TIMED_OUT" in message) == (True, True)
        assert "b1.internal" not in message

    def test_successful_async_commit_is_silent(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import logging

        from confluent_kafka import TopicPartition

        from loom.streaming.kafka._config import ConsumerSettings
        from loom.streaming.kafka.client._consumer import KafkaConsumerClient

        settings = ConsumerSettings(
            brokers=("k1:9092",), group_id="g1", topics=("orders",), delivery="at_least_once"
        )
        client = KafkaConsumerClient.unassigned(settings)

        with caplog.at_level(logging.ERROR):
            client._on_commit(None, [TopicPartition("orders", 3, 91)])

        assert caplog.records == []
