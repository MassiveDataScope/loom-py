"""``SqsSettlement``: one decision per message, one batch call per action and queue."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack
from typing import Any

import pytest

from loom.core.connections.aws import AwsClients
from loom.core.job.outcome import Deferred, Failed, JobOutcome, Rejected, Succeeded
from loom.core.job.policy import Countdown
from loom.jobs.aws.queue import (
    DEFERRALS_ATTRIBUTE,
    REJECTION_REASON_ATTRIBUTE,
    DeadLetterPolicy,
    QueueConfig,
    SqsRecord,
)
from loom.jobs.aws.settlement import SETTLEMENT_TIMEOUT_S, SqsSettlement
from loom.jobs.aws.sqs import SQSConfig
from tests.helpers.extras import AwsClientFactory, patched_aws_clients
from tests.helpers.sqs import ACCOUNT, FakeSqs, queue_url

pytestmark = pytest.mark.timeout(10)

_SOURCE = queue_url("jobs")
_DEAD = queue_url("jobs-dlq")
_ARN = f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs"
_FIFO_ARN = f"{_ARN}.fifo"
_POLICY = {
    _SOURCE: {
        "deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq",
        "maxReceiveCount": 4,
    }
}


def _record(name: str, *, arn: str = _ARN, **attributes: Any) -> SqsRecord:
    return SqsRecord(name, f"handle-{name}", "{}", attributes=attributes, queue_arn=arn)


def _failed(error: str = "boom", seconds: int = 4) -> Failed:
    return Failed(ValueError(error), Countdown(seconds))


class Settling:
    """A settlement over a fake SQS client, with what the fake and the client factory saw."""

    def __init__(
        self, client: FakeSqs, settlement: SqsSettlement, factory: AwsClientFactory
    ) -> None:
        self.client = client
        self.settlement = settlement
        self.factory = factory

    def settle_all(self, settled: list[tuple[SqsRecord, JobOutcome]]) -> list[SqsRecord]:
        return self.settlement.settle_all(settled)


@pytest.fixture
def settling() -> Iterator[Callable[..., Settling]]:
    with ExitStack() as stack:

        def build(dead_letter: str = "redrive", **options: Any) -> Settling:
            client = FakeSqs(_POLICY, **options)
            factory = stack.enter_context(patched_aws_clients(client))
            config = SQSConfig(
                queues={
                    "jobs": QueueConfig(url=_SOURCE, dead_letter=DeadLetterPolicy(dead_letter)),
                    "ordered": QueueConfig(
                        url=f"{_SOURCE}.fifo", dead_letter=DeadLetterPolicy.DISCARD
                    ),
                }
            )
            return Settling(client, SqsSettlement(AwsClients(), config), factory)

        yield build


def _logged(
    caplog: pytest.LogCaptureFixture, event: str | None = None, **fields: Any
) -> list[dict[str, Any]]:
    wanted = {"event": event, **fields} if event else fields
    return [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and wanted.items() <= record.msg.items()
    ]


def _ids(records: list[SqsRecord]) -> list[str]:
    return [record.message_id for record in records]


class TestSqsSettlement:
    def test_a_succeeded_message_costs_no_call_and_builds_no_client(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), Succeeded(None))])

        assert (redelivered, sum(sqs.client.calls.values()), sqs.factory.call_count) == ([], 0, 0)

    def test_a_rejected_message_goes_to_the_dead_letter_queue_with_its_reason(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), Rejected("tenant deleted"))])

        (entry,) = sqs.client.entries("send_message_batch", _DEAD)
        reason = entry["MessageAttributes"][REJECTION_REASON_ATTRIBUTE]["StringValue"]
        assert (redelivered, reason) == ([], "tenant deleted")

    def test_a_rejected_message_of_a_discard_queue_is_dropped_with_an_error_log(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling(dead_letter="discard")

        redelivered = sqs.settle_all([(_record("a"), Rejected("tenant deleted"))])

        assert (redelivered, sum(sqs.client.calls.values())) == ([], 0)
        assert len(_logged(caplog, outcome="discarded")) == 1

    def test_a_deferred_message_is_sent_again_with_its_delay_and_one_deferral_more(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), Deferred(Countdown(30), "warming up"))])

        (entry,) = sqs.client.entries("send_message_batch", _SOURCE)
        attribute = entry["MessageAttributes"][DEFERRALS_ATTRIBUTE]["StringValue"]
        assert (redelivered, entry["DelaySeconds"], attribute) == ([], 30, "1")

    def test_a_failed_message_becomes_visible_after_its_backoff_and_is_reported(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), _failed(seconds=8))])

        (entry,) = sqs.client.entries("change_message_visibility_batch", _SOURCE)
        assert (_ids(redelivered), entry["VisibilityTimeout"]) == (["a"], 8)

    def test_a_deferral_on_a_fifo_queue_is_refused_and_settled_as_a_failed_attempt(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all(
            [(_record("a", arn=_FIFO_ARN), Deferred(Countdown(30), "warming up"))]
        )

        (entry,) = sqs.client.entries("change_message_visibility_batch")
        assert (_ids(redelivered), sqs.client.calls["send_message_batch"]) == (["a"], 0)
        assert entry["VisibilityTimeout"] == 30
        refusals = [
            record for record in caplog.records if "takes no per-message delay" in str(record.msg)
        ]
        assert len(refusals) == 1

    def test_a_failed_dead_letter_send_reports_only_that_message(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(refuse=frozenset({"b"}))

        redelivered = sqs.settle_all([(_record(name), Rejected("no")) for name in "abc"])

        assert _ids(redelivered) == ["b"]

    def test_a_failed_resend_reports_only_that_message(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(refuse=frozenset({"c"}))
        deferred = Deferred(Countdown(5), "wait")

        redelivered = sqs.settle_all([(_record(name), deferred) for name in "abc"])

        assert _ids(redelivered) == ["c"]

    def test_a_failed_visibility_change_is_logged_and_the_message_is_reported_anyway(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling(refuse=frozenset({"a"}))

        redelivered = sqs.settle_all([(_record("a"), _failed()), (_record("b"), _failed())])

        (line,) = _logged(caplog, "SQS settlement entries failed")
        assert (_ids(redelivered), line["entries"]) == (["a", "b"], {"a": "Throttled"})

    def test_a_call_that_raises_reports_every_message_of_it(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(raises=RuntimeError("down"))

        redelivered = sqs.settle_all(
            [(_record("a"), Rejected("no")), (_record("b"), Deferred(Countdown(5), "wait"))]
        )

        assert _ids(redelivered) == ["a", "b"]

    def test_a_message_of_no_configured_queue_is_reported_and_nothing_is_sent(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()
        stray = _record("a", arn=f"arn:aws:sqs:eu-west-1:{ACCOUNT}:elsewhere")

        redelivered = sqs.settle_all([(stray, Rejected("no"))])

        assert (_ids(redelivered), sqs.client.batches) == (["a"], [])

    def test_the_calls_are_grouped_by_action_in_batches_of_ten_and_nothing_is_deleted(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()
        settled: list[tuple[SqsRecord, JobOutcome]] = [
            *((_record(f"f{n}"), _failed()) for n in range(25)),
            *((_record(f"r{n}"), Rejected("no")) for n in range(3)),
            *((_record(f"d{n}"), Deferred(Countdown(5), "wait")) for n in range(12)),
            (_record("ok"), Succeeded(None)),
        ]

        sqs.settle_all(settled)

        calls = [(method, len(entries)) for method, _, entries in sqs.client.batches]
        assert sorted(calls) == [
            ("change_message_visibility_batch", 5),
            ("change_message_visibility_batch", 10),
            ("change_message_visibility_batch", 10),
            ("send_message_batch", 2),
            ("send_message_batch", 3),
            ("send_message_batch", 10),
        ]
        deleted = (sqs.client.calls["delete_message"], sqs.client.calls["delete_message_batch"])
        assert deleted == (0, 0)

    def test_the_client_it_asks_for_fits_the_safety_margin(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        sqs.settle_all([(_record("a"), _failed())])

        config = sqs.factory.options[0]["config"]
        assert (config.connect_timeout, config.read_timeout) == (
            SETTLEMENT_TIMEOUT_S,
            SETTLEMENT_TIMEOUT_S,
        )
        assert config.retries["total_max_attempts"] == 1
