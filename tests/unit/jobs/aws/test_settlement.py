"""``SQSJobSettlement``: one decision per message, one batch call per action and queue."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.connections.aws import AwsClients
from loom.core.engine.events import AbandonReason
from loom.core.job.outcome import (
    Abandoned,
    Failed,
    JobAbandonment,
    JobOutcome,
    Postponed,
    Succeeded,
)
from loom.core.job.policy import Countdown
from loom.jobs.aws.queue import (
    ABANDONMENT_REASON_ATTRIBUTE,
    POSTPONEMENTS_ATTRIBUTE,
    AbandonedMessage,
    QueueConfig,
    SQSRecord,
)
from loom.jobs.aws.settlement import SETTLEMENT_TIMEOUT_S, SQSJobSettlement
from loom.jobs.aws.sqs import SQSConfig
from tests.helpers.aws import ACCOUNT, OTHER_REGION
from tests.helpers.aws_island import FakeAwsIsland
from tests.helpers.loggers import logged_events
from tests.helpers.sqs import FakeSqs, queue_url

pytestmark = pytest.mark.timeout(10)

_SOURCE = queue_url("jobs")
_DEAD = queue_url("jobs-dlq")
_ARN = f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:jobs"
_FIFO_ARN = f"{_ARN}.fifo"
_POLICY = {
    _SOURCE: {
        "deadLetterTargetArn": f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:jobs-dlq",
        "maxReceiveCount": 4,
    }
}


def _record(name: str, *, arn: str = _ARN, **attributes: Any) -> SQSRecord:
    return SQSRecord(name, f"handle-{name}", "{}", attributes=attributes, queue_arn=arn)


def _failed(error: str = "boom", seconds: int = 4) -> Failed:
    return Failed(ValueError(error), Countdown(seconds))


class Settling:
    """A settlement over a fake SQS client, with what the fake and the AWS island saw."""

    def __init__(
        self, client: FakeSqs, settlement: SQSJobSettlement, island: FakeAwsIsland
    ) -> None:
        self.client = client
        self.settlement = settlement
        self.island = island

    def settle_all(self, settled: list[tuple[SQSRecord, JobOutcome]]) -> list[SQSRecord]:
        return self.settlement.settle_all(settled)


@pytest.fixture
def settling(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Settling]:
    def build(
        abandoned: str = "dead_letter",
        prime: bool = True,
        consumes: str = "jobs",
        **options: Any,
    ) -> Settling:
        client = FakeSqs(_POLICY, **options)
        island = FakeAwsIsland(client=client).install(monkeypatch)
        config = SQSConfig(
            queues={
                "jobs": QueueConfig(url=_SOURCE, abandoned=AbandonedMessage(abandoned)),
                "ordered": QueueConfig(url=f"{_SOURCE}.fifo", abandoned=AbandonedMessage.DISCARD),
            }
        )
        settlement = SQSJobSettlement(AwsClients(), config, consumes)
        if prime and settlement.queue.abandoned is AbandonedMessage.DEAD_LETTER:
            settlement.queue.read_dead_letter(client)
        return Settling(client, settlement, island)

    return build


def _logged(
    caplog: pytest.LogCaptureFixture, event: str | None = None, **fields: Any
) -> list[dict[str, Any]]:
    wanted = {"event": event, **fields} if event else fields
    return [entry for entry in logged_events(caplog) if wanted.items() <= entry.items()]


def _ids(records: list[SQSRecord]) -> list[str]:
    return [record.message_id for record in records]


def _abandoned(reason: str = "tenant deleted") -> Abandoned:
    return Abandoned(AbandonReason.JOB_ASKED, JobAbandonment(reason))


class TestSQSJobSettlement:
    def test_a_succeeded_message_costs_no_call_and_builds_no_client(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(prime=False)

        redelivered = sqs.settle_all([(_record("a"), Succeeded(None))])

        assert (redelivered, sum(sqs.client.calls.values()), len(sqs.island.built)) == ([], 0, 0)

    def test_an_abandoned_message_goes_to_the_dead_letter_queue_with_its_reason(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), _abandoned())])

        (entry,) = sqs.client.entries("send_message_batch", _DEAD)
        reason = entry["MessageAttributes"][ABANDONMENT_REASON_ATTRIBUTE]["StringValue"]
        assert (redelivered, reason) == ([], "tenant deleted")

    def test_an_abandoned_message_of_a_discard_queue_is_dropped_with_an_error_log(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling(abandoned="discard")

        redelivered = sqs.settle_all([(_record("a"), _abandoned())])

        assert (redelivered, sum(sqs.client.calls.values())) == ([], 0)
        assert len(_logged(caplog, outcome="discarded")) == 1

    def test_a_postponed_message_is_sent_again_with_its_delay_and_one_postponement_more(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), Postponed(Countdown(30), "warming up"))])

        (entry,) = sqs.client.entries("send_message_batch", _SOURCE)
        attribute = entry["MessageAttributes"][POSTPONEMENTS_ATTRIBUTE]["StringValue"]
        assert (redelivered, entry["DelaySeconds"], attribute) == ([], 30, "1")

    def test_a_failed_message_becomes_visible_after_its_backoff_and_is_reported(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()

        redelivered = sqs.settle_all([(_record("a"), _failed(seconds=8))])

        (entry,) = sqs.client.entries("change_message_visibility_batch", _SOURCE)
        assert (_ids(redelivered), entry["VisibilityTimeout"]) == (["a"], 8)

    def test_a_failed_dead_letter_send_reports_only_that_message(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(refuse=frozenset({"b"}))

        redelivered = sqs.settle_all([(_record(name), _abandoned("no")) for name in "abc"])

        assert _ids(redelivered) == ["b"]

    def test_a_failed_resend_reports_only_that_message(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(refuse=frozenset({"c"}))
        postponed = Postponed(Countdown(5), "wait")

        redelivered = sqs.settle_all([(_record(name), postponed) for name in "abc"])

        assert _ids(redelivered) == ["c"]

    def test_a_failed_visibility_change_is_logged_and_the_message_is_reported_anyway(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling(refuse=frozenset({"a"}))

        redelivered = sqs.settle_all([(_record("a"), _failed()), (_record("b"), _failed())])

        (line,) = _logged(caplog, "SQS settlement entries failed")
        assert (_ids(redelivered), line["entries"]) == (["a", "b"], "dict")

    def test_a_call_that_raises_reports_every_message_of_it(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling(raises=RuntimeError("down"))

        redelivered = sqs.settle_all(
            [(_record("a"), _abandoned("no")), (_record("b"), Postponed(Countdown(5), "wait"))]
        )

        assert _ids(redelivered) == ["a", "b"]

    def test_dead_letter_sends_go_through_the_bounded_client_and_the_init_client_sends_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = FakeAwsIsland(make_client=lambda service, config: FakeSqs(_POLICY)).install(
            monkeypatch
        )
        clients = AwsClients()
        config = SQSConfig(queues={"jobs": QueueConfig(url=_SOURCE)})
        settlement = SQSJobSettlement(clients, config, "jobs")
        settlement.queue.read_dead_letter(clients.client("sqs", config))

        redelivered = settlement.settle_all([(_record("a"), _abandoned("no"))])

        by_attempts = island.created_by_max_attempts()
        bounded = by_attempts.pop(1)
        [retrying] = by_attempts.values()
        assert redelivered == []
        assert retrying.entries("send_message_batch") == []
        assert [(url, len(entries)) for _, url, entries in bounded.batches] == [(_DEAD, 1)]
        assert retrying.calls["get_queue_attributes"] == 1
        assert bounded.metadata_calls == 0

    def test_a_dead_letter_queue_not_read_at_init_lists_the_abandonments_without_a_call(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling(prime=False)
        sqs.client.lookup_raises = RuntimeError("must not be asked")

        redelivered = sqs.settle_all([(_record(name), _abandoned("no")) for name in "ab"])

        assert (_ids(redelivered), sqs.client.batches) == (["a", "b"], [])
        assert len([r for r in caplog.records if "settlement call raised" in str(r.msg)]) == 1
        assert not _logged(caplog, "SQS settlement entries failed")

    def test_a_message_of_no_configured_queue_is_reported_and_nothing_is_sent(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()
        stray = _record("a", arn=f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:elsewhere")

        redelivered = sqs.settle_all([(stray, _abandoned("no"))])

        assert (_ids(redelivered), sqs.client.batches) == (["a"], [])

    def test_a_foreign_arn_is_logged_once_however_many_records_come_from_it(
        self, settling: Callable[..., Settling], caplog: pytest.LogCaptureFixture
    ) -> None:
        sqs = settling()
        foreign = f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:elsewhere"
        stray = [(_record(name, arn=foreign), _abandoned("no")) for name in "ab"]

        redelivered = sqs.settle_all(stray)

        errors = _logged(caplog, queue_arn=foreign)
        assert (_ids(redelivered), len(errors)) == (["a", "b"], 1)
        assert "not the one this function consumes" in str(errors[0]["event"])

    def test_the_calls_are_grouped_by_action_in_batches_of_ten_and_nothing_is_deleted(
        self, settling: Callable[..., Settling]
    ) -> None:
        sqs = settling()
        settled: list[tuple[SQSRecord, JobOutcome]] = [
            *((_record(f"f{n}"), _failed()) for n in range(25)),
            *((_record(f"r{n}"), _abandoned("no")) for n in range(3)),
            *((_record(f"d{n}"), Postponed(Countdown(5), "wait")) for n in range(12)),
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

        _, config = sqs.island.built[0]
        assert (config.connect_timeout, config.read_timeout) == (
            SETTLEMENT_TIMEOUT_S,
            SETTLEMENT_TIMEOUT_S,
        )
        assert config.max_attempts == 1
