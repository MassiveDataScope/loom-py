"""``SqsQueue`` and ``SqsQueues``: batched sends and settlement, the dead-letter queue, config."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.job.config import JobSettings, RetrySource
from loom.core.job.job import Job
from loom.core.job.policy import DEFAULT_RETRY, Countdown
from loom.jobs.aws.errors import SQSDispatchError, SQSQueueConfigError
from loom.jobs.aws.queue import (
    DEFERRALS_ATTRIBUTE,
    REASON_MAX_CHARS,
    REJECTION_REASON_ATTRIBUTE,
    DeadLetterPolicy,
    OutgoingMessage,
    QueueConfig,
    SqsQueue,
    SqsQueues,
    SqsRecord,
)
from loom.jobs.aws.sqs import SQSConfig
from tests.helpers.sqs import ACCOUNT, FakeSqs, queue_url

pytestmark = pytest.mark.timeout(10)

_SOURCE = queue_url("jobs")
_DEAD = queue_url("jobs-dlq")
_FIFO_DEAD = queue_url("jobs-dlq.fifo")
_POLICY = {"deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq", "maxReceiveCount": 4}
_FIFO_POLICY = {**_POLICY, "deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq.fifo"}


class EmailJob(Job[None]):
    def execute(self) -> None:
        return None


class ReportJob(Job[None]):
    def execute(self) -> None:
        return None


def _records(count: int, **attributes: Any) -> list[SqsRecord]:
    return [
        SqsRecord(f"m{index}", f"h{index}", f"body-{index}", attributes=attributes)
        for index in range(count)
    ]


def _queue(
    client: FakeSqs, *, url: str = _SOURCE, policy: DeadLetterPolicy = DeadLetterPolicy.REDRIVE
) -> SqsQueue:
    return SqsQueue(client, "jobs", QueueConfig(url=url, dead_letter=policy))


def _operations(queue: SqsQueue) -> dict[str, Any]:
    """Each batch operation on 25 records, by the client method it calls."""
    records = _records(25)
    pairs = [(record, Countdown(3)) for record in records]
    return {
        "send_message_batch": lambda: queue.send_batch(
            [OutgoingMessage(r.message_id, "b") for r in records]
        ),
        "change_message_visibility_batch": lambda: queue.change_visibility_batch(pairs),
        "defer": lambda: queue.defer_batch(pairs),
        "reject": lambda: queue.reject_batch([(record, "no") for record in records]),
    }


class TestBatches:
    @pytest.mark.parametrize(
        "operation", ["send_message_batch", "change_message_visibility_batch", "defer", "reject"]
    )
    def test_25_entries_go_in_three_calls_of_at_most_ten(self, operation: str) -> None:
        client = FakeSqs({_SOURCE: _POLICY})
        result = _operations(_queue(client))[operation]()

        assert [len(entries) for _, _, entries in client.batches] == [10, 10, 5]
        assert len(result.succeeded) == 25
        assert result.failed == {}

    def test_eleven_entries_are_two_calls(self) -> None:
        client = FakeSqs()
        _queue(client).send_batch([OutgoingMessage(f"m{i}", "b") for i in range(11)])

        assert [len(entries) for _, _, entries in client.batches] == [10, 1]

    def test_bodies_over_a_mebibyte_split_the_call(self) -> None:
        client = FakeSqs()
        big = "x" * (400 * 1024)
        _queue(client).send_batch([OutgoingMessage(f"m{i}", big) for i in range(3)])

        assert [len(entries) for _, _, entries in client.batches] == [2, 1]

    def test_a_partial_failure_returns_only_the_failed_ids_with_their_code(self) -> None:
        client = FakeSqs(refuse=frozenset({"m3", "m12"}))
        result = _queue(client).change_visibility_batch(
            [(record, Countdown(1)) for record in _records(15)]
        )

        assert result.failed == {"m3": "Throttled", "m12": "Throttled"}
        assert set(result.succeeded) == {f"m{i}" for i in range(15)} - {"m3", "m12"}
        assert result.refused == ["m3", "m12"]

    def test_a_call_that_raises_fails_its_entries_and_never_raises(self) -> None:
        error = type("Denied", (Exception,), {"response": {"Error": {"Code": "AccessDenied"}}})()
        client = FakeSqs(raises=error)
        result = _queue(client).send_batch([OutgoingMessage(f"m{i}", "b") for i in range(12)])

        assert result.succeeded == ()
        assert set(result.failed.values()) == {"AccessDenied"}
        assert len(result.failed) == 12
        assert result.refused == []

    def test_a_visibility_change_names_each_receipt_handle_and_delay(self) -> None:
        client = FakeSqs()
        _queue(client).change_visibility_batch([(SqsRecord("m1", "h1", "b"), Countdown(7))])

        assert client.batches == [
            (
                "change_message_visibility_batch",
                _SOURCE,
                [{"Id": "m1", "ReceiptHandle": "h1", "VisibilityTimeout": 7}],
            )
        ]

    def test_the_queue_has_no_way_to_delete_a_message(self) -> None:
        assert [name for name in dir(SqsQueue) if "delete" in name.lower()] == []


class TestReject:
    def test_a_rejection_goes_to_the_dead_letter_queue_with_its_reason_and_attributes(self) -> None:
        client = FakeSqs({_SOURCE: _POLICY})
        owner = {"DataType": "String", "StringValue": "acme"}
        record = SqsRecord("m1", "h1", "the-body", attributes={"owner": owner})

        _queue(client).reject_batch([(record, "bad payload")])

        ((_, url, (entry,)),) = client.batches
        assert url == _DEAD
        assert entry["MessageBody"] == "the-body"
        assert entry["MessageAttributes"] == {
            "owner": owner,
            REJECTION_REASON_ATTRIBUTE: {"DataType": "String", "StringValue": "bad payload"},
        }
        assert "MessageGroupId" not in entry

    def test_a_long_reason_is_cut(self) -> None:
        client = FakeSqs({_SOURCE: _POLICY})

        _queue(client).reject_batch([(SqsRecord("m1", "h1", "b"), "r" * 1000)])

        reason = client.batches[0][2][0]["MessageAttributes"][REJECTION_REASON_ATTRIBUTE]
        assert reason["StringValue"] == "r" * REASON_MAX_CHARS

    def test_a_fifo_dead_letter_queue_gets_the_group_id_and_a_new_deduplication_id(self) -> None:
        client = FakeSqs({_SOURCE: _FIFO_POLICY})
        record = SqsRecord("m1", "h1", "b", group_id="group-7")

        _queue(client).reject_batch([(record, "no"), (SqsRecord("m2", "h2", "b", "group-7"), "no")])
        _queue(client).reject_batch([(record, "no")])

        first, second, third = (entry for _, _, entries in client.batches for entry in entries)
        assert client.batches[0][1] == _FIFO_DEAD
        assert first["MessageGroupId"] == second["MessageGroupId"] == "group-7"
        assert len({first["MessageDeduplicationId"], second["MessageDeduplicationId"]}) == 2
        assert third["MessageDeduplicationId"] != first["MessageDeduplicationId"]
        assert "m1" not in first["MessageDeduplicationId"]

    def test_the_dead_letter_queue_is_read_once_for_many_rejections(self) -> None:
        client = FakeSqs({_SOURCE: _POLICY})
        queue = _queue(client)

        for _ in range(5):
            queue.reject_batch([(record, "no") for record in _records(12)])

        assert client.calls["get_queue_attributes"] == 1
        assert client.calls["get_queue_url"] == 1
        assert queue.dead_letter().max_receive_count == 4

    def test_a_redrive_queue_without_a_policy_names_the_queue_and_both_options(self) -> None:
        queue = _queue(FakeSqs())

        with pytest.raises(SQSQueueConfigError) as raised:
            queue.dead_letter()

        message = str(raised.value)
        assert "'jobs'" in message
        assert "RedrivePolicy" in message
        assert "dead_letter: discard" in message

    def test_a_discard_queue_has_no_dead_letter_queue_to_reject_to(self) -> None:
        client = FakeSqs({_SOURCE: _POLICY})

        with pytest.raises(SQSQueueConfigError, match="dead_letter: discard"):
            _queue(client, policy=DeadLetterPolicy.DISCARD).reject_batch(
                [(SqsRecord("m1", "h1", "b"), "no")]
            )

        assert client.metadata_calls == 0


class TestDefer:
    def test_a_deferral_resends_body_and_attributes_with_the_delay_and_one_more(self) -> None:
        client = FakeSqs()
        owner = {"DataType": "String", "StringValue": "acme"}
        record = SqsRecord("m1", "h1", "the-body", attributes={"owner": owner})

        _queue(client).defer_batch([(record, Countdown(30))])

        ((_, url, (entry,)),) = client.batches
        assert url == _SOURCE
        assert entry["MessageBody"] == "the-body"
        assert entry["DelaySeconds"] == 30
        assert entry["MessageAttributes"] == {
            "owner": owner,
            DEFERRALS_ATTRIBUTE: {"DataType": "Number", "StringValue": "1"},
        }

    def test_each_deferral_counts_on_from_the_message_attribute(self) -> None:
        client = FakeSqs()
        counted = {DEFERRALS_ATTRIBUTE: {"DataType": "Number", "StringValue": "4"}}

        _queue(client).defer_batch([(SqsRecord("m1", "h1", "b", attributes=counted), Countdown(0))])

        entry = client.batches[0][2][0]
        assert entry["MessageAttributes"][DEFERRALS_ATTRIBUTE]["StringValue"] == "5"
        assert "DelaySeconds" not in entry

    def test_a_fifo_queue_refuses_a_deferral(self) -> None:
        client = FakeSqs()

        with pytest.raises(SQSDispatchError, match="FIFO"):
            _queue(client, url=queue_url("jobs.fifo")).defer_batch(
                [(SqsRecord("m1", "h1", "b"), Countdown(1))]
            )

        assert client.batches == []


class TestRecord:
    def test_a_lambda_record_reads_into_the_api_shape(self) -> None:
        event = {
            "messageId": "m1",
            "receiptHandle": "h1",
            "body": "b",
            "attributes": {"MessageGroupId": "g"},
            "messageAttributes": {
                DEFERRALS_ATTRIBUTE: {"stringValue": "2", "dataType": "Number"},
                "blob": {"binaryValue": "AQI=", "dataType": "Binary"},
            },
        }

        record = SqsRecord.from_event(event)

        assert (record.group_id, record.deferrals) == ("g", 2)
        assert record.attributes["blob"] == {"DataType": "Binary", "BinaryValue": b"\x01\x02"}


class TestQueues:
    @pytest.fixture
    def queues(self) -> tuple[SqsQueues, FakeSqs]:
        client = FakeSqs()
        return (
            SqsQueues(
                client,
                {
                    "jobs": QueueConfig(url=_SOURCE),
                    "orders": QueueConfig(url=queue_url("orders.fifo")),
                    "other": QueueConfig(
                        url="https://sqs.eu-west-1.amazonaws.com/999999999999/jobs"
                    ),
                },
            ),
            client,
        )

    @pytest.mark.parametrize(
        ("arn", "name"),
        [
            (f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs", "jobs"),
            (f"arn:aws:sqs:eu-west-1:{ACCOUNT}:orders.fifo", "orders"),
            ("arn:aws:sqs:eu-west-1:999999999999:jobs", "other"),
        ],
    )
    def test_an_event_source_arn_matches_account_and_queue_name(
        self, queues: tuple[SqsQueues, FakeSqs], arn: str, name: str
    ) -> None:
        assert queues[0].at(arn).name == name

    @pytest.mark.parametrize(
        "arn",
        [
            f"arn:aws:sqs:eu-west-1:{ACCOUNT}:unknown",
            "arn:aws:sqs:eu-west-1:111111111111:jobs",
            f"arn:aws:sqs:us-east-1:{ACCOUNT}:jobs",
        ],
    )
    def test_an_arn_of_no_configured_queue_is_refused(
        self, queues: tuple[SqsQueues, FakeSqs], arn: str
    ) -> None:
        with pytest.raises(SQSDispatchError, match="no queue of sqs.queues"):
            queues[0].at(arn)

    def test_an_unmapped_name_is_refused(self, queues: tuple[SqsQueues, FakeSqs]) -> None:
        with pytest.raises(SQSDispatchError, match="maps no URL for queue 'missing'"):
            queues[0].named("missing")

    def test_finding_a_queue_calls_nothing(self, queues: tuple[SqsQueues, FakeSqs]) -> None:
        queues[0].named("jobs")
        queues[0].at(f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs")

        assert queues[1].calls == {}


class TestConfig:
    def test_a_queue_is_a_url_and_redrive_by_default(self) -> None:
        config = SQSConfig.load(
            ConfigContext.from_dict(
                {
                    "sqs": {
                        "queues": {
                            "jobs": {"url": _SOURCE},
                            "mail": {"url": _DEAD, "dead_letter": "discard"},
                        }
                    }
                }
            )
        )

        assert config.queues == {
            "jobs": QueueConfig(url=_SOURCE, dead_letter=DeadLetterPolicy.REDRIVE),
            "mail": QueueConfig(url=_DEAD, dead_letter=DeadLetterPolicy.DISCARD),
        }

    def test_a_queue_written_as_a_bare_url_is_refused_naming_the_mapping(self) -> None:
        with pytest.raises(ConfigError) as raised:
            SQSConfig.load(ConfigContext.from_dict({"sqs": {"queues": {"jobs": _SOURCE}}}))

        assert str(raised.value) == (
            f"sqs.queues.jobs is a URL; write it as a mapping: "
            f"{{url: {_SOURCE}, dead_letter: redrive|discard}}"
        )

    @pytest.mark.parametrize("queues", ["https://sqs.eu-west-1.amazonaws.com/1/q", ["a", "b"]])
    def test_queues_that_are_no_mapping_are_refused_naming_the_expected_shape(
        self, queues: Any
    ) -> None:
        with pytest.raises(ConfigError, match=r"sqs\.queues must be a mapping of queue name to"):
            SQSConfig.load(ConfigContext.from_dict({"sqs": {"queues": queues}}))

    def test_an_unknown_dead_letter_policy_is_refused(self) -> None:
        document = {"sqs": {"queues": {"jobs": {"url": _SOURCE, "dead_letter": "bin"}}}}

        with pytest.raises(ConfigError, match="dead_letter"):
            SQSConfig.load(ConfigContext.from_dict(document))


class TestAttemptLimit:
    @staticmethod
    def _settings(**jobs: dict[str, Any]) -> JobSettings:
        return JobSettings.from_config(
            ConfigContext.from_dict({"jobs": jobs}), [EmailJob, ReportJob]
        )

    def test_a_redrive_queue_limits_attempts_to_its_max_receive_count(self) -> None:
        settings = self._settings()
        queue = _queue(FakeSqs({_SOURCE: _POLICY}))

        retries = queue.consumed_retries([EmailJob], lambda job: settings.of(job).retry)

        assert retries[EmailJob].policy.max_attempts == 4
        assert retries[EmailJob].sources["max_attempts"] is RetrySource.REDRIVE

    def test_declaring_max_attempts_for_a_job_on_a_redrive_queue_is_refused(self) -> None:
        settings = self._settings(EmailJob={"retry": {"max_attempts": 5}})
        queue = _queue(FakeSqs({_SOURCE: _POLICY}))

        with pytest.raises(ConfigError) as raised:
            queue.consumed_retries([ReportJob, EmailJob], lambda job: settings.of(job).retry)

        message = str(raised.value)
        assert "EmailJob declares jobs.<Job>.retry.max_attempts" in message
        assert "maxReceiveCount (4)" in message
        assert "dead_letter: discard" in message

    def test_the_defaults_layer_does_not_apply_to_a_redrive_queue(self) -> None:
        settings = self._settings(defaults={"retry": {"max_attempts": 2}})
        queue = _queue(FakeSqs({_SOURCE: _POLICY}))

        retries = queue.consumed_retries([EmailJob], lambda job: settings.of(job).retry)

        assert retries[EmailJob].policy.max_attempts == 4

    @pytest.mark.parametrize(("declared", "expected"), [(None, 3), (5, 5)])
    def test_a_discard_queue_uses_max_attempts_and_asks_sqs_nothing(
        self, declared: int | None, expected: int
    ) -> None:
        retry = {} if declared is None else {"retry": {"max_attempts": declared}}
        settings = self._settings(EmailJob=retry)
        client = FakeSqs({_SOURCE: _POLICY})
        queue = _queue(client, policy=DeadLetterPolicy.DISCARD)

        retries = queue.consumed_retries([EmailJob], lambda job: settings.of(job).retry)

        assert retries[EmailJob].policy.max_attempts == expected
        assert client.metadata_calls == 0

    def test_a_redrive_queue_without_a_policy_is_refused(self) -> None:
        settings = self._settings()

        with pytest.raises(SQSQueueConfigError, match="RedrivePolicy"):
            _queue(FakeSqs()).consumed_retries([EmailJob], lambda job: settings.of(job).retry)


class TestWire:
    def test_a_request_carries_every_field_the_message_sets(self) -> None:
        message = OutgoingMessage(
            "m1",
            "b",
            delay=Countdown(5),
            group_id="g",
            dedup_id="d",
            attributes={"k": {"DataType": "String", "StringValue": "v"}},
        )

        assert message.request() == {
            "MessageBody": "b",
            "DelaySeconds": 5,
            "MessageGroupId": "g",
            "MessageDeduplicationId": "d",
            "MessageAttributes": {"k": {"DataType": "String", "StringValue": "v"}},
        }


class TestSqsRecordFromEvent:
    def test_a_record_reads_its_receive_count_queue_and_fifo_marker(self) -> None:
        raw = {
            "messageId": "m",
            "receiptHandle": "h",
            "body": "b",
            "attributes": {"ApproximateReceiveCount": "3"},
            "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:orders.fifo",
        }

        record = SqsRecord.from_event(raw)

        assert (record.receive_count, record.is_fifo, record.is_settleable) == (3, True, True)

    @pytest.mark.parametrize("raw", ["bad", {"body": "{"}, {"messageId": "m", "body": "b"}])
    def test_a_record_that_lacks_what_settlement_needs_is_not_settleable(self, raw: Any) -> None:
        assert SqsRecord.from_event(raw).is_settleable is False

    def test_an_unreadable_record_has_an_empty_id(self) -> None:
        assert SqsRecord.from_event("bad").message_id == ""

    def test_a_standard_queue_record_is_not_fifo(self) -> None:
        assert SqsRecord.from_event({"eventSourceARN": "arn:aws:sqs:r:1:orders"}).is_fifo is False


class TestUnservedPolicy:
    def test_a_discard_queue_gives_the_loom_default(self) -> None:
        queue = _queue(FakeSqs(), policy=DeadLetterPolicy.DISCARD)

        assert queue.unserved_policy() == DEFAULT_RETRY

    def test_a_redrive_queue_limits_attempts_as_its_policy_does(self) -> None:
        queue = _queue(FakeSqs({_SOURCE: _POLICY}))

        assert queue.unserved_policy().max_attempts == 4
