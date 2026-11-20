"""``SQSJobSettlement`` against a real queue: each outcome does what the table says."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest

from loom.connections.aws import AwsClients
from loom.core.config import ConfigContext
from loom.core.job.outcome import Abandoned, Failed, JobOutcome, Postponed
from loom.core.job.policy import Countdown
from loom.jobs.aws.queue import POSTPONEMENTS_ATTRIBUTE, SQSRecord
from loom.jobs.aws.settlement import SQSJobSettlement
from loom.jobs.aws.sqs import SQSConfig
from tests.integration.aws.conftest import REGION, AwsClientFactory, unique

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@pytest.fixture
def sqs(aws: AwsClientFactory) -> Any:
    return aws("sqs")


@pytest.fixture
def queues(sqs: Any) -> Iterator[tuple[str, str]]:
    dead = sqs.create_queue(QueueName=unique("loom-it-dlq"))["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=dead, AttributeNames=["QueueArn"])["Attributes"]
    policy = {"deadLetterTargetArn": arn["QueueArn"], "maxReceiveCount": "5"}
    source = sqs.create_queue(
        QueueName=unique("loom-it-source"),
        Attributes={"RedrivePolicy": json.dumps(policy), "VisibilityTimeout": "30"},
    )["QueueUrl"]
    yield source, dead
    sqs.delete_queue(QueueUrl=source)
    sqs.delete_queue(QueueUrl=dead)


def _received(sqs: Any, url: str, count: int, arn: str = "", wait: int = 5) -> list[SQSRecord]:
    records: list[SQSRecord] = []
    deadline = time.monotonic() + wait * 3
    while len(records) < count and time.monotonic() < deadline:
        found = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        for message in found.get("Messages", []):
            records.append(
                SQSRecord(
                    message["MessageId"],
                    message["ReceiptHandle"],
                    message["Body"],
                    attributes=message.get("MessageAttributes", {}),
                    queue_arn=arn,
                    receive_count=int(message["Attributes"]["ApproximateReceiveCount"]),
                )
            )
    return records


def test_an_abandonment_a_postponement_and_a_failure_are_each_settled_on_a_real_queue(
    sqs: Any, queues: tuple[str, str], aws_url: str
) -> None:
    source, dead = queues
    for name in ("abandoned", "postponed", "failed"):
        sqs.send_message(QueueUrl=source, MessageBody=name)
    arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    config = SQSConfig.load(
        ConfigContext.from_dict(
            {
                "aws": {"url": aws_url, "region": REGION},
                "sqs": {"queues": {"jobs": {"url": source}}},
            }
        )
    )
    settlement = SQSJobSettlement(AwsClients(), config)
    received = {record.body: record for record in _received(sqs, source, 3, arn)}
    outcomes: dict[str, JobOutcome] = {
        "abandoned": Abandoned("tenant deleted"),
        "postponed": Postponed(Countdown(1), "warming up"),
        "failed": Failed(ValueError("boom"), Countdown(1)),
    }

    redelivered = settlement.settle_all([(received[name], outcomes[name]) for name in outcomes])

    assert [record.body for record in redelivered] == ["failed"]
    for name in ("abandoned", "postponed"):
        sqs.delete_message(QueueUrl=source, ReceiptHandle=received[name].receipt_handle)
    in_dead = _received(sqs, dead, 1)
    assert [record.body for record in in_dead] == ["abandoned"]
    again = {record.body: record for record in _received(sqs, source, 2, arn, wait=3)}
    assert set(again) == {"postponed", "failed"}
    assert again["postponed"].receive_count == 1
    assert again["postponed"].attributes[POSTPONEMENTS_ATTRIBUTE]["StringValue"] == "1"
