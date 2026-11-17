"""A message loom rejects reaches the dead-letter queue and moves back to its source."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest

from loom.jobs.aws.queue import REJECTION_REASON_ATTRIBUTE, QueueConfig, SqsQueue, SqsRecord
from tests.integration.aws.conftest import AwsClientFactory, unique

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


def _arn(sqs: Any, url: str) -> str:
    attributes = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return str(attributes["Attributes"]["QueueArn"])


@pytest.fixture
def sqs(aws: AwsClientFactory) -> Any:
    return aws("sqs")


@pytest.fixture
def queues(sqs: Any) -> Iterator[tuple[str, str]]:
    dead = sqs.create_queue(QueueName=unique("loom-it-dlq"))["QueueUrl"]
    policy = {"deadLetterTargetArn": _arn(sqs, dead), "maxReceiveCount": "3"}
    source = sqs.create_queue(
        QueueName=unique("loom-it-source"), Attributes={"RedrivePolicy": json.dumps(policy)}
    )["QueueUrl"]
    yield source, dead
    sqs.delete_queue(QueueUrl=source)
    sqs.delete_queue(QueueUrl=dead)


def test_a_rejected_message_lands_in_the_dead_letter_queue_and_moves_back_to_a_destination(
    sqs: Any, queues: tuple[str, str]
) -> None:
    source, dead = queues
    sqs.send_message(QueueUrl=source, MessageBody="body")
    received = sqs.receive_message(QueueUrl=source, WaitTimeSeconds=5)["Messages"][0]
    queue = SqsQueue(sqs, "source", QueueConfig(url=source))
    queue.read_dead_letter(sqs)

    result = queue.reject_batch(
        [(SqsRecord(received["MessageId"], received["ReceiptHandle"], received["Body"]), "why")]
    )

    assert result.failed == {}
    assert queue.dead_letter().queue.url == dead
    assert queue.dead_letter().max_receive_count == 3
    in_dead = sqs.receive_message(
        QueueUrl=dead, WaitTimeSeconds=5, MessageAttributeNames=["All"], VisibilityTimeout=0
    )["Messages"][0]
    assert in_dead["MessageAttributes"][REJECTION_REASON_ATTRIBUTE]["StringValue"] == "why"

    sqs.start_message_move_task(SourceArn=_arn(sqs, dead), DestinationArn=_arn(sqs, source))
    deadline = time.monotonic() + 20
    moved: list[dict[str, Any]] = []
    while not moved and time.monotonic() < deadline:
        moved = sqs.receive_message(QueueUrl=source, WaitTimeSeconds=1).get("Messages", [])
    assert [message["Body"] for message in moved] == ["body"]
