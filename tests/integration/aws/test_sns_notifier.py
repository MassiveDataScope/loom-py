"""The ``sns`` notifier publishes through the endpoint the flow YAML's ``aws:`` names."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.config import load_config
from loom.prefect.notify import NotifyEvent, build_notifiers
from tests.integration.aws.conftest import REGION, AwsClientFactory, ConfigWriter, unique

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]


@dataclass(frozen=True)
class _Subscribed:
    topic_arn: str
    queue_url: str


@pytest.fixture
def subscribed(aws: AwsClientFactory) -> Iterator[_Subscribed]:
    """A topic whose messages land, raw, in a queue the test reads."""
    sns, sqs = aws("sns"), aws("sqs")
    topic_arn = sns.create_topic(Name=unique("loom-it-flows"))["TopicArn"]
    queue_url = sqs.create_queue(QueueName=unique("loom-it-flow-events"))["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn["Attributes"]["QueueArn"],
        Attributes={"RawMessageDelivery": "true"},
    )
    yield _Subscribed(topic_arn, queue_url)
    sns.delete_topic(TopicArn=topic_arn)
    sqs.delete_queue(QueueUrl=queue_url)


def _flow_yaml(write_config: ConfigWriter, topic_arn: str) -> Path:
    notification = {"kind": "sns", "topic_arn": topic_arn, "on_completion": True}
    return write_config({"notifications": [notification]})


def test_a_completed_flow_reaches_the_topic(
    write_config: ConfigWriter, aws: AwsClientFactory, subscribed: _Subscribed
) -> None:
    assert subscribed.topic_arn.split(":")[3] == REGION
    path = _flow_yaml(write_config, subscribed.topic_arn)
    raw: Any = OmegaConf.to_container(load_config(str(path)))
    (notifier,) = build_notifiers(raw["notifications"], aws=raw["aws"])

    notifier.notify(
        NotifyEvent(
            flow_name="daily-orders",
            flow_run_name="run-1",
            flow_run_url="",
            state="Completed",
            correlation_id="c-1",
        )
    )

    response = aws("sqs").receive_message(QueueUrl=subscribed.queue_url, WaitTimeSeconds=5)
    (message,) = response["Messages"]
    assert message["Body"].startswith("daily-orders -> Completed")
