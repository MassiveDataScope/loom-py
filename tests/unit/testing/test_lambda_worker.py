"""A user test of a Lambda ``jobs`` function runs end to end over moto: no patch, no region."""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.core.plugins.optional import MissingExtraError
from loom.testing import TEST_QUEUE, LambdaEvent, LambdaWorker

_JOBS = """
from loom.core.job.job import Job
from loom.core.job.policy import Backoff


class SendEmailJob(Job[None]):
    __retry__ = Backoff(first_s=30, factor=2, max_s=300, jitter=False)

    def execute(self, marker: str) -> None:
        if marker == "fail":
            raise ValueError("smtp is down")
"""
_REJECTION = "loom.rejection_reason"

Config = Callable[[str], str]


@pytest.fixture
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Config]:
    package = tmp_path / "acme_mail"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "jobs.py").write_text(_JOBS)
    monkeypatch.syspath_prepend(str(tmp_path))

    def write(dead_letter: str) -> str:
        queue = {**TEST_QUEUE, "dead_letter": dead_letter}
        document = {
            "app": {"name": "acme-mail", "packages": ["acme_mail"], "job_service": "inline"},
            "sqs": {"queues": {"jobs": queue}},
            "lambda": {
                "functions": {"worker": {"trigger": {"source": "jobs", "queue": {"name": "jobs"}}}}
            },
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(document))
        return str(path)

    yield write
    for name in tuple(sys.modules):
        if name.split(".")[0] == "acme_mail":
            sys.modules.pop(name)


def _delivered(worker: LambdaWorker, body: str) -> dict[str, Any]:
    """Send *body* to the queue and receive it, as the event source mapping would."""
    worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody=body)
    received = worker.sqs.receive_message(
        QueueUrl=worker.queue_url,
        VisibilityTimeout=0,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )["Messages"][0]
    arn = worker.sqs.get_queue_attributes(QueueUrl=worker.queue_url, AttributeNames=["QueueArn"])
    record: dict[str, Any] = LambdaEvent.sqs_record(
        received["Body"],
        message_id=received["MessageId"],
        queue_arn=arn["Attributes"]["QueueArn"],
    )["Records"][0]
    record["receiptHandle"] = received["ReceiptHandle"]
    return record


def _job(marker: str) -> str:
    return f'{{"job": "SendEmailJob", "params": {{"marker": "{marker}"}}}}'


def _messages(worker: LambdaWorker, url: str | None) -> list[dict[str, Any]]:
    assert url is not None
    found = worker.sqs.receive_message(
        QueueUrl=url, VisibilityTimeout=0, MessageAttributeNames=["All"]
    )
    messages: list[dict[str, Any]] = found.get("Messages", [])
    return messages


class TestLambdaWorker:
    def test_a_job_that_succeeds_reports_nothing(self, config: Config) -> None:
        with LambdaWorker(config("discard"), function="worker", environment="test") as worker:
            record = _delivered(worker, _job("ok"))

            result = worker({"Records": [record]}, None)

        assert result == {"batchItemFailures": []}

    def test_a_job_that_fails_is_reported_and_stays_invisible_for_its_backoff(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("discard"), function="worker", environment="test") as worker:
            record = _delivered(worker, _job("fail"))

            result = worker({"Records": [record]}, None)

            assert result == {"batchItemFailures": [{"itemIdentifier": record["messageId"]}]}
            assert _messages(worker, worker.queue_url) == []

    def test_a_rejection_on_a_redrive_queue_lands_in_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("redrive"), function="worker", environment="test") as worker:
            record = _delivered(worker, "{")

            result = worker({"Records": [record]}, None)

            (dead,) = _messages(worker, worker.dead_letter_url)
            reason = dead["MessageAttributes"][_REJECTION]["StringValue"]
        assert (result, dead["Body"], reason) == (
            {"batchItemFailures": []},
            "{",
            "MalformedJobMessage",
        )

    def test_a_discard_queue_has_no_dead_letter_queue(self, config: Config) -> None:
        with LambdaWorker(config("discard"), function="worker", environment="test") as worker:
            assert worker.dead_letter_url is None

    def test_without_moto_the_error_names_the_extra(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "moto", None)
        monkeypatch.delitem(sys.modules, "loom.testing._moto", raising=False)

        with pytest.raises(MissingExtraError, match="testing-aws"):
            LambdaWorker(config("discard"), function="worker", environment="test")
