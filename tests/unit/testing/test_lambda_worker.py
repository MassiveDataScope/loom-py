"""A user test of a Lambda ``jobs`` function runs end to end over moto: no patch, no region."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.core.job.job import Job
from loom.core.plugins.optional import MissingExtraError
from loom.testing import TEST_QUEUE, LambdaWorker

_JOBS = """
from loom.core.job.job import Job
from loom.core.job.policy import Backoff


class SendEmailJob(Job[None]):
    __retry__ = Backoff(first_s=30, factor=2, max_s=300, jitter=False)

    def execute(self, marker: str) -> None:
        if marker == "fail":
            raise ValueError("smtp is down")


class FlakyEmailJob(Job[None]):
    __retry__ = Backoff(first_s=0, factor=1, max_s=0, jitter=False)

    def execute(self) -> None:
        raise ValueError("smtp is down")
"""
_REJECTION = "loom.rejection_reason"
_NOTHING = {"batchItemFailures": []}

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


def _job(name: str) -> type[Job[None]]:
    job: type[Job[None]] = getattr(importlib.import_module("acme_mail.jobs"), name)
    return job


def _messages(worker: LambdaWorker, url: str | None) -> list[dict[str, Any]]:
    assert url is not None
    found = worker.sqs.receive_message(
        QueueUrl=url, VisibilityTimeout=0, MessageAttributeNames=["All"]
    )
    messages: list[dict[str, Any]] = found.get("Messages", [])
    return messages


class TestLambdaWorker:
    def test_a_job_that_succeeds_reports_nothing_and_leaves_the_queue_empty(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("discard"), function="worker", environment="test") as worker:
            result = worker.deliver(_job("SendEmailJob"), params={"marker": "ok"})

            assert (result, _messages(worker, worker.queue_url)) == (_NOTHING, [])

    def test_a_job_that_fails_is_reported_and_stays_invisible_for_its_backoff(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("discard"), function="worker", environment="test") as worker:
            result = worker.deliver(_job("SendEmailJob"), params={"marker": "fail"})

            assert len(result["batchItemFailures"]) == 1
            assert (worker.redeliver(), _messages(worker, worker.queue_url)) == (_NOTHING, [])

    def test_a_failure_that_comes_back_is_rejected_when_the_attempts_run_out(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("discard"), function="worker", environment="test") as worker:
            first = worker.deliver(_job("FlakyEmailJob"))
            second = worker.redeliver()
            third = worker.redeliver()

            assert [len(r["batchItemFailures"]) for r in (first, second, third)] == [1, 1, 0]
            assert _messages(worker, worker.queue_url) == []

    def test_a_rejection_on_a_redrive_queue_lands_in_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("redrive"), function="worker", environment="test") as worker:
            worker.deliver(_job("FlakyEmailJob"))
            worker.redeliver()
            worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
            reason = dead["MessageAttributes"][_REJECTION]["StringValue"]
        assert reason == "ValueError"

    def test_a_message_that_cannot_be_read_is_rejected_to_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with LambdaWorker(config("redrive"), function="worker", environment="test") as worker:
            worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody="{")

            result = worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
        assert (result, dead["Body"]) == (_NOTHING, "{")

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
