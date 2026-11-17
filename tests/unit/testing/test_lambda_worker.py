"""A user test of a Lambda ``jobs`` function runs end to end over moto."""

from __future__ import annotations

import hashlib
import importlib
import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
import yaml
from moto import mock_aws

from loom.core.config import ConfigError
from loom.core.identity import Identity
from loom.core.job.envelope import decode_job_envelope
from loom.core.job.job import Job
from loom.core.plugins.optional import MissingExtraError
from loom.serverless.aws.lambda_.handler import LambdaAdapter
from loom.testing import TEST_QUEUE, LambdaWorker

_JOBS = """
from loom.core.job.job import Job
from loom.core.job.policy import Backoff

_NO_WAIT = Backoff(first_s=0, factor=1, max_s=0, jitter=False)


class SendEmailJob(Job[None]):
    __queue__ = "jobs"
    __retry__ = Backoff(first_s=30, factor=2, max_s=300, jitter=False)

    def execute(self, marker: str) -> None:
        if marker == "fail":
            raise ValueError("smtp is down")


class FlakyEmailJob(Job[None]):
    __queue__ = "jobs"
    __retry__ = _NO_WAIT

    def execute(self) -> None:
        raise ValueError("smtp is down")


class FlakyOrderJob(Job[None]):
    __queue__ = "jobs"
    __group_by__ = "customer"
    __retry__ = _NO_WAIT

    def execute(self) -> None:
        raise ValueError("warehouse is down")
"""
_REJECTION = "loom.rejection_reason"
_NOTHING = {"batchItemFailures": []}
_FIFO_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/loom-orders.fifo"

Config = Callable[..., str]


@pytest.fixture(autouse=True)
def _no_ambient_aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for name in ("AWS_DEFAULT_REGION", "AWS_REGION", "MOTO_ACCOUNT_ID", "AWS_ACCESS_KEY_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-aws-credentials"))


@pytest.fixture
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Config]:
    package = tmp_path / "acme_mail"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "jobs.py").write_text(_JOBS)
    monkeypatch.syspath_prepend(str(tmp_path))

    def write(
        dead_letter: str = "discard",
        *,
        url: str = TEST_QUEUE["url"],
        region: str | None = "us-east-1",
        batch_size: int | None = None,
        job_service: str = "inline",
        function: dict[str, Any] | None = None,
        mapped: bool = True,
    ) -> str:
        queue: dict[str, Any] = {
            "name": "jobs",
            **({"batch_size": batch_size, "batching_window_s": 1} if batch_size else {}),
        }
        document: dict[str, Any] = {
            "app": {"name": "acme-mail", "packages": ["acme_mail"], "job_service": job_service},
            "sqs": {
                "queues": {"jobs" if mapped else "other": {"url": url, "dead_letter": dead_letter}}
            },
            "lambda": {
                "functions": {
                    "worker": {"trigger": {"source": "jobs", "queue": queue}},
                    **({"public": function} if function else {}),
                }
            },
        }
        if region:
            document["aws"] = {"region": region}
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


def _open(config: str) -> LambdaWorker:
    return LambdaWorker(config, function="worker", environment="test")


def _messages(worker: LambdaWorker, url: str | None) -> list[dict[str, Any]]:
    assert url is not None
    found = worker.sqs.receive_message(
        QueueUrl=url,
        VisibilityTimeout=0,
        AttributeNames=["All"],
        MessageAttributeNames=["All"],
    )
    messages: list[dict[str, Any]] = found.get("Messages", [])
    return messages


def _left(worker: LambdaWorker, url: str | None) -> int:
    """Visible and in-flight messages of the queue at *url*."""
    assert url is not None
    names = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    got = worker.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=names)["Attributes"]
    return sum(int(got[name]) for name in names)


class TestDelivery:
    def test_a_job_that_succeeds_reports_nothing_and_leaves_no_message_or_dead_letter_queue(
        self, config: Config
    ) -> None:
        with _open(config()) as worker:
            result = worker.deliver(_job("SendEmailJob"), params={"marker": "ok"})

            assert (result, _left(worker, worker.queue_url)) == (_NOTHING, 0)
            assert worker.dead_letter_url is None

    def test_a_job_that_fails_is_reported_and_stays_invisible_for_its_backoff(
        self, config: Config
    ) -> None:
        with _open(config()) as worker:
            result = worker.deliver(_job("SendEmailJob"), params={"marker": "fail"})

            assert len(result["batchItemFailures"]) == 1
            assert (worker.redeliver(), _messages(worker, worker.queue_url)) == (_NOTHING, [])

    def test_a_failure_that_comes_back_is_rejected_when_the_attempts_run_out(
        self, config: Config
    ) -> None:
        with _open(config()) as worker:
            results = [
                worker.deliver(_job("FlakyEmailJob")),
                worker.redeliver(),
                worker.redeliver(),
            ]

            assert [len(r["batchItemFailures"]) for r in results] == [1, 1, 0]
            assert _left(worker, worker.queue_url) == 0

    def test_a_rejection_on_a_redrive_queue_lands_in_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with _open(config("redrive")) as worker:
            worker.deliver(_job("FlakyEmailJob"))
            worker.redeliver()
            worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
            assert _left(worker, worker.queue_url) == 0
        assert dead["MessageAttributes"][_REJECTION]["StringValue"] == "ValueError"

    def test_a_message_that_cannot_be_read_is_rejected_to_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with _open(config("redrive")) as worker:
            worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody="{")

            result = worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
        assert (result, dead["Body"]) == (_NOTHING, "{")

    def test_redeliver_takes_up_to_the_batch_size_of_the_trigger(self, config: Config) -> None:
        with _open(config(batch_size=2)) as worker:
            for _ in range(3):
                body = '{"job": "SendEmailJob", "params": {"marker": "fail"}}'
                worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody=body)

            sizes = [len(worker.redeliver()["batchItemFailures"]) for _ in range(2)]

        assert sizes == [2, 1]

    def test_redeliver_does_not_invoke_the_function_when_nothing_is_visible(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def refuse(*_: Any, **__: Any) -> None:
            raise AssertionError("the function was invoked")

        with _open(config()) as worker:
            monkeypatch.setattr(LambdaWorker, "__call__", refuse)

            assert worker.redeliver() == _NOTHING

    def test_a_fifo_message_takes_the_group_and_deduplication_of_the_queue_placement(
        self, config: Config
    ) -> None:
        with _open(config(url=_FIFO_URL)) as worker:
            worker.deliver(_job("FlakyOrderJob"), {"customer": "c-7"})

            (message,) = _messages(worker, worker.queue_url)
            envelope = decode_job_envelope(message["Body"])
            assert message["Attributes"]["MessageGroupId"] == hashlib.sha256(b"c-7").hexdigest()
            assert message["Attributes"]["MessageDeduplicationId"] == envelope.task_id


class TestRecords:
    def test_a_record_carries_the_group_id_of_a_fifo_message_to_its_dead_letter_queue(
        self, config: Config
    ) -> None:
        with _open(config("redrive", url=_FIFO_URL)) as worker:
            worker.deliver(_job("FlakyOrderJob"), {"customer": "c-7"})
            worker.redeliver()
            worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
        assert dead["Attributes"]["MessageGroupId"] == hashlib.sha256(b"c-7").hexdigest()

    def test_a_record_carries_the_message_attributes_to_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        trace = {"DataType": "String", "StringValue": "trace-9"}
        with _open(config("redrive")) as worker:
            worker.sqs.send_message(
                QueueUrl=worker.queue_url,
                MessageBody='{"job": "FlakyEmailJob"}',
                MessageAttributes={"trace": trace},
            )
            for _ in range(3):
                worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
        assert dead["MessageAttributes"]["trace"]["StringValue"] == "trace-9"

    def test_deliver_sends_the_identity_in_the_envelope(self, config: Config) -> None:
        with _open(config()) as worker:
            worker.deliver(_job("FlakyEmailJob"), identity=Identity(subject="alice"))

            (message,) = _messages(worker, worker.queue_url)
        assert decode_job_envelope(message["Body"]).identity["subject"] == "alice"  # type: ignore[index]

    def test_redeliver_receives_at_most_ten_messages_whatever_the_batch_size(
        self, config: Config
    ) -> None:
        with _open(config(batch_size=50)) as worker:
            for _ in range(12):
                body = '{"job": "SendEmailJob", "params": {"marker": "fail"}}'
                worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody=body)

            assert len(worker.redeliver()["batchItemFailures"]) == 10


class TestWhereTheQueueLives:
    @pytest.mark.parametrize("where", ["environment", "configuration"])
    def test_a_queue_of_another_region_and_account_is_created_where_the_url_says(
        self, config: Config, monkeypatch: pytest.MonkeyPatch, where: str
    ) -> None:
        url = "https://sqs.eu-west-1.amazonaws.com/111122223333/orders"
        monkeypatch.setenv("MOTO_ACCOUNT_ID", "111122223333")
        if where == "environment":
            monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")

        with _open(config(url=url, region="eu-west-1" if where == "configuration" else None)) as w:
            result = w.deliver(_job("SendEmailJob"), params={"marker": "ok"})

            assert (w.queue_url, result) == (url, _NOTHING)

    @pytest.mark.parametrize(
        ("url", "region", "differs"),
        [
            ("https://sqs.us-east-1.amazonaws.com/111122223333/orders", "us-east-1", "account"),
            ("http://localhost:4566/123456789012/orders", "us-east-1", "host"),
            (TEST_QUEUE["url"], "eu-west-1", "host"),
        ],
        ids=["account", "host", "configured-region"],
    )
    def test_a_queue_moto_would_make_elsewhere_is_refused_naming_both_urls_and_the_way_out(
        self, config: Config, url: str, region: str, differs: str
    ) -> None:
        with pytest.raises(ConfigError) as refused:
            _open(config(url=url, region=region))

        message = str(refused.value)
        assert url in message
        assert differs in message
        assert "AWS_DEFAULT_REGION" in message
        assert "MOTO_ACCOUNT_ID" in message
        assert "TEST_QUEUE" in message

    def test_a_region_of_the_aws_profile_other_than_the_urls_is_refused(
        self, config: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        profile = tmp_path / "aws-config"
        profile.write_text("[default]\nregion = ap-south-1\n")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(profile))

        with pytest.raises(ConfigError, match="ap-south-1"):
            _open(config(region=None))

    def test_a_queue_the_function_names_but_the_configuration_does_not_map_is_refused(
        self, config: Config
    ) -> None:
        with pytest.raises(ConfigError, match=r"sqs\.queues\.jobs is missing"):
            _open(config(mapped=False))

    def test_a_function_that_is_no_jobs_function_is_refused_naming_it(self, config: Config) -> None:
        path = config(function={"trigger": {"source": "http", "interfaces": ["Items"]}})

        with pytest.raises(ConfigError, match="lambda.functions.public is not a jobs function"):
            LambdaWorker(path, function="public", environment="test")

    def test_a_jobs_function_that_dispatches_through_the_sqs_job_service_binds(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

        with _open(config(job_service="sqs", region=None)) as worker:
            assert worker.deliver(_job("SendEmailJob"), params={"marker": "ok"}) == _NOTHING


class TestLifecycle:
    def test_a_second_close_does_nothing(self, config: Config) -> None:
        worker = _open(config())

        worker.close()
        worker.close()

    def test_closing_leaves_a_mock_the_user_entered_running(self, config: Config) -> None:
        with mock_aws():
            outside = boto3.client("sqs", region_name="us-east-1")
            outside.create_queue(QueueName="user-queue")

            with _open(config()):
                pass

            assert any("user-queue" in url for url in outside.list_queues()["QueueUrls"])

    def test_a_failing_init_closes_the_application_and_stops_the_mock(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[str] = []
        original = LambdaAdapter.close

        def bind(*_: Any) -> None:
            raise RuntimeError("bind failed")

        def close(adapter: LambdaAdapter) -> None:
            closed.append("adapter")
            original(adapter)

        monkeypatch.setattr(LambdaAdapter, "bind", bind)
        monkeypatch.setattr(LambdaAdapter, "close", close)

        with pytest.raises(RuntimeError, match="bind failed"):
            _open(config())

        assert (closed, "AWS_ACCESS_KEY_ID" in os.environ) == (["adapter"], False)

    def test_a_base_exception_during_init_still_closes_the_application_and_the_mock(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[str] = []
        original = LambdaAdapter.close

        def interrupt(*_: Any) -> None:
            raise KeyboardInterrupt

        def close(adapter: LambdaAdapter) -> None:
            closed.append("adapter")
            original(adapter)

        monkeypatch.setattr(LambdaAdapter, "bind", interrupt)
        monkeypatch.setattr(LambdaAdapter, "close", close)

        with pytest.raises(KeyboardInterrupt):
            _open(config())

        assert (closed, "AWS_ACCESS_KEY_ID" in os.environ) == (["adapter"], False)

    def test_the_mock_stops_even_when_closing_the_function_raises(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        worker = _open(config())
        original = LambdaAdapter.close

        def close_then_fail(adapter: LambdaAdapter) -> None:
            original(adapter)
            raise RuntimeError("close failed")

        monkeypatch.setattr(LambdaAdapter, "close", close_then_fail)
        with pytest.raises(RuntimeError, match="close failed"):
            worker.close()

        assert "AWS_ACCESS_KEY_ID" not in os.environ

    def test_without_moto_the_error_names_the_extra(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "moto", None)
        monkeypatch.delitem(sys.modules, "loom.testing._moto", raising=False)

        with pytest.raises(MissingExtraError, match="testing-aws"):
            _open(config())
