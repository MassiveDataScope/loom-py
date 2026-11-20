"""A user test of a Lambda ``jobs`` function runs end to end over moto."""

from __future__ import annotations

import hashlib
import http.server
import importlib
import json
import socket
import sys
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import boto3
import msgspec
import pytest
import yaml
from botocore.exceptions import ClientError
from moto import mock_aws

from loom.connections.aws import AwsClients
from loom.core.config import ConfigError
from loom.core.identity import Identity
from loom.core.job.envelope import JobEnvelope, decode_job_envelope
from loom.core.job.job import Job
from loom.core.plugins.optional import MissingExtraError
from loom.jobs.aws.sqs import SQSConfig
from loom.serverless.aws.lambda_.handler import LambdaAdapter
from loom.testing import TEST_QUEUE, LambdaWorker, Override, make
from tests.helpers.aws import ACCOUNT, OTHER_REGION, REGION, is_moto_answering

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
_ABANDONMENT_REASON = "loom.abandonment_reason"
_NOTHING = {"batchItemFailures": []}
_QUEUE_REGION = TEST_QUEUE["url"].split(".")[1]
_FIFO_URL = f"https://sqs.{_QUEUE_REGION}.amazonaws.com/{ACCOUNT}/loom-orders.fifo"

Config = Callable[..., str]
FAILING = {"marker": "fail"}


@pytest.fixture
def body() -> Callable[..., str]:
    """The wire body of a job message: the envelope of *job* with the fields a test names."""

    def build(job: str, **fields: Any) -> str:
        return msgspec.json.encode(make(JobEnvelope, job=job, task_id=None, **fields)).decode()

    return build


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
        region: str | None = _QUEUE_REGION,
        batch_size: int | None = None,
        job_service: str = "inline",
        function: dict[str, Any] | None = None,
        mapped: bool = True,
        endpoint: str | None = None,
        sqs_endpoint: str | None = None,
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
        if sqs_endpoint:
            document["sqs"]["url"] = sqs_endpoint
        if region or endpoint:
            document["aws"] = {
                **({"region": region} if region else {}),
                **({"url": endpoint} if endpoint else {}),
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

    def test_a_failure_that_comes_back_is_abandoned_when_the_attempts_run_out(
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

    def test_an_abandonment_on_a_redrive_queue_lands_in_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with _open(config("redrive")) as worker:
            worker.deliver(_job("FlakyEmailJob"))
            worker.redeliver()
            worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
            assert _left(worker, worker.queue_url) == 0
        assert dead["MessageAttributes"][_ABANDONMENT_REASON]["StringValue"] == "ValueError"

    def test_a_message_that_cannot_be_read_is_abandoned_to_the_dead_letter_queue(
        self, config: Config
    ) -> None:
        with _open(config("redrive")) as worker:
            worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody="{")

            result = worker.redeliver()

            (dead,) = _messages(worker, worker.dead_letter_url)
        assert (result, dead["Body"]) == (_NOTHING, "{")

    def test_redeliver_takes_up_to_the_batch_size_of_the_trigger(
        self, config: Config, body: Callable[..., str]
    ) -> None:
        with _open(config(batch_size=2)) as worker:
            for _ in range(3):
                worker.sqs.send_message(
                    QueueUrl=worker.queue_url, MessageBody=body("SendEmailJob", params=FAILING)
                )

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
        self, config: Config, body: Callable[..., str]
    ) -> None:
        trace = {"DataType": "String", "StringValue": "trace-9"}
        with _open(config("redrive")) as worker:
            worker.sqs.send_message(
                QueueUrl=worker.queue_url,
                MessageBody=body("FlakyEmailJob"),
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

    def test_redeliver_takes_ten_messages_by_default(
        self, config: Config, body: Callable[..., str]
    ) -> None:
        with _open(config()) as worker:
            for _ in range(12):
                worker.sqs.send_message(
                    QueueUrl=worker.queue_url, MessageBody=body("SendEmailJob", params=FAILING)
                )

            assert len(worker.redeliver()["batchItemFailures"]) == 10

    def test_redeliver_receives_at_most_ten_messages_whatever_the_batch_size(
        self, config: Config, body: Callable[..., str]
    ) -> None:
        with _open(config(batch_size=50)) as worker:
            for _ in range(12):
                worker.sqs.send_message(
                    QueueUrl=worker.queue_url, MessageBody=body("SendEmailJob", params=FAILING)
                )

            assert len(worker.redeliver()["batchItemFailures"]) == 10


class TestWhereTheQueueLives:
    @pytest.mark.parametrize("where", ["environment", "configuration"])
    def test_a_queue_of_another_region_and_account_is_created_where_the_url_says(
        self, config: Config, monkeypatch: pytest.MonkeyPatch, where: str
    ) -> None:
        url = f"https://sqs.{OTHER_REGION}.amazonaws.com/111122223333/orders"
        monkeypatch.setenv("MOTO_ACCOUNT_ID", "111122223333")
        if where == "environment":
            monkeypatch.setenv("AWS_DEFAULT_REGION", OTHER_REGION)

        with _open(config(url=url, region=OTHER_REGION if where == "configuration" else None)) as w:
            result = w.deliver(_job("SendEmailJob"), params={"marker": "ok"})

            assert (w.queue_url, result) == (url, _NOTHING)

    @pytest.mark.parametrize(
        ("url", "region", "made", "differs"),
        [
            (
                f"https://sqs.{_QUEUE_REGION}.amazonaws.com/111122223333/orders",
                _QUEUE_REGION,
                f"https://sqs.{_QUEUE_REGION}.amazonaws.com/{ACCOUNT}/orders",
                f"account '111122223333' against '{ACCOUNT}'",
            ),
            (
                TEST_QUEUE["url"],
                OTHER_REGION,
                f"https://sqs.{OTHER_REGION}.amazonaws.com/{ACCOUNT}/loom-jobs",
                f"region '{_QUEUE_REGION}' against '{OTHER_REGION}'",
            ),
            (
                f"https://sqs.{OTHER_REGION}.amazonaws.com/111122223333/orders",
                _QUEUE_REGION,
                f"https://sqs.{_QUEUE_REGION}.amazonaws.com/{ACCOUNT}/orders",
                f"region '{OTHER_REGION}' against '{_QUEUE_REGION}'; "
                f"account '111122223333' against '{ACCOUNT}'",
            ),
        ],
        ids=["account", "region", "region-and-account"],
    )
    def test_a_queue_moto_makes_elsewhere_is_refused_with_what_differs_and_the_way_out(
        self, config: Config, url: str, region: str, made: str, differs: str
    ) -> None:
        expected = (
            f"moto made the queue {made} for the configured {url} ({differs}): set aws.region "
            "or AWS_DEFAULT_REGION to the URL's region, MOTO_ACCOUNT_ID to its account, or "
            "use loom.testing.TEST_QUEUE"
        )

        with pytest.raises(ConfigError) as refused:
            _open(config(url=url, region=region))

        assert str(refused.value) == expected

    @pytest.mark.parametrize("url", [f"http://localhost:4566/{ACCOUNT}/orders", "orders"])
    def test_a_url_that_is_not_an_aws_sqs_one_is_refused_before_any_call(
        self, config: Config, url: str
    ) -> None:
        expected = (
            f"queue URL {url} is not an AWS SQS URL "
            "(https://sqs.<region>.amazonaws.com/<account>/<queue>): "
            "use loom.testing.TEST_QUEUE or a URL of that shape"
        )

        with pytest.raises(ConfigError) as refused:
            _open(config(url=url))

        assert str(refused.value) == expected

    def test_the_way_out_names_sqs_region_when_the_sqs_section_sets_one(
        self, config: Config, tmp_path: Path
    ) -> None:
        path = Path(config(region=OTHER_REGION))
        document = yaml.safe_load(path.read_text())
        document["sqs"]["region"] = OTHER_REGION
        path.write_text(yaml.safe_dump(document))

        with pytest.raises(ConfigError, match=r"set sqs\.region to the URL's region"):
            _open(str(path))

    def test_a_region_of_the_aws_profile_other_than_the_urls_is_refused(
        self, config: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        profile = tmp_path / "aws-config"
        profile.write_text("[default]\nregion = ap-south-1\n")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(profile))

        expected = (
            f"moto made the queue https://sqs.ap-south-1.amazonaws.com/{ACCOUNT}/loom-jobs "
            f"for the configured {TEST_QUEUE['url']} "
            f"(region '{_QUEUE_REGION}' against 'ap-south-1'): "
            "set aws.region or AWS_DEFAULT_REGION to the URL's region, MOTO_ACCOUNT_ID to its "
            "account, or use loom.testing.TEST_QUEUE"
        )

        with pytest.raises(ConfigError) as refused:
            _open(config(region=None))

        assert str(refused.value) == expected

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
        monkeypatch.setenv("AWS_DEFAULT_REGION", _QUEUE_REGION)

        with _open(config(job_service="sqs", region=None)) as worker:
            assert worker.deliver(_job("SendEmailJob"), params={"marker": "ok"}) == _NOTHING


class _Endpoint(http.server.BaseHTTPRequestHandler):
    requests: list[str] = []

    def do_POST(self) -> None:
        self.requests.append(self.path)
        self.send_response(500)
        self.end_headers()

    do_GET = do_POST

    def log_message(self, *_: Any) -> None:
        return None


_ENDPOINT_REFUSAL = (
    "moto did not answer the request to {endpoint}, so it would have left the process: "
    "remove aws.url and sqs.url from the configuration, AWS_ENDPOINT_URL, "
    "AWS_ENDPOINT_URL_SQS, AWS_USE_FIPS_ENDPOINT and AWS_USE_DUALSTACK_ENDPOINT from the "
    "environment, and any endpoint_url of the AWS profile"
)


class TestNothingReachesAnotherEndpoint:
    @pytest.fixture(autouse=True)
    def _no_network(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def refuse(*_: Any, **__: Any) -> None:
            raise AssertionError("a connection was attempted")

        monkeypatch.setattr(socket.socket, "connect", refuse)

    @pytest.mark.parametrize(
        "source", ["aws.url", "sqs.url", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SQS"]
    )
    def test_an_endpoint_override_is_refused_before_any_call(
        self, config: Config, monkeypatch: pytest.MonkeyPatch, source: str
    ) -> None:
        endpoint = "http://localhost:4566"
        if source == "aws.url":
            path = config(endpoint=endpoint)
        elif source == "sqs.url":
            path = config(sqs_endpoint=endpoint)
        else:
            monkeypatch.setenv(source, endpoint)
            path = config()

        with pytest.raises(ConfigError) as refused:
            _open(path)

        assert str(refused.value) == _ENDPOINT_REFUSAL.format(endpoint=endpoint)

    def test_an_endpoint_url_of_the_aws_profile_is_refused_before_any_call(
        self, config: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        endpoint = "http://localhost:4566"
        profile = tmp_path / "aws-config"
        profile.write_text(f"[default]\nregion = {_QUEUE_REGION}\nendpoint_url = {endpoint}\n")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(profile))

        with pytest.raises(ConfigError) as refused:
            _open(config(region=None))

        assert str(refused.value) == _ENDPOINT_REFUSAL.format(endpoint=endpoint)

    def test_a_live_endpoint_receives_nothing(self, config: Config) -> None:
        _Endpoint.requests = []
        server = http.server.HTTPServer(("127.0.0.1", 0), _Endpoint)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        endpoint = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            thread.start()
            with pytest.raises(ConfigError):
                _open(config(endpoint=endpoint))
        finally:
            server.shutdown()
            server.server_close()

        assert _Endpoint.requests == []

    @pytest.mark.parametrize("source", ["aws.url", "profile"])
    def test_an_aws_host_with_an_explicit_port_is_refused_not_sent(
        self, config: Config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
    ) -> None:
        endpoint = f"https://sqs.{_QUEUE_REGION}.amazonaws.com:443"
        if source == "aws.url":
            path = config(endpoint=endpoint)
        else:
            profile = tmp_path / "aws-config"
            profile.write_text(f"[default]\nregion = {_QUEUE_REGION}\nendpoint_url = {endpoint}\n")
            monkeypatch.setenv("AWS_CONFIG_FILE", str(profile))
            path = config(region=None)

        with pytest.raises(ConfigError) as refused:
            _open(path)

        assert str(refused.value) == _ENDPOINT_REFUSAL.format(endpoint=endpoint)

    def test_a_dualstack_endpoint_is_refused_not_sent(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AWS_USE_DUALSTACK_ENDPOINT", "true")
        endpoint = f"https://sqs.{_QUEUE_REGION}.api.aws"

        with pytest.raises(ConfigError) as refused:
            _open(config())

        assert str(refused.value) == _ENDPOINT_REFUSAL.format(endpoint=endpoint)

    def test_a_fips_endpoint_is_answered_by_moto_with_its_own_404_and_nothing_is_sent(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AWS_USE_FIPS_ENDPOINT", "true")

        with pytest.raises(ClientError, match="Not yet implemented"):
            _open(config())


class TestApplication:
    def test_the_sqs_client_is_the_applications_own(self, config: Config) -> None:
        with _open(config()) as worker:
            aws = worker.application.kernel.container.resolve(AwsClients)
            sqs_config = SQSConfig.load(worker.application.config)

            assert aws.client("sqs", sqs_config) is worker.sqs

    def test_overrides_reach_the_application(self, config: Config) -> None:
        marker = object()
        key = type("Marker", (), {})

        with LambdaWorker(
            config(),
            function="worker",
            environment="test",
            overrides=[Override.instance(key, marker)],
        ) as worker:
            assert worker.application.kernel.container.resolve(key) is marker

    def test_the_dead_letter_policy_allows_three_receives(self, config: Config) -> None:
        with _open(config("redrive")) as worker:
            got = worker.sqs.get_queue_attributes(
                QueueUrl=worker.queue_url, AttributeNames=["RedrivePolicy"]
            )["Attributes"]["RedrivePolicy"]

        assert json.loads(got)["maxReceiveCount"] == 3

    def test_a_fifo_url_makes_a_fifo_queue(self, config: Config) -> None:
        with _open(config(url=_FIFO_URL)) as worker:
            got = worker.sqs.get_queue_attributes(QueueUrl=worker.queue_url, AttributeNames=["All"])

        assert got["Attributes"]["FifoQueue"] == "true"


class TestLifecycle:
    def test_a_failed_init_stops_the_mock_even_when_closing_the_clients_raises(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def bind(*_: Any) -> None:
            raise RuntimeError("bind failed")

        def close(*_: Any) -> None:
            raise RuntimeError("clients failed")

        monkeypatch.setattr(LambdaAdapter, "bind", bind)
        monkeypatch.setattr(AwsClients, "close", close)

        with pytest.raises(RuntimeError, match="clients failed"):
            _open(config())

        assert not is_moto_answering()

    def test_a_closed_worker_refuses_to_deliver_or_redeliver(self, config: Config) -> None:
        worker = _open(config())
        worker.close()

        with pytest.raises(RuntimeError, match="the worker is closed"):
            worker.redeliver()
        with pytest.raises(RuntimeError, match="the worker is closed"):
            worker.deliver(_job("SendEmailJob"), params={"marker": "ok"})

    def test_a_normal_close_closes_the_applications_clients_once(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[str] = []
        original = AwsClients.close

        def close(clients: AwsClients) -> None:
            closed.append("clients")
            original(clients)

        monkeypatch.setattr(AwsClients, "close", close)

        _open(config()).close()

        assert closed == ["clients"]

    def test_a_second_close_does_nothing(self, config: Config) -> None:
        worker = _open(config())

        worker.close()
        worker.close()

    def test_closing_leaves_a_mock_the_user_entered_running(self, config: Config) -> None:
        with mock_aws():
            outside = boto3.client("sqs", region_name=REGION)
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

        assert (closed, is_moto_answering()) == (["adapter"], False)

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

        assert (closed, is_moto_answering()) == (["adapter"], False)

    @pytest.mark.parametrize("failure", ["refusal", "bind"])
    def test_a_failed_init_closes_the_applications_clients_once(
        self, config: Config, monkeypatch: pytest.MonkeyPatch, failure: str
    ) -> None:
        closed: list[str] = []
        original = AwsClients.close

        def close(clients: AwsClients) -> None:
            closed.append("clients")
            original(clients)

        def bind(*_: Any) -> None:
            raise RuntimeError("bind failed")

        monkeypatch.setattr(AwsClients, "close", close)
        if failure == "bind":
            monkeypatch.setattr(LambdaAdapter, "bind", bind)
        path = config(region=OTHER_REGION) if failure == "refusal" else config()

        with pytest.raises((ConfigError, RuntimeError)):
            _open(path)

        assert closed == ["clients"]

    def test_the_mock_stops_even_when_closing_the_clients_raises(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        worker = _open(config())
        original = AwsClients.close

        def close_then_fail(clients: AwsClients) -> None:
            original(clients)
            raise RuntimeError("clients failed")

        monkeypatch.setattr(AwsClients, "close", close_then_fail)
        with pytest.raises(RuntimeError, match="clients failed"):
            worker.close()

        assert not is_moto_answering()

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

        assert not is_moto_answering()

    def test_without_moto_the_error_names_the_extra(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "moto", None)
        monkeypatch.delitem(sys.modules, "loom.testing._moto", raising=False)

        with pytest.raises(MissingExtraError, match="testing-aws"):
            _open(config())
