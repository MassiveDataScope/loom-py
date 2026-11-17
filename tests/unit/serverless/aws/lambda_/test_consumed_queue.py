"""A function consuming an SQS queue checks it at Init; only a consumer asks SQS about it."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from loom.composition import AppConfig, Application
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.jobs.aws.errors import SQSQueueConfigError
from loom.serverless.aws.lambda_ import LambdaAdapter
from tests.helpers.composition import composition_project
from tests.helpers.extras import patched_aws_clients
from tests.helpers.sqs import ACCOUNT, FakeSqs, queue_url
from tests.unit.serverless.aws.lambda_._probe import Probe
from tests.unit.serverless.aws.lambda_._records import WORKER, with_worker

_JOBS = queue_url("jobs")
_POLICY = {
    _JOBS: {
        "deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq",
        "maxReceiveCount": 4,
    }
}


def _bind(
    tmp_path: Path,
    probe: tuple[type[Job[None]], Probe],
    client: FakeSqs | None,
    *,
    queue: dict[str, Any] | None = None,
    jobs: dict[str, Any] | None = None,
    unmapped: bool = False,
    named: bool = True,
) -> Application:
    mapping = {"batch_size": 5} if named else {}
    document = with_worker({"app": {"name": "lambda-sqs"}, "jobs": jobs or {}}, **mapping)
    document["aws"] = {"region": "eu-west-1"}
    document["sqs"] = {"queues": {} if unmapped else {"jobs": {"url": _JOBS, **(queue or {})}}}
    project = composition_project(
        ConfigContext.from_dict(document),
        app=AppConfig(name="lambda-sqs", entry=WORKER),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    adapter = LambdaAdapter(jobs=[probe[0]])
    with patched_aws_clients(client) if client is not None else nullcontext():
        application = project.compose(adapter)
        adapter.bind(application)
        adapter.close()
    return application


def _logged(caplog: pytest.LogCaptureFixture, event: str) -> list[dict[str, Any]]:
    return [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == event
    ]


class TestConsumedQueue:
    def test_a_redrive_queue_with_a_policy_binds_after_one_read_of_it(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        client = FakeSqs(_POLICY)

        _bind(tmp_path, probe, client)

        assert client.calls["get_queue_attributes"] == 1

    def test_a_redrive_queue_without_a_policy_fails_start_up_naming_the_queue(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        with pytest.raises(SQSQueueConfigError) as raised:
            _bind(tmp_path, probe, FakeSqs())

        assert "'jobs'" in str(raised.value)
        assert "dead_letter: discard" in str(raised.value)

    def test_max_attempts_on_a_job_of_a_redrive_queue_fails_start_up(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "jobs", "retry": {"max_attempts": 7}}}

        with pytest.raises(
            ConfigError, match=r"declares jobs\.<Job>\.retry\.max_attempts.*maxReceiveCount"
        ):
            _bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

    def test_a_job_of_another_queue_is_not_checked_against_this_one(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "elsewhere", "retry": {"max_attempts": 7}}}

        _bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

    def test_a_discard_queue_takes_max_attempts_and_asks_sqs_nothing(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        client = FakeSqs()
        jobs = {probe[0].__name__: {"queue": "jobs", "retry": {"max_attempts": 7}}}

        _bind(tmp_path, probe, client, queue={"dead_letter": "discard"}, jobs=jobs)

        assert client.metadata_calls == 0

    def test_the_consumer_policy_leaves_the_applications_settings_untouched(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        jobs = {"defaults": {"retry": {"max_attempts": 9}}}
        jobs[probe[0].__name__] = {"queue": "jobs"}

        application = _bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

        assert application.job_settings.of(probe[0]).retry.policy.max_attempts == 9

    def test_the_consumer_logs_the_one_policy_line_of_its_jobs(
        self,
        tmp_path: Path,
        probe: tuple[type[Job[None]], Probe],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "jobs"}}

        _bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

        (logged,) = _logged(caplog, "Job retry policies")
        assert logged["queue"] == "jobs"
        assert "max_attempts=4 (RedrivePolicy maxReceiveCount)" in logged["jobs"][0]["retry"]

    def test_a_queue_that_cannot_be_read_at_start_up_fails_naming_it(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        failure = ClientError(
            {"Error": {"Code": "Throttling", "Message": "slow"}}, "GetQueueAttributes"
        )
        client = FakeSqs(_POLICY, lookup_raises=failure)

        with pytest.raises(ConfigError, match=r"queue 'jobs'.*Throttling"):
            _bind(tmp_path, probe, client)

    def test_an_error_that_is_not_the_sdks_propagates_unwrapped(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        client = FakeSqs(_POLICY, lookup_raises=TypeError("a bug"))

        with pytest.raises(TypeError, match="a bug"):
            _bind(tmp_path, probe, client)

    @pytest.mark.parametrize("named", [True, False])
    def test_the_policy_is_read_at_start_up_with_the_retrying_client_only(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], named: bool
    ) -> None:
        retrying, bounded = FakeSqs(_POLICY), FakeSqs(_POLICY)

        def session(**_: Any) -> Any:
            class Session:
                def client(self, service: str, **options: Any) -> FakeSqs:
                    attempts = options["config"].retries["total_max_attempts"]
                    return bounded if attempts == 1 else retrying

            return Session()

        jobs = {probe[0].__name__: {"queue": "jobs"}}
        with patch("boto3.Session", side_effect=session):
            _bind(tmp_path, probe, None, jobs=jobs, named=named)

        assert (retrying.calls["get_queue_attributes"], bounded.metadata_calls) == (1, 0)

    def test_a_function_declaring_no_queue_tolerates_a_redrive_queue_without_a_policy(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        _bind(tmp_path, probe, FakeSqs(), named=False)

    def test_a_consumed_queue_sqs_queues_does_not_map_fails_start_up(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe]
    ) -> None:
        with pytest.raises(ConfigError, match=r"sqs\.queues\.jobs is missing"):
            _bind(tmp_path, probe, FakeSqs(), unmapped=True)
