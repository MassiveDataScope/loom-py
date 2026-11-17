"""A function consuming an SQS queue checks it at Init; only a consumer asks SQS about it."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig
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
    client: FakeSqs,
    *,
    queue: dict[str, Any] | None = None,
    jobs: dict[str, Any] | None = None,
) -> None:
    document = with_worker({"app": {"name": "lambda-sqs"}, "jobs": jobs or {}}, batch_size=5)
    document["sqs"] = {"queues": {"jobs": {"url": _JOBS, **(queue or {})}}}
    project = composition_project(
        ConfigContext.from_dict(document),
        app=AppConfig(name="lambda-sqs", entry=WORKER),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    adapter = LambdaAdapter(jobs=[probe[0]])
    with patched_aws_clients(client):
        application = project.compose(adapter)
        adapter.bind(application)
        adapter.close()


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

        with pytest.raises(ConfigError, match="declares max_attempts.*maxReceiveCount"):
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
