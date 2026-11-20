"""A function consuming an SQS queue checks it at Init; only a consumer asks SQS about it."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, Application
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.jobs.aws.errors import SQSQueueConfigError
from loom.serverless.aws.lambda_ import LambdaAdapter
from tests.helpers.aws import ACCOUNT, OTHER_REGION
from tests.helpers.aws_island import FakeAwsIsland, FakeSdkError
from tests.helpers.composition import composition_project
from tests.helpers.sqs import FakeSqs, queue_url
from tests.unit.serverless.aws.lambda_._probe import Probe
from tests.unit.serverless.aws.lambda_._records import WORKER, with_worker

_JOBS = queue_url("jobs")
_POLICY = {
    _JOBS: {
        "deadLetterTargetArn": f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:jobs-dlq",
        "maxReceiveCount": 4,
    }
}


Bind = Callable[..., Application]


@pytest.fixture
def bind(monkeypatch: pytest.MonkeyPatch) -> Bind:
    """Compose, bind and close a jobs function over *client*; a ``None`` client leaves AWS as is."""

    def build(
        tmp_path: Path,
        probe: tuple[type[Job[None]], Probe],
        client: FakeSqs | None,
        *,
        queue: dict[str, Any] | None = None,
        jobs: dict[str, Any] | None = None,
        unmapped: bool = False,
    ) -> Application:
        document = with_worker({"app": {"name": "lambda-sqs"}, "jobs": jobs or {}}, batch_size=5)
        document["aws"] = {"region": OTHER_REGION}
        document["sqs"] = {"queues": {} if unmapped else {"jobs": {"url": _JOBS, **(queue or {})}}}
        project = composition_project(
            ConfigContext.from_dict(document),
            app=AppConfig(name="lambda-sqs", entry=WORKER),
            code_path=tmp_path,
            config_root=tmp_path,
        )
        adapter = LambdaAdapter(jobs=[probe[0]])
        if client is not None:
            FakeAwsIsland(client=client).install(monkeypatch)
        application = project.compose(adapter)
        adapter.bind(application)
        adapter.close()
        return application

    return build


def _logged(caplog: pytest.LogCaptureFixture, event: str) -> list[dict[str, Any]]:
    return [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == event
    ]


class TestConsumedQueue:
    def test_a_redrive_queue_with_a_policy_binds_after_one_read_of_it(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        client = FakeSqs(_POLICY)

        bind(tmp_path, probe, client)

        assert client.calls["get_queue_attributes"] == 1

    def test_a_redrive_queue_without_a_policy_fails_start_up_naming_the_queue(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        with pytest.raises(SQSQueueConfigError) as raised:
            bind(tmp_path, probe, FakeSqs())

        assert "'jobs'" in str(raised.value)
        assert "dead_letter: discard" in str(raised.value)

    def test_max_attempts_on_a_job_of_a_redrive_queue_fails_start_up(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "jobs", "retry": {"max_attempts": 7}}}

        with pytest.raises(
            ConfigError, match=r"declares jobs\.<Job>\.retry\.max_attempts.*maxReceiveCount"
        ):
            bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

    def test_a_job_of_another_queue_is_not_checked_against_this_one(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "elsewhere", "retry": {"max_attempts": 7}}}

        bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

    def test_a_discard_queue_takes_max_attempts_and_asks_sqs_nothing(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        client = FakeSqs()
        jobs = {probe[0].__name__: {"queue": "jobs", "retry": {"max_attempts": 7}}}

        bind(tmp_path, probe, client, queue={"dead_letter": "discard"}, jobs=jobs)

        assert client.metadata_calls == 0

    def test_the_consumer_policy_leaves_the_applications_settings_untouched(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        jobs = {"defaults": {"retry": {"max_attempts": 9}}}
        jobs[probe[0].__name__] = {"queue": "jobs"}

        application = bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

        assert application.job_settings.of(probe[0]).retry.policy.max_attempts == 9

    def test_the_consumer_logs_the_one_policy_line_of_its_jobs(
        self,
        tmp_path: Path,
        probe: tuple[type[Job[None]], Probe],
        caplog: pytest.LogCaptureFixture,
        bind: Bind,
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "jobs"}}

        bind(tmp_path, probe, FakeSqs(_POLICY), jobs=jobs)

        (logged,) = [
            line for line in _logged(caplog, "Job retry policies") if line["transport"] == "sqs"
        ]
        assert logged["queue"] == "jobs"
        assert "max_attempts=4 (RedrivePolicy maxReceiveCount)" in logged["jobs"][0]["retry"]

    def test_a_queue_that_cannot_be_read_at_start_up_fails_naming_it(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], mocked_aws: None, bind: Bind
    ) -> None:
        with pytest.raises(ConfigError, match=r"queue 'jobs'.*NonExistentQueue"):
            bind(tmp_path, probe, None)

    def test_an_error_that_is_not_the_sdks_propagates_unwrapped(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        client = FakeSqs(_POLICY, lookup_raises=TypeError("a bug"))

        with pytest.raises(TypeError, match="a bug"):
            bind(tmp_path, probe, client)

    def test_a_fake_sdk_error_at_init_fails_naming_the_queue_through_the_fake_island(
        self,
        tmp_path: Path,
        probe: tuple[type[Job[None]], Probe],
        monkeypatch: pytest.MonkeyPatch,
        bind: Bind,
    ) -> None:
        FakeAwsIsland(
            make_client=lambda service, config: FakeSqs(lookup_raises=FakeSdkError("throttled"))
        ).install(monkeypatch)

        with pytest.raises(ConfigError, match=r"queue 'jobs'.*FakeSdkError: throttled"):
            bind(tmp_path, probe, None)

    def test_the_policy_is_read_at_start_up_with_the_retrying_client_only(
        self,
        tmp_path: Path,
        probe: tuple[type[Job[None]], Probe],
        bind: Bind,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        jobs = {probe[0].__name__: {"queue": "jobs"}}
        island = FakeAwsIsland(make_client=lambda service, config: FakeSqs(_POLICY)).install(
            monkeypatch
        )
        bind(tmp_path, probe, None, jobs=jobs)

        by_attempts = island.created_by_max_attempts()
        bounded = by_attempts.pop(1)
        [retrying] = by_attempts.values()
        assert retrying.calls["get_queue_attributes"] == 1
        assert bounded.metadata_calls == 0

    @pytest.mark.parametrize(
        "policy",
        [
            "not json",
            json.dumps({"maxReceiveCount": 4}),
            json.dumps({"deadLetterTargetArn": "arn", "maxReceiveCount": 4}),
            json.dumps(
                {
                    "deadLetterTargetArn": f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:d",
                    "maxReceiveCount": "x",
                }
            ),
            json.dumps({"deadLetterTargetArn": 7, "maxReceiveCount": 4}),
            json.dumps(
                {
                    "deadLetterTargetArn": f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:d",
                    "maxReceiveCount": 0,
                }
            ),
        ],
        ids=["bad-json", "no-target", "bad-target", "bad-count", "target-not-text", "zero-count"],
    )
    def test_a_redrive_policy_loom_cannot_read_fails_start_up_naming_the_queue(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], policy: str, bind: Bind
    ) -> None:
        client = FakeSqs()
        client.get_queue_attributes = lambda **_: {  # type: ignore[method-assign]
            "Attributes": {"RedrivePolicy": policy}
        }

        with pytest.raises(SQSQueueConfigError, match=r"queue 'jobs' has a RedrivePolicy"):
            bind(tmp_path, probe, client)

    def test_a_consumed_queue_sqs_queues_does_not_map_fails_start_up(
        self, tmp_path: Path, probe: tuple[type[Job[None]], Probe], bind: Bind
    ) -> None:
        with pytest.raises(ConfigError, match=r"sqs\.queues\.jobs is missing"):
            bind(tmp_path, probe, FakeSqs(), unmapped=True)
