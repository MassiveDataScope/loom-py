"""A jobs function settles each SQS message by what its job came to, and calls SQS no more."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest

from loom.composition import AppConfig
from loom.core.config import ConfigContext
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import JobRejection, JobRetry
from loom.core.job.policy import Backoff
from loom.core.use_case import Delivery
from loom.jobs.aws.queue import DEFERRALS_ATTRIBUTE, REJECTION_REASON_ATTRIBUTE
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaHandler
from tests.helpers.composition import composition_project
from tests.helpers.extras import patched_aws_clients
from tests.helpers.sqs import ACCOUNT, FakeSqs, queue_url
from tests.unit.serverless.aws.lambda_._records import (
    WORKER,
    make_record,
    with_worker,
)

pytestmark = pytest.mark.timeout(10)

_JOBS = queue_url("jobs")
_DEAD = queue_url("jobs-dlq")
_STEADY = Backoff(first_s=4, factor=2, max_s=100, jitter=False)


class AcmeOkJob(Job[None]):
    async def execute(self, marker: str = "") -> None:
        return None


class AcmeSyncJob(Job[None]):
    async def execute(self, marker: str = "") -> None:
        return None


class AcmeWaitingJob(Job[None]):
    async def execute(self, marker: str = "") -> None:
        raise JobRetry("tenant warming up", 30)


class AcmeRejectingJob(Job[None]):
    async def execute(self, marker: str = "") -> None:
        raise JobRejection("tenant deleted")


class AcmeFailingJob(Job[None]):
    __retry__ = _STEADY

    async def execute(self, marker: str = "") -> None:
        raise ValueError("boom")


class AcmeRecordingJob(Job[None]):
    seen: ClassVar[list[tuple[int, int, str | None]]] = []

    async def execute(self, marker: str = "", delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.deferrals, delivery.task_id))


_JOB_TYPES = [
    AcmeOkJob,
    AcmeSyncJob,
    AcmeWaitingJob,
    AcmeRejectingJob,
    AcmeFailingJob,
    AcmeRecordingJob,
]
Serve = Callable[..., LambdaHandler]


@pytest.fixture
def sqs(request: pytest.FixtureRequest) -> FakeSqs:
    options: dict[str, Any] = getattr(request, "param", {})
    policy = {
        "deadLetterTargetArn": f"arn:aws:sqs:eu-west-1:{ACCOUNT}:jobs-dlq",
        "maxReceiveCount": options.get("max_receive", 4),
    }
    return FakeSqs({_JOBS: policy}, refuse=options.get("refuse", frozenset()))


@pytest.fixture
def serve(tmp_path: Path, sqs: FakeSqs) -> Iterator[Serve]:
    adapters: list[LambdaAdapter] = []

    def build(dead_letter: str = "redrive") -> LambdaHandler:
        document = with_worker(
            {
                "app": {"name": "lambda-sqs"},
                "jobs": {job.__name__: {"queue": "jobs"} for job in _JOB_TYPES},
            },
            batch_size=5,
        )
        queues = document["sqs"]["queues"]
        queues["jobs"]["dead_letter"] = dead_letter
        queues["jobs_fifo"] = {"url": f"{_JOBS}.fifo", "dead_letter": "discard"}
        project = composition_project(
            ConfigContext.from_dict(document),
            app=AppConfig(name="lambda-sqs", entry=WORKER),
            code_path=tmp_path,
            config_root=tmp_path,
        )
        adapter = LambdaAdapter(jobs=_JOB_TYPES)
        adapters.append(adapter)
        return adapter.bind(project.compose(adapter))

    with patched_aws_clients(sqs):
        yield build
        for adapter in adapters:
            adapter.close()


def _record(message_id: str, job: type[Job[Any]], **options: Any) -> dict[str, Any]:
    return make_record(message_id, "x", job_name=job.__qualname__, **options)


def _failed(result: dict[str, Any]) -> list[str]:
    return [item["itemIdentifier"] for item in result["batchItemFailures"]]


class TestSettlementThroughTheHandler:
    def test_a_mixed_batch_of_two_job_types_is_settled_message_by_message(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        records = [
            _record("ok", AcmeOkJob),
            _record("sync", AcmeSyncJob),
            _record("wait", AcmeWaitingJob),
            _record("reject", AcmeRejectingJob),
            _record("fail", AcmeFailingJob),
        ]

        result = serve()({"Records": records}, None)

        (deferred,) = sqs.entries("send_message_batch", _JOBS)
        (rejected,) = sqs.entries("send_message_batch", _DEAD)
        (visibility,) = sqs.entries("change_message_visibility_batch", _JOBS)
        assert _failed(result) == ["fail"]
        assert (
            deferred["DelaySeconds"],
            rejected["MessageAttributes"][REJECTION_REASON_ATTRIBUTE],
        ) == (
            30,
            {"DataType": "String", "StringValue": "tenant deleted"},
        )
        assert deferred["MessageAttributes"][DEFERRALS_ATTRIBUTE]["StringValue"] == "1"
        assert visibility["VisibilityTimeout"] == 4
        assert [method for method, _, _ in sqs.batches].count("send_message_batch") == 2

    def test_a_batch_that_all_succeeded_makes_no_call_to_sqs(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        handler = serve()
        bound = sqs.calls.copy()
        records = [_record(str(n), AcmeOkJob) for n in range(5)]

        result = handler({"Records": records}, None)

        assert (_failed(result), sqs.calls) == ([], bound)

    def test_a_mixed_batch_never_deletes_a_message(self, serve: Serve, sqs: FakeSqs) -> None:
        records = [
            _record("1", AcmeOkJob),
            _record("2", AcmeRejectingJob),
            _record("3", AcmeFailingJob),
        ]

        serve()({"Records": records}, None)

        assert (sqs.calls["delete_message"], sqs.calls["delete_message_batch"]) == (0, 0)

    @pytest.mark.parametrize(("body_task_id", "task_id"), [("task-9", "task-9"), (None, "m-1")])
    def test_the_job_runs_as_the_attempt_deferrals_and_task_the_record_carries(
        self, serve: Serve, body_task_id: str | None, task_id: str
    ) -> None:
        AcmeRecordingJob.seen.clear()
        body = json.dumps(
            {
                "job": AcmeRecordingJob.__qualname__,
                "params": {"marker": "x"},
                "task_id": body_task_id,
            }
        )
        record = make_record("m-1", "x", body=body, receive_count=4, deferrals=2)

        serve()({"Records": [record]}, None)

        assert AcmeRecordingJob.seen == [(4, 2, task_id)]

    @pytest.mark.parametrize(
        ("receive_count", "failed", "dead_lettered"), [(3, ["1"], 0), (4, [], 1)]
    )
    def test_the_consumer_policy_is_the_redrive_limit_of_its_queue(
        self, serve: Serve, sqs: FakeSqs, receive_count: int, failed: list[str], dead_lettered: int
    ) -> None:
        record = _record("1", AcmeFailingJob, receive_count=receive_count)

        result = serve()({"Records": [record]}, None)

        assert _failed(result) == failed
        assert len(sqs.entries("send_message_batch", _DEAD)) == dead_lettered

    @pytest.mark.parametrize("sqs", [{"max_receive": 2}], indirect=True)
    def test_a_job_that_keeps_asking_to_wait_is_never_dead_lettered(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        handler = serve()

        for deferrals in range(6):
            record = _record(f"m{deferrals}", AcmeWaitingJob, deferrals=deferrals)
            assert _failed(handler({"Records": [record]}, None)) == []

        sent = sqs.entries("send_message_batch", _JOBS)
        assert [e["MessageAttributes"][DEFERRALS_ATTRIBUTE]["StringValue"] for e in sent] == [
            str(count) for count in range(1, 7)
        ]
        assert sqs.entries("send_message_batch", _DEAD) == []
        assert sqs.calls["change_message_visibility_batch"] == 0

    @pytest.mark.parametrize("sqs", [{"refuse": frozenset({"2"})}], indirect=True)
    def test_a_failed_dead_letter_send_lists_only_that_message(self, serve: Serve) -> None:
        records = [_record(str(n), AcmeRejectingJob) for n in (1, 2, 3)]

        result = serve()({"Records": records}, None)

        assert _failed(result) == ["2"]

    def test_a_rejection_on_a_discard_queue_lists_nothing_and_sends_nothing(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        result = serve(dead_letter="discard")({"Records": [_record("1", AcmeRejectingJob)]}, None)

        assert (_failed(result), sqs.batches) == ([], [])

    def test_the_calls_to_settle_a_large_batch_carry_ten_messages_at_most(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        records = [_record(str(n), AcmeFailingJob) for n in range(25)]

        result = serve()({"Records": records}, None)

        assert len(_failed(result)) == 25
        assert [len(entries) for _, _, entries in sqs.batches] == [10, 10, 5]

    def test_a_deferral_on_a_fifo_queue_is_listed_as_a_failure(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        record = _record("1", AcmeWaitingJob, fifo=True)

        result = serve()({"Records": [record]}, None)

        assert (_failed(result), sqs.calls["send_message_batch"]) == (["1"], 0)
        assert sqs.calls["change_message_visibility_batch"] == 1

    def test_a_record_never_run_stays_listed_without_a_call_to_settle_it(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        class OutOfTime:
            def get_remaining_time_in_millis(self) -> int:
                return 10

        result = serve()(
            {"Records": [_record("1", AcmeOkJob), _record("2", AcmeOkJob)]}, OutOfTime()
        )

        assert (_failed(result), sqs.batches) == (["1", "2"], [])
