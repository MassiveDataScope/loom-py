"""A jobs function settles each SQS message by what its job came to, and calls SQS no more."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest

from loom.composition import AppConfig
from loom.core.config import ConfigContext
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment, JobPostponement
from loom.core.job.policy import Backoff
from loom.core.use_case import Delivery
from loom.jobs.aws.queue import ABANDONMENT_REASON_ATTRIBUTE, POSTPONEMENTS_ATTRIBUTE
from loom.serverless.aws.lambda_ import LambdaAdapter, LambdaHandler
from tests.helpers.aws import ACCOUNT, OTHER_REGION
from tests.helpers.aws_island import FakeAwsIsland
from tests.helpers.composition import composition_project
from tests.helpers.sqs import FakeSqs, queue_url
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
        raise JobPostponement("tenant warming up", 30)


class AcmeAbandoningJob(Job[None]):
    async def execute(self, marker: str = "") -> None:
        raise JobAbandonment("tenant deleted")


class AcmeFailingJob(Job[None]):
    __retry__ = _STEADY

    async def execute(self, marker: str = "") -> None:
        raise ValueError("boom")


class AcmeRecordingJob(Job[None]):
    seen: ClassVar[list[tuple[int, int, str | None]]] = []

    async def execute(self, marker: str = "", delivery: JobDelivery | None = Delivery()) -> None:
        assert delivery is not None
        self.seen.append((delivery.attempt, delivery.postponements, delivery.task_id))


_JOB_TYPES = [
    AcmeOkJob,
    AcmeSyncJob,
    AcmeWaitingJob,
    AcmeAbandoningJob,
    AcmeFailingJob,
    AcmeRecordingJob,
]
Serve = Callable[..., LambdaHandler]


@pytest.fixture
def sqs(request: pytest.FixtureRequest) -> FakeSqs:
    options: dict[str, Any] = getattr(request, "param", {})
    policy = {
        "deadLetterTargetArn": f"arn:aws:sqs:{OTHER_REGION}:{ACCOUNT}:jobs-dlq",
        "maxReceiveCount": options.get("max_receive", 4),
    }
    return FakeSqs({_JOBS: policy}, refuse=options.get("refuse", frozenset()))


@pytest.fixture
def serve(tmp_path: Path, sqs: FakeSqs, monkeypatch: pytest.MonkeyPatch) -> Iterator[Serve]:
    adapters: list[LambdaAdapter] = []

    def build(dead_letter: str = "redrive", *, fifo: bool = False) -> LambdaHandler:
        document = with_worker(
            {
                "app": {"name": "lambda-sqs"},
                "jobs": {job.__name__: {"queue": "jobs"} for job in _JOB_TYPES},
            },
            batch_size=5,
        )
        queue = document["sqs"]["queues"]["jobs"]
        queue["dead_letter"] = "discard" if fifo else dead_letter
        if fifo:
            queue["url"] = f"{_JOBS}.fifo"
        project = composition_project(
            ConfigContext.from_dict(document),
            app=AppConfig(name="lambda-sqs", entry=WORKER),
            code_path=tmp_path,
            config_root=tmp_path,
        )
        adapter = LambdaAdapter(jobs=_JOB_TYPES)
        adapters.append(adapter)
        return adapter.bind(project.compose(adapter))

    FakeAwsIsland(client=sqs).install(monkeypatch)
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
            _record("abandon", AcmeAbandoningJob),
            _record("fail", AcmeFailingJob),
        ]

        result = serve()({"Records": records}, None)

        (postponed,) = sqs.entries("send_message_batch", _JOBS)
        (abandoned,) = sqs.entries("send_message_batch", _DEAD)
        (visibility,) = sqs.entries("change_message_visibility_batch", _JOBS)
        assert _failed(result) == ["fail"]
        assert (
            postponed["DelaySeconds"],
            abandoned["MessageAttributes"][ABANDONMENT_REASON_ATTRIBUTE],
        ) == (
            30,
            {"DataType": "String", "StringValue": "tenant deleted"},
        )
        assert postponed["MessageAttributes"][POSTPONEMENTS_ATTRIBUTE]["StringValue"] == "1"
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
            _record("2", AcmeAbandoningJob),
            _record("3", AcmeFailingJob),
        ]

        serve()({"Records": records}, None)

        assert (sqs.calls["delete_message"], sqs.calls["delete_message_batch"]) == (0, 0)

    @pytest.mark.parametrize(("body_task_id", "task_id"), [("task-9", "task-9"), (None, "m-1")])
    def test_the_job_runs_as_the_attempt_postponements_and_task_the_record_carries(
        self, serve: Serve, body_task_id: str | None, task_id: str
    ) -> None:
        AcmeRecordingJob.seen.clear()
        record = make_record(
            "m-1",
            "x",
            job_name=AcmeRecordingJob.__qualname__,
            task_id=body_task_id,
            receive_count=4,
            postponements=2,
        )

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

        for postponements in range(6):
            record = _record(f"m{postponements}", AcmeWaitingJob, postponements=postponements)
            assert _failed(handler({"Records": [record]}, None)) == []

        sent = sqs.entries("send_message_batch", _JOBS)
        assert [e["MessageAttributes"][POSTPONEMENTS_ATTRIBUTE]["StringValue"] for e in sent] == [
            str(count) for count in range(1, 7)
        ]
        assert sqs.entries("send_message_batch", _DEAD) == []
        assert sqs.calls["change_message_visibility_batch"] == 0

    @pytest.mark.parametrize("sqs", [{"refuse": frozenset({"2"})}], indirect=True)
    def test_a_failed_dead_letter_send_lists_only_that_message(self, serve: Serve) -> None:
        records = [_record(str(n), AcmeAbandoningJob) for n in (1, 2, 3)]

        result = serve()({"Records": records}, None)

        assert _failed(result) == ["2"]

    def test_an_abandonment_on_a_discard_queue_lists_nothing_and_sends_nothing(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        result = serve(dead_letter="discard")({"Records": [_record("1", AcmeAbandoningJob)]}, None)

        assert (_failed(result), sqs.batches) == ([], [])

    def test_the_calls_to_settle_a_large_batch_carry_ten_messages_at_most(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        records = [_record(str(n), AcmeFailingJob) for n in range(25)]

        result = serve()({"Records": records}, None)

        assert len(_failed(result)) == 25
        assert [len(entries) for _, _, entries in sqs.batches] == [10, 10, 5]

    def test_a_job_postponement_on_a_fifo_queue_is_abandoned_not_settled_as_a_failed_attempt(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        record = _record("1", AcmeWaitingJob, fifo=True)

        result = serve(fifo=True)({"Records": [record]}, None)

        assert _failed(result) == []
        assert sqs.calls["change_message_visibility_batch"] == 0
        assert sqs.calls["send_message_batch"] == 0

    def test_a_job_postponement_on_a_standard_queue_is_postponed_with_its_delay(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        result = serve()({"Records": [_record("1", AcmeWaitingJob)]}, None)

        (resent,) = sqs.entries("send_message_batch", _JOBS)
        assert (_failed(result), resent["DelaySeconds"]) == ([], 30)

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

    def test_a_malformed_message_is_abandoned_to_the_dead_letter_queue_with_its_class_as_reason(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        record = make_record("1", "x", body="{")

        result = serve()({"Records": [record]}, None)

        (abandoned,) = sqs.entries("send_message_batch", _DEAD)
        reason = abandoned["MessageAttributes"][ABANDONMENT_REASON_ATTRIBUTE]["StringValue"]
        assert (_failed(result), reason) == ([], "MalformedJobMessage")

    @pytest.mark.parametrize(
        ("receive_count", "failed", "dead_lettered"), [(3, ["1"], 0), (4, [], 1)]
    )
    def test_an_unknown_job_is_retried_up_to_the_limit_of_its_queue(
        self, serve: Serve, sqs: FakeSqs, receive_count: int, failed: list[str], dead_lettered: int
    ) -> None:
        record = make_record("1", "x", job_name="MissingJob", receive_count=receive_count)

        result = serve()({"Records": [record]}, None)

        assert _failed(result) == failed
        assert len(sqs.entries("send_message_batch", _DEAD)) == dead_lettered

    @pytest.mark.parametrize(("receive_count", "failed"), [(2, ["1"]), (3, [])])
    def test_an_unknown_job_on_a_discard_queue_is_abandoned_when_the_attempts_run_out(
        self, serve: Serve, receive_count: int, failed: list[str]
    ) -> None:
        record = make_record("1", "x", job_name="MissingJob", receive_count=receive_count)

        result = serve(dead_letter="discard")({"Records": [record]}, None)

        assert _failed(result) == failed

    def test_settling_a_batch_reads_no_queue_metadata_after_start_up(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        handler = serve()
        bound = (sqs.calls["get_queue_attributes"], sqs.calls["get_queue_url"])
        records = [_record("1", AcmeAbandoningJob), _record("2", AcmeAbandoningJob)]

        handler({"Records": records}, None)

        assert bound == (1, 1)
        assert (sqs.calls["get_queue_attributes"], sqs.calls["get_queue_url"]) == bound

    @pytest.mark.parametrize(
        ("postponements", "receive_count"),
        [("x", "1"), ("-1", "1"), (None, "x"), (None, "0")],
        ids=[
            "non-numeric postponements",
            "negative postponements",
            "non-numeric count",
            "zero count",
        ],
    )
    def test_a_record_with_a_count_it_cannot_read_alone_is_abandoned(
        self, serve: Serve, sqs: FakeSqs, postponements: str | None, receive_count: str
    ) -> None:
        bad = _record("bad", AcmeOkJob, postponements=postponements)
        bad["attributes"]["ApproximateReceiveCount"] = receive_count
        records = [_record("ok", AcmeOkJob), bad, _record("abandon", AcmeAbandoningJob)]

        result = serve()({"Records": records}, None)

        sent = sqs.entries("send_message_batch", _DEAD)
        reasons = {
            entry["Id"]: entry["MessageAttributes"][ABANDONMENT_REASON_ATTRIBUTE]["StringValue"]
            for entry in sent
        }
        assert _failed(result) == []
        assert reasons == {"bad": "MalformedJobMessage", "abandon": "tenant deleted"}

    def test_a_malformed_message_on_a_discard_queue_is_dropped_at_the_first_delivery(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        record = make_record("1", "x", body="{", receive_count=1)

        result = serve(dead_letter="discard")({"Records": [record]}, None)

        assert (_failed(result), sqs.batches) == ([], [])

    def test_settling_never_reads_queue_metadata_even_when_asked_to(
        self, serve: Serve, sqs: FakeSqs
    ) -> None:
        handler = serve()
        sqs.lookup_raises = RuntimeError("no metadata call during an invocation")
        records = [_record("1", AcmeAbandoningJob), _record("2", AcmeFailingJob)]

        result = handler({"Records": records}, None)

        assert _failed(result) == ["2"]
        assert [entry["Id"] for entry in sqs.entries("send_message_batch", _DEAD)] == ["1"]
