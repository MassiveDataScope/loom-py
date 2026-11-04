"""``sqs_record`` feeds the real Lambda SQS handler what ``SQSJobService`` sends."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, compose_application
from loom.core.config import ConfigContext
from loom.core.identity import Identity, current_identity, reset_identity, set_identity
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.job import Job
from loom.serverless.aws.lambda_ import LambdaSQSAdapter
from loom.serverless.aws.lambda_.sqs import SQSHandler
from loom.testing import sqs_raw_record, sqs_record
from tests.helpers.composition import composition_source
from tests.helpers.jobs import SQSClient, sqs_service

pytestmark = pytest.mark.timeout(30)

_SEEN: list[tuple[str, str]] = []
_ALICE = Identity(subject="alice", roles=("buyer",), mechanism="jwt")


class MarkJob(Job[None]):
    __retries__ = 0

    def execute(self, marker: str) -> None:
        _SEEN.append((marker, current_identity().subject))
        if marker == "fail":
            raise ValueError("job failed")


@pytest.fixture
def seen() -> Iterator[list[tuple[str, str]]]:
    _SEEN.clear()
    yield _SEEN
    _SEEN.clear()


@pytest.fixture
def handler(tmp_path: Path) -> Iterator[SQSHandler]:
    source = composition_source(
        ConfigContext.from_dict({"app": {"name": "sqs-events"}}),
        app=AppConfig(name="sqs-events", job_service="inline"),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    adapter = LambdaSQSAdapter(jobs=[MarkJob])
    yield adapter.bind(compose_application(source, adapter))
    adapter.close()


def _failed(result: dict[str, Any]) -> list[str]:
    return [failure["itemIdentifier"] for failure in result["batchItemFailures"]]


def _marked(marker: str, message_id: str, **options: Any) -> dict[str, Any]:
    return sqs_record(MarkJob, params={"marker": marker}, message_id=message_id, **options)


def test_a_batch_that_succeeds_retries_nothing(
    handler: SQSHandler, seen: list[tuple[str, str]]
) -> None:
    result = handler({"Records": [_marked("a", "1"), _marked("b", "2")]}, None)

    assert (_failed(result), seen) == ([], [("a", ""), ("b", "")])


@pytest.mark.parametrize(
    ("group_id", "failed", "ran"),
    [(None, ["2"], ["a", "fail", "c"]), ("customer-7", ["2", "3"], ["a", "fail"])],
    ids=["standard", "fifo"],
)
def test_a_failed_record_is_retried_and_a_fifo_group_stops_at_it(
    handler: SQSHandler,
    seen: list[tuple[str, str]],
    group_id: str | None,
    failed: list[str],
    ran: list[str],
) -> None:
    records = [_marked(m, str(i), group_id=group_id) for i, m in enumerate(["a", "fail", "c"], 1)]

    result = handler({"Records": records}, None)

    assert (_failed(result), [marker for marker, _ in seen]) == (failed, ran)


def test_the_identity_reaches_the_job(handler: SQSHandler, seen: list[tuple[str, str]]) -> None:
    handler({"Records": [_marked("a", "1", identity=_ALICE)]}, None)

    assert seen == [("a", "alice")]


def test_a_raw_body_loom_cannot_read_is_retried(
    handler: SQSHandler, seen: list[tuple[str, str]]
) -> None:
    result = handler({"Records": [sqs_raw_record("{", message_id="poison")]}, None)

    assert (_failed(result), seen) == (["poison"], [])


async def test_the_body_is_the_one_sqs_job_service_sends() -> None:
    client = SQSClient()
    service = sqs_service(client)
    token = set_identity(_ALICE)
    try:
        service.dispatch(MarkJob, params={"marker": "a"}, payload={"n": 1}, queue="emails")
        await flush_pending_dispatches()
    finally:
        reset_identity(token)
        clear_pending_dispatches()

    record = sqs_record(MarkJob, {"n": 1}, params={"marker": "a"}, identity=_ALICE)
    assert [request["MessageBody"] for request in client.requests] == [record["body"]]
