"""Acme shipping on the Lambda ``jobs`` function, and both Lambda functions kept warm (H1).

The ``jobs`` function runs ``TrackShipmentJob`` from an SQS record carrying
the enqueuer's identity. A job refused for its caller is not retryable,
so the function discards its record (the queue says ``dead_letter: discard``)
and logs it as failed finally: that log is where the refusal surfaces. An
envelope that does not decode is rejected the same way.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.identity import Identity, current_caller
from loom.serverless.aws.lambda_ import LambdaHandler
from loom.testing import LambdaEvent, LambdaTestClient
from tests.integration.hexagonal.conftest import (
    ANA,
    BAD_IDENTITIES,
    BO,
    CALLERS,
    PARAMS,
    Compose,
    Seen,
    acted_as,
    job_body,
    key_of,
    report_for,
)
from tests.integration.hexagonal.shipping import TrackShipmentJob

CONSUMED = {"batchItemFailures": []}


@pytest.fixture
def lambda_worker(compose: Compose, no_aws: None) -> Iterator[LambdaHandler]:
    """The ``acme-worker`` Lambda function, closed when the test ends, over a stand-in SQS."""
    yield compose("lambda-jobs")[1]


def _job(caller: Identity | None) -> LambdaEvent:
    return LambdaEvent.jobs(TrackShipmentJob, params=PARAMS, identity=caller)


def _failures(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str]]:
    return [
        (str(record.msg["event"]), str(record.msg["error_class"]))
        for record in caplog.records
        if isinstance(record.msg, dict) and "error_class" in record.msg
    ]


def _reported(event: LambdaEvent) -> dict[str, list[dict[str, str]]]:
    return {"batchItemFailures": [{"itemIdentifier": event["Records"][0]["messageId"]}]}


FAILED_FINALLY = "SQS job failed finally"


class TestTheJobsFunction:
    @CALLERS
    def test_runs_the_job_through_adapters_acting_as_the_enqueuer(
        self, lambda_worker: LambdaHandler, seen: Seen, caller: Identity
    ) -> None:
        assert lambda_worker(_job(caller), None) == CONSUMED
        assert seen.calls == acted_as(caller)

    def test_a_record_without_a_caller_fails_finally_before_reaching_an_adapter(
        self, lambda_worker: LambdaHandler, seen: Seen, caplog: pytest.LogCaptureFixture
    ) -> None:
        assert lambda_worker(_job(None), None) == CONSUMED
        assert _failures(caplog) == [(FAILED_FINALLY, "Unauthenticated")]
        assert seen.calls == []

    @pytest.mark.parametrize(
        ("bad", "retried", "failure"),
        [
            ("empty-subject", False, (FAILED_FINALLY, "Unauthenticated")),
            ("not-a-mapping", False, (FAILED_FINALLY, "MalformedJobMessage")),
            ("subject-not-a-string", False, (FAILED_FINALLY, "Unauthenticated")),
            ("expired", False, (FAILED_FINALLY, "DelegationExpired")),
            ("unreadable-until", False, (FAILED_FINALLY, "DelegationExpired")),
        ],
    )
    def test_a_record_naming_no_valid_caller_fails_before_reaching_an_adapter(
        self,
        lambda_worker: LambdaHandler,
        seen: Seen,
        caplog: pytest.LogCaptureFixture,
        bad: str,
        retried: bool,
        failure: tuple[str, str],
    ) -> None:
        event = LambdaEvent.sqs_record(job_body(BAD_IDENTITIES[bad]))

        assert lambda_worker(event, None) == (_reported(event) if retried else CONSUMED)
        assert _failures(caplog) == [failure]
        assert seen.calls == []


class TestWarmInvocations:
    def test_the_http_function_keeps_no_caller_between_invocations(
        self, compose: Compose, seen: Seen
    ) -> None:
        with LambdaTestClient(compose("lambda-http")[1]) as client:
            ana = client.get("/shipments/ACME-1", headers=key_of(ANA))
            nobody = client.get("/shipments/ACME-1")
            bo = client.get("/shipments/ACME-1", headers=key_of(BO))

        assert (ana.json(), nobody.status_code, bo.json()) == (report_for(ANA), 401, report_for(BO))
        assert seen.calls == acted_as(ANA, BO)
        assert current_caller() is None

    def test_the_jobs_function_keeps_no_caller_between_invocations(
        self, lambda_worker: LambdaHandler, seen: Seen, caplog: pytest.LogCaptureFixture
    ) -> None:
        replies = [lambda_worker(_job(caller), None) for caller in (ANA, None, BO)]

        assert replies == [CONSUMED] * 3
        assert _failures(caplog) == [(FAILED_FINALLY, "Unauthenticated")]
        assert seen.calls == acted_as(ANA, BO)
        assert current_caller() is None
