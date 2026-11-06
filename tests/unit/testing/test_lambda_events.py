"""``lambda_event`` builds the event AWS delivers for each trigger source."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from loom.testing import lambda_event

pytestmark = pytest.mark.timeout(5)


def _body(event: dict[str, Any]) -> Any:
    (record,) = event["Records"]
    assert record["eventSource"] == "aws:sqs"
    return json.loads(record["body"])


def test_an_http_event_carries_the_payload_as_its_json_body() -> None:
    event = lambda_event("http", {"name": "ada"}, method="POST", path="/users", query="a=1")

    assert (event["version"], event["rawPath"], event["rawQueryString"]) == ("2.0", "/users", "a=1")
    assert event["requestContext"]["http"]["method"] == "POST"
    assert event["headers"]["content-type"] == "application/json"
    assert json.loads(base64.b64decode(event["body"])) == {"name": "ada"}


def test_a_rest_api_event_has_a_resource_and_no_version() -> None:
    event = lambda_event("http", path="/users", event_format="v1")

    assert "version" not in event
    assert (event["resource"], event["httpMethod"]) == ("/{proxy+}", "GET")


def test_a_jobs_event_is_one_sqs_record_of_a_job_envelope() -> None:
    event = lambda_event("jobs", {"to": "ada"}, job="SendEmail", message_id="m1", group_id="g")

    (record,) = event["Records"]
    assert (record["messageId"], record["attributes"]["MessageGroupId"]) == ("m1", "g")
    assert record["eventSourceARN"].endswith(".fifo")
    assert _body(event) == {
        "job": "SendEmail",
        "payload": {"to": "ada"},
        "params": None,
        "trace_id": None,
        "identity": None,
    }


def test_an_eventbridge_event_carries_the_payload_as_its_detail() -> None:
    event = lambda_event("eventbridge", {"n": 1}, event_id="e1", detail_type="Invoice")

    assert (event["id"], event["detail-type"], event["detail"]) == ("e1", "Invoice", {"n": 1})


def test_an_sns_event_carries_the_payload_as_its_message() -> None:
    (record,) = lambda_event("sns", {"n": 1}, notification_id="n1")["Records"]

    assert record["EventSource"] == "aws:sns"
    assert (record["Sns"]["Type"], record["Sns"]["MessageId"]) == ("Notification", "n1")
    assert json.loads(record["Sns"]["Message"]) == {"n": 1}


def test_an_s3_event_encodes_each_object_key_as_s3_does() -> None:
    event = lambda_event("s3", objects=["a b/c+d.csv", {"key": "e.csv", "size": 2}])

    keys = [record["s3"]["object"]["key"] for record in event["Records"]]
    assert keys == ["a+b%2Fc%2Bd.csv", "e.csv"]


def test_a_schedule_event_is_its_input() -> None:
    assert lambda_event("schedule", {"n": 1}) == {"n": 1}


@pytest.mark.parametrize(
    ("source", "payload", "carried"),
    [
        ("eventbridge", {"n": 1}, "detail"),
        ("schedule", {"n": 1}, None),
        ("s3", None, "Records"),
        ("sns", {"n": 1}, "Message"),
    ],
)
def test_via_sqs_the_event_is_the_body_of_one_sqs_record(
    source: Any, payload: dict[str, Any] | None, carried: str | None
) -> None:
    options = {"objects": ["k"]} if source == "s3" else {}

    body = _body(lambda_event(source, payload, via="sqs", message_id="m1", **options))

    assert body == payload if carried is None else carried in body


@pytest.mark.parametrize(
    ("source", "options"),
    [
        ("http", {"via": "sqs"}),
        ("jobs", {"via": "sqs", "job": "J"}),
        ("eventbridge", {"objects": ["k"]}),
        ("s3", {"objects": [{"key": "k", "colour": "red"}]}),
    ],
    ids=["http-via-sqs", "jobs-via-sqs", "key-of-another-source", "unknown-object-key"],
)
def test_a_key_the_source_does_not_take_is_refused(source: Any, options: dict[str, Any]) -> None:
    with pytest.raises(TypeError):
        lambda_event(source, **options)
