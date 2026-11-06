"""``LambdaEvent`` builds the event AWS delivers for each trigger source, queued too."""

from __future__ import annotations

import json

import pytest

from loom.testing import LambdaEvent

pytestmark = pytest.mark.timeout(5)


def test_an_s3_event_encodes_each_object_key_as_s3_does() -> None:
    event = LambdaEvent.s3(["a b/c+d.csv", {"key": "e.csv", "size": 2}])

    keys = [record["s3"]["object"]["key"] for record in event["Records"]]
    assert keys == ["a+b%2Fc%2Bd.csv", "e.csv"]


def test_an_event_via_sqs_is_one_record_whose_body_is_the_event() -> None:
    event = LambdaEvent.eventbridge({"n": 1}, event_id="e1")

    (record,) = event.via_sqs(message_id="m1", group_id="g")["Records"]

    assert (record["messageId"], json.loads(record["body"])) == ("m1", event)
    assert record["attributes"]["MessageGroupId"] == "g"
    assert record["eventSourceARN"].endswith(".fifo")


def test_an_sns_message_via_sqs_carries_the_sns_envelope_not_the_lambda_event() -> None:
    event = LambdaEvent.sns({"n": 1}, notification_id="n1")

    (record,) = event.via_sqs()["Records"]

    assert json.loads(record["body"]) == event["Records"][0]["Sns"]


def test_only_the_events_a_queue_can_carry_offer_via_sqs() -> None:
    assert hasattr(LambdaEvent.sns(), "via_sqs")
    assert not hasattr(LambdaEvent.http("POST", "/x"), "via_sqs")
    assert not hasattr(LambdaEvent.sqs_record("{}"), "via_sqs")


def test_an_http_request_takes_one_body() -> None:
    with pytest.raises(TypeError, match="json_body= or body=, not both"):
        LambdaEvent.http("POST", "/x", json_body={"a": 1}, body="{}")


def test_an_http_json_body_is_typed_unless_the_caller_typed_it() -> None:
    typed = LambdaEvent.http("POST", "/x", json_body={"a": 1}, headers={"Content-Type": "text/x"})
    untyped = LambdaEvent.http("POST", "/x", json_body={"a": 1})

    assert (typed["headers"]["content-type"], untyped["headers"]["content-type"]) == (
        "text/x",
        "application/json",
    )
