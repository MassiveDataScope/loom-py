"""``lambda_event`` builds the event AWS delivers for each trigger source."""

from __future__ import annotations

from typing import Any

import pytest

from loom.testing import lambda_event

pytestmark = pytest.mark.timeout(5)


def test_an_s3_event_encodes_each_object_key_as_s3_does() -> None:
    event = lambda_event("s3", objects=["a b/c+d.csv", {"key": "e.csv", "size": 2}])

    keys = [record["s3"]["object"]["key"] for record in event["Records"]]
    assert keys == ["a+b%2Fc%2Bd.csv", "e.csv"]


@pytest.mark.parametrize(
    ("payload", "meta", "unused"),
    [(None, {"job": "J"}, "job"), ({"n": 1}, {}, "payload")],
    ids=["meta", "payload"],
)
def test_a_body_written_by_hand_refuses_what_it_would_ignore(
    payload: dict[str, Any] | None, meta: dict[str, Any], unused: str
) -> None:
    with pytest.raises(TypeError, match=f"body= is sent as written; drop {unused}"):
        lambda_event("jobs", payload, body="{}", **meta)


@pytest.mark.parametrize(
    ("source", "message"),
    [("jobs", "jobs events are always via sqs"), ("http", "http events are not delivered via sqs")],
)
def test_via_sqs_is_refused_where_it_means_nothing(source: Any, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        lambda_event(source, via="sqs")
