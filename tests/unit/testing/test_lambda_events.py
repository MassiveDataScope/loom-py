"""``lambda_event`` builds the event AWS delivers for each trigger source."""

from __future__ import annotations

import pytest

from loom.testing import lambda_event

pytestmark = pytest.mark.timeout(5)


def test_an_s3_event_encodes_each_object_key_as_s3_does() -> None:
    event = lambda_event("s3", objects=["a b/c+d.csv", {"key": "e.csv", "size": 2}])

    keys = [record["s3"]["object"]["key"] for record in event["Records"]]
    assert keys == ["a+b%2Fc%2Bd.csv", "e.csv"]
