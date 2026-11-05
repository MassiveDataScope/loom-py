"""``lambda.functions``: an sqs function's event source mapping settings."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.serverless.aws.lambda_.functions import LambdaFunction, declared_lambda

pytestmark = pytest.mark.timeout(5)


def _declared(worker: dict[str, Any]) -> LambdaFunction:
    config = ConfigContext.from_dict({"lambda": {"functions": {"worker": worker}}})
    return declared_lambda(config).functions["worker"]


def test_an_sqs_function_declares_its_event_source_mapping() -> None:
    knobs = {
        "queue": "orders",
        "batch_size": 10000,
        "batching_window_s": 300,
        "max_concurrency": 1000,
        "report_batch_item_failures": False,
    }
    function = _declared({"sqs": True, **knobs})

    assert {key: getattr(function, key) for key in knobs} == knobs


@pytest.mark.parametrize(
    ("knob", "value"),
    [
        ("batch_size", 0),
        ("batch_size", 10001),
        ("batching_window_s", -1),
        ("batching_window_s", 301),
        ("max_concurrency", 1),
        ("max_concurrency", 1001),
        ("report_batch_item_failures", "no"),
    ],
)
def test_an_out_of_range_setting_is_refused(knob: str, value: object) -> None:
    worker = {"sqs": True, "batching_window_s": 1, knob: value}
    with pytest.raises(ConfigError, match=knob):
        _declared(worker)


@pytest.mark.parametrize(
    ("knob", "value"),
    [
        ("queue", "orders"),
        ("batch_size", 2),
        ("batching_window_s", 0),
        ("max_concurrency", 2),
        ("report_batch_item_failures", True),
    ],
)
def test_an_http_function_takes_no_queue_or_batching_setting(knob: str, value: object) -> None:
    worker = {"http": ["ItemInterface"], knob: value}
    with pytest.raises(ConfigError, match=f"worker.{knob} applies to an sqs function only"):
        _declared(worker)


@pytest.mark.parametrize(("window", "refused"), [(None, True), (0, True), (1, False)])
def test_a_batch_over_ten_needs_a_batching_window(window: int | None, refused: bool) -> None:
    worker = {"sqs": True, "batch_size": 11, "batching_window_s": window}
    if refused:
        with pytest.raises(ConfigError, match="needs batching_window_s >= 1"):
            _declared(worker)
    else:
        assert _declared(worker).batch_size == 11
