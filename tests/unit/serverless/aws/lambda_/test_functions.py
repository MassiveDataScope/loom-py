"""``lambda.functions``: each function's ``trigger``, one source's keys only."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.core.use_case import Input
from loom.serverless.aws.lambda_ import S3Object
from loom.serverless.aws.lambda_.functions import (
    EventTrigger,
    LambdaFunction,
    declared_lambda,
    trigger_job,
)

pytestmark = pytest.mark.timeout(5)


def _declared(trigger: dict[str, Any]) -> LambdaFunction:
    config = ConfigContext.from_dict({"lambda": {"functions": {"worker": {"trigger": trigger}}}})
    return declared_lambda(config).functions["worker"]


def _jobs(**knobs: Any) -> dict[str, Any]:
    return {"source": "jobs", **knobs}


def test_a_jobs_function_declares_its_event_source_mapping() -> None:
    knobs = {
        "queue": "orders",
        "batch_size": 10000,
        "batching_window_s": 300,
        "max_concurrency": 1000,
        "report_batch_item_failures": False,
    }
    function = _declared(_jobs(**knobs))

    assert {key: getattr(function.trigger, key) for key in knobs} == knobs


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
    with pytest.raises(ConfigError, match=knob):
        _declared(_jobs(**{"batching_window_s": 1, knob: value}))


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
    trigger = {"source": "http", "interfaces": ["ItemInterface"], knob: value}
    with pytest.raises(ConfigError, match=f"unknown field `{knob}`"):
        _declared(trigger)


@pytest.mark.parametrize(("window", "refused"), [(None, True), (0, True), (1, False)])
def test_a_batch_over_ten_needs_a_batching_window(window: int | None, refused: bool) -> None:
    trigger = _jobs(batch_size=11, batching_window_s=window)
    if refused:
        with pytest.raises(ConfigError, match="needs batching_window_s >= 1"):
            _declared(trigger)
    else:
        assert _declared(trigger).trigger.batch_size == 11  # type: ignore[union-attr]


@pytest.mark.parametrize("source", ["eventbridge", "s3", "sns", "schedule"])
def test_an_event_trigger_declares_its_job_and_identity(source: str) -> None:
    identity = {"subject": "svc:invoices", "roles": ["importer"], "attributes": {"tenant": "acme"}}

    trigger = _declared({"source": source, "job": "ImportInvoice", "identity": identity}).trigger

    assert isinstance(trigger, EventTrigger)
    assert (trigger.job, trigger.via) == ("ImportInvoice", None)
    assert trigger.identity is not None
    assert (trigger.identity.subject, trigger.identity.roles) == ("svc:invoices", ("importer",))
    assert trigger.identity.attributes == {"tenant": "acme"}


def test_the_form_before_trigger_is_refused_naming_trigger() -> None:
    config = ConfigContext.from_dict({"lambda": {"functions": {"worker": {"sqs": True}}}})

    with pytest.raises(ConfigError, match=r"worker\.sqs: .* under 'trigger'"):
        declared_lambda(config)


def test_two_functions_consuming_one_queue_are_refused() -> None:
    functions = {
        "worker": {"trigger": _jobs(queue="jobs")},
        "alerts": {"trigger": {"source": "sns", "via": "sqs", "queue": "jobs", "job": "J"}},
    }
    config = ConfigContext.from_dict({"lambda": {"functions": functions}})

    with pytest.raises(ConfigError, match=r"queue 'jobs' is served by both .*worker and .*alerts"):
        declared_lambda(config)


@pytest.mark.parametrize(
    ("trigger", "message"),
    [
        pytest.param(
            {"source": "eventbridge", "job": "J", "interfaces": ["X"]},
            "unknown field `interfaces`",
            id="http-key-on-eventbridge",
        ),
        pytest.param({"source": "jobs", "job": "J"}, "unknown field `job`", id="event-key-on-jobs"),
        pytest.param(
            {"source": "s3", "job": "J", "queue": "jobs"},
            r"trigger\.queue applies to a trigger via sqs only",
            id="queue-on-direct",
        ),
        pytest.param(
            {"source": "sns", "job": "J", "via": "sqs", "maximum_retry_attempts": 1},
            r"trigger\.maximum_retry_attempts applies to a direct trigger only",
            id="retries-via-sqs",
        ),
        pytest.param(
            {"source": "schedule", "job": "J", "maximum_retry_attempts": 3},
            "maximum_retry_attempts",
            id="retries-over-two",
        ),
        pytest.param({"source": "kinesis", "job": "J"}, "kinesis", id="unknown-source"),
        pytest.param({"source": "eventbridge"}, "missing required field `job`", id="no-job"),
        pytest.param(
            {"source": "eventbridge", "job": "J", "identity": {"roles": ["admin"]}},
            "missing required field `subject`",
            id="identity-without-subject",
        ),
    ],
)
def test_a_trigger_accepts_only_its_own_keys(trigger: dict[str, Any], message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        _declared(trigger)


@pytest.mark.parametrize(
    "trigger",
    [
        {"job": "${oc.env:JOB}"},
        {"job": "J", "identity": {"subject": "${ssm:/subject}"}},
        {"job": "J", "identity": {"subject": "s", "roles": ["${oc.env:ROLE}"]}},
        {"job": "J", "identity": {"subject": "s", "attributes": {"tenant": "${oc.env:T}"}}},
    ],
    ids=["job", "subject", "role", "attribute"],
)
def test_a_trigger_job_and_identity_are_literal(trigger: dict[str, Any]) -> None:
    with pytest.raises(ConfigError, match="written as values, not placeholders"):
        _declared({"source": "eventbridge", **trigger})


class _Arrived(S3Object, frozen=True, kw_only=True):
    """An s3 job's input, extending the object loom decodes."""


class _Import(Job[None]):
    async def execute(self, found: _Arrived = Input(), attempt: int = 1) -> None:
        return None


class _NoInput(Job[None]):
    async def execute(self, attempt: int = 1) -> None:
        return None


class _Required(Job[None]):
    async def execute(self, tenant: str, found: _Arrived = Input()) -> None:
        return None


def test_a_trigger_job_with_an_input_and_no_required_parameter_is_accepted() -> None:
    assert trigger_job("worker", EventTrigger(job="_Import"), [_NoInput, _Import]) is _Import


@pytest.mark.parametrize(
    ("job", "jobs", "message"),
    [
        ("_Missing", [_Import], "'_Missing', which is no discovered Job"),
        ("_NoInput", [_NoInput], "_NoInput.execute declares no Input"),
        ("_Required", [_Required], "_Required.execute requires tenant"),
    ],
    ids=["undiscovered", "no-input", "required-parameter"],
)
def test_a_trigger_job_an_event_cannot_run_is_refused(
    job: str, jobs: list[type[Job[Any]]], message: str
) -> None:
    with pytest.raises(ConfigError, match=message):
        trigger_job("worker", EventTrigger(job=job), jobs)
