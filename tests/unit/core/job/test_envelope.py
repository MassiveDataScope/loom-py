"""Job wire contract and shared failure metadata."""

from __future__ import annotations

import pytest

from loom.core.engine.post_commit import PostCommitError
from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.job.envelope import JobEnvelope, decode_job_envelope, wire_job_name
from loom.core.job.outcome import failure_log_fields


@pytest.mark.parametrize("job", ["Task", "loom.job.Task"])
def test_job_name_strips_only_wire_prefix(job: str) -> None:
    assert JobEnvelope(job=job).job_name == "Task"


def test_wire_job_name() -> None:
    assert wire_job_name("Task") == "loom.job.Task"


@pytest.mark.parametrize("body", ["{", "null", "[]", '{"job": 3}'])
def test_decoder_raises_typed_error(body: str) -> None:
    with pytest.raises(MalformedJobMessage):
        decode_job_envelope(body)


@pytest.mark.parametrize("error", [MalformedJobMessage("{"), UnknownJob("missing")])
def test_poison_errors_are_retryable(error: Exception) -> None:
    assert error.retryable is True  # type: ignore[attr-defined]


def test_post_commit_failure_log_fields() -> None:
    error = PostCommitError(committed=True, failures=(ValueError("failed"),))
    assert failure_log_fields(error) == {"committed": True, "failures": 1}


def test_ordinary_failure_log_fields() -> None:
    assert failure_log_fields(ValueError("failed")) == {}
