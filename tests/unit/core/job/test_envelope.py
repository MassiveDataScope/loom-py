"""Job wire contract and shared failure metadata."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.errors import MalformedJobMessage, UnknownJob
from loom.core.job.envelope import (
    JobEnvelope,
    decode_job_envelope,
    encode_job_message,
    wire_job_name,
)
from loom.core.job.job import Job
from loom.testing import make


class _AnyJob(Job[None]):
    def execute(self) -> None:
        return None


class TestJobEnvelope:
    @pytest.mark.parametrize("job", ["Task", "loom.job.Task"])
    def test_job_name_strips_only_wire_prefix(self, job: str) -> None:
        assert make(JobEnvelope, job=job).job_name == "Task"

    def test_wire_job_name(self) -> None:
        assert wire_job_name("Task") == "loom.job.Task"

    @pytest.mark.parametrize("body", ["{", "null", "[]", '{"job": 3}'])
    def test_decoder_raises_typed_error(self, body: str) -> None:
        with pytest.raises(MalformedJobMessage):
            decode_job_envelope(body)

    @pytest.mark.parametrize(
        ("error", "is_retryable"),
        [(MalformedJobMessage("{"), False), (UnknownJob("missing"), True)],
    )
    def test_a_malformed_message_is_abandoned_and_an_unknown_job_retried(
        self, error: Exception, is_retryable: bool
    ) -> None:
        assert error.is_retryable is is_retryable  # type: ignore[attr-defined]


class TestJobEnvelopeTaskId:
    def test_a_message_without_a_task_id_decodes_to_none(self) -> None:
        wire = msgspec.to_builtins(make(JobEnvelope, job="Task"))
        del wire["task_id"]

        assert decode_job_envelope(msgspec.json.encode(wire).decode()).task_id is None

    def test_a_task_id_survives_the_wire(self) -> None:
        body = encode_job_message(
            _AnyJob, payload=None, params=None, identity=None, task_id="task-7"
        )
        assert decode_job_envelope(body.decode()).task_id == "task-7"
