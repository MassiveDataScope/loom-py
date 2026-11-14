"""Transport-neutral job wire fields and execution context."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import msgspec

from loom.core.errors import MalformedJobMessage
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.identity.wire import decode_delegation, encode_identity
from loom.core.job.job import Job
from loom.core.tracing import reset_trace_id, set_trace_id

TASK_JOB_PREFIX = "loom.job"
"""Public prefix for fully qualified job names on the wire."""


def wire_job_name(qualname: str) -> str:
    """Return the transport name for a job qualname."""
    return f"{TASK_JOB_PREFIX}.{qualname}"


class JobEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Public JSON wire contract: ``job``, ``payload``, ``params``, ``trace_id``, ``identity``.

    ``job`` accepts a bare job qualname or ``loom.job.<qualname>``.
    ``payload`` may be null; consumers treat it as an empty mapping.
    ``identity`` carries the deadline of the caller's delegation under
    ``until`` (a Unix time); the worker gives a caller without one a
    delegation from its own clock.
    """

    job: str
    payload: dict[str, Any] | None = msgspec.field(default_factory=dict)
    params: dict[str, Any] | None = None
    trace_id: str | None = None
    identity: dict[str, Any] | None = None

    @property
    def job_name(self) -> str:
        """Return the unprefixed job qualname."""
        return self.job.removeprefix(f"{TASK_JOB_PREFIX}.")


def encode_job_message(
    job_type: type[Job[Any]],
    *,
    payload: dict[str, Any] | None,
    params: dict[str, Any] | None,
    identity: Identity | None,
    trace_id: str | None = None,
    until: float | None = None,
) -> bytes:
    """The JSON :class:`JobEnvelope` that runs *job_type* as *identity*, until *until*."""
    job, who = wire_job_name(job_type.__qualname__), encode_identity(identity, until)
    return msgspec.json.encode(
        JobEnvelope(job=job, payload=payload or {}, params=params, trace_id=trace_id, identity=who)
    )


def decode_job_envelope(body: str) -> JobEnvelope:
    """Decode a job message with msgspec validation."""
    try:
        return msgspec.json.decode(body, type=JobEnvelope)
    except (msgspec.DecodeError, msgspec.ValidationError, TypeError) as exc:
        raise MalformedJobMessage(str(exc)) from exc


@contextmanager
def job_context(trace_id: str | None, identity: dict[str, Any] | None) -> Iterator[Identity | None]:
    """Publish a message's trace and caller, restoring both after execution.

    The worker acts as the caller the message carries, and as no one when it
    carries none (an unreadable identity included): never as whatever the
    worker's context held. It resumes the delegation the caller carries; a
    caller with none gets ``now + identity.max_delegation_seconds``, and an
    unreadable deadline has expired, so the job's execution is refused.
    """
    caller, until = decode_delegation(identity)
    token = set_identity(caller, until=until)
    trace_token = set_trace_id(trace_id) if trace_id else None
    try:
        yield caller
    finally:
        if trace_token is not None:
            reset_trace_id(trace_token)
        reset_identity(token)
