"""Transport-neutral job wire fields and execution context."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import msgspec

from loom.core.errors import MalformedJobMessage
from loom.core.identity import Delegation, Identity, reset_identity, set_identity
from loom.core.job.job import Job
from loom.core.logger.field import ErrorSummary
from loom.core.tracing import active_trace_id

TASK_JOB_PREFIX = "loom.job"
"""Public prefix for fully qualified job names on the wire."""


def wire_job_name(qualname: str) -> str:
    """Return the transport name for a job qualname."""
    return f"{TASK_JOB_PREFIX}.{qualname}"


class JobEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Public JSON wire contract: ``job``, ``payload``, ``params``, ``trace_id``, ``identity``.

    ``job`` accepts a bare job qualname or ``loom.job.<qualname>``.
    ``task_id`` is optional.
    ``payload`` may be null; consumers treat it as an empty mapping.
    ``identity`` carries the deadline of the caller's delegation under
    ``until`` (a Unix time); the worker gives a caller without one a
    delegation from its own clock. ``task_id`` is the id of the message the
    transport assigned, for a transport that cannot read it from the message
    itself; it is absent from a message that was never sent.
    """

    job: str
    payload: dict[str, Any] | None = msgspec.field(default_factory=dict)
    params: dict[str, Any] | None = None
    trace_id: str | None = None
    identity: dict[str, Any] | None = None
    task_id: str | None = None

    @property
    def job_name(self) -> str:
        """Return the unprefixed job qualname."""
        return self.job.removeprefix(f"{TASK_JOB_PREFIX}.")


def encode_job_message(
    job_type: type[Job[Any]],
    *,
    payload: dict[str, Any] | None,
    params: dict[str, Any] | None,
    identity: Identity | Delegation | None,
    trace_id: str | None = None,
    task_id: str | None = None,
) -> bytes:
    """The JSON :class:`JobEnvelope` that runs *job_type* as *identity*.

    Args:
        job_type: The job to run.
        payload: The job's payload.
        params: The job's parameters.
        identity: The caller, or the caller with the deadline of its
            delegation (:func:`~loom.core.identity.captured_delegation`).
        trace_id: The trace the job continues.
        task_id: The id the transport gave the message, when it carries it in the body.

    Returns:
        The encoded envelope.
    """
    delegation = identity if isinstance(identity, Delegation) else Delegation(identity, None)
    job, who = wire_job_name(job_type.__qualname__), delegation.encoded()
    return msgspec.json.encode(
        JobEnvelope(
            job=job,
            payload=payload or {},
            params=params,
            trace_id=trace_id,
            identity=who,
            task_id=task_id,
        )
    )


def decode_job_envelope(body: str) -> JobEnvelope:
    """Decode a job message with msgspec validation."""
    try:
        return msgspec.json.decode(body, type=JobEnvelope)
    except (msgspec.DecodeError, msgspec.ValidationError, TypeError) as exc:
        raise MalformedJobMessage(ErrorSummary.of(exc).describe()) from exc


@contextmanager
def job_context(trace_id: str | None, identity: dict[str, Any] | None) -> Iterator[Identity | None]:
    """Publish a message's trace and caller, restoring both after execution.

    The worker acts as the caller the message carries, and as no one when it
    carries none (an unreadable identity included): never as whatever the
    worker's context held. It resumes the delegation the caller carries; a
    caller with none gets ``now + identity.max_delegation_seconds``, and an
    unreadable deadline has expired, so the job's execution is refused.
    """
    delegation = Delegation.decoded(identity)
    token = set_identity(delegation.caller, until=delegation.until)
    try:
        with active_trace_id(trace_id or None):
            yield delegation.caller
    finally:
        reset_identity(token)
