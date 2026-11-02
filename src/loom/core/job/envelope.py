"""Transport-neutral job wire fields and execution context."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import msgspec

from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.identity.wire import decode_identity
from loom.core.tracing import reset_trace_id, set_trace_id

TASK_JOB_PREFIX = "loom.job"
"""Public prefix for fully qualified job names on the wire."""


class JobEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Public JSON wire contract: ``job``, ``payload``, ``params``, ``trace_id``, ``identity``.

    ``job`` accepts a bare job qualname or ``loom.job.<qualname>``.
    ``payload`` may be null; consumers treat it as an empty mapping.
    """

    job: str
    payload: dict[str, Any] | None = msgspec.field(default_factory=dict)
    params: dict[str, Any] | None = None
    trace_id: str | None = None
    identity: dict[str, Any] | None = None


def decode_job_envelope(body: str) -> JobEnvelope:
    """Decode a job message with msgspec validation."""
    return msgspec.json.decode(body, type=JobEnvelope)


@contextmanager
def job_context(trace_id: str | None, identity: dict[str, Any] | None) -> Iterator[Identity | None]:
    """Publish a message's trace and caller, restoring both after execution."""
    trace_token = set_trace_id(trace_id) if trace_id else None
    caller = decode_identity(identity)
    identity_token = set_identity(caller) if caller is not None else None
    try:
        yield caller
    finally:
        if identity_token is not None:
            reset_identity(identity_token)
        if trace_token is not None:
            reset_trace_id(trace_token)
