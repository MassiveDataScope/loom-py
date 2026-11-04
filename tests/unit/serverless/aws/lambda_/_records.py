"""SQS record builders shared by Lambda adapter tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from loom.testing import sqs_raw_record

RecordFactory = Callable[..., dict[str, Any]]


def make_record(
    message_id: str,
    marker: str,
    *,
    identity: dict[str, Any] | None = None,
    trace_id: str | None = None,
    fifo: bool = False,
    job_name: str = "ProbeJob",
    body: str | None = None,
) -> dict[str, Any]:
    if body is None:
        envelope = {"job": job_name, "params": {"marker": marker}, "identity": identity}
        body = json.dumps(envelope | {"payload": {}, "trace_id": trace_id})
    return sqs_raw_record(body, message_id=message_id, group_id="group" if fifo else None)
