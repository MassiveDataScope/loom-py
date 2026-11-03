"""SQS record builders shared by Lambda adapter tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

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
    return {
        "messageId": message_id,
        "body": body
        if body is not None
        else json.dumps(
            {
                "job": job_name,
                "payload": {},
                "params": {"marker": marker},
                "identity": identity,
                "trace_id": trace_id,
            }
        ),
        "eventSourceARN": "arn:aws:sqs:us-east-1:123:queue.fifo" if fifo else "queue",
    }
