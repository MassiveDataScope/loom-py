"""SQS record builders and the jobs function shared by Lambda adapter tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import msgspec

from loom.core.job.envelope import JobEnvelope
from loom.testing import LambdaEvent, make

RecordFactory = Callable[..., dict[str, Any]]
WORKER = "worker"
JOBS_ARN = "arn:aws:sqs:eu-west-1:123456789012:jobs"
"""The queue :func:`with_worker` consumes, as the event source of its records."""


def consumed_queues(*names: str) -> dict[str, Any]:
    """The ``aws`` and ``sqs`` sections mapping each of *names* to a queue that discards."""
    queues = {
        name: {
            "url": f"https://sqs.eu-west-1.amazonaws.com/123456789012/{name}",
            "abandoned": "discard",
        }
        for name in names
    }
    return {"aws": {"region": "eu-west-1"}, "sqs": {"queues": queues}}


def with_worker(document: dict[str, Any], **mapping: Any) -> dict[str, Any]:
    """*document* declaring the jobs function :data:`WORKER`, its ``queue`` block *mapping*.

    The queue of the block is mapped by ``sqs.queues``, as a consumer needs.
    """
    trigger = {"source": "jobs", "queue": {"name": "jobs", **mapping}}
    worker = {"trigger": trigger}
    return consumed_queues("jobs") | document | {"lambda": {"functions": {WORKER: worker}}}


def make_record(
    message_id: str,
    marker: str,
    *,
    identity: dict[str, Any] | None = None,
    trace_id: str | None = None,
    fifo: bool = False,
    job_name: str = "ProbeJob",
    body: str | None = None,
    task_id: str | None = None,
    receive_count: int = 1,
    postponements: int | str | None = None,
) -> dict[str, Any]:
    if body is None:
        envelope = make(
            JobEnvelope,
            job=job_name,
            params={"marker": marker},
            payload={},
            identity=identity,
            trace_id=trace_id,
            task_id=task_id,
        )
        body = msgspec.json.encode(envelope).decode()
    arn = JOBS_ARN + (".fifo" if fifo else "")
    event = LambdaEvent.sqs_record(
        body, message_id=message_id, group_id="group" if fifo else None, queue_arn=arn
    )
    record: dict[str, Any] = event["Records"][0]
    record["attributes"]["ApproximateReceiveCount"] = str(receive_count)
    if postponements is not None:
        record["messageAttributes"]["loom.postponements"] = {
            "dataType": "Number",
            "stringValue": str(postponements),
        }
    return record
