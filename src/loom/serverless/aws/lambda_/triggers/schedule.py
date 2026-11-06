"""An EventBridge Scheduler invocation: the schedule's input is the payload of one job."""

from __future__ import annotations

from typing import Any

from loom.serverless.aws.lambda_.triggers import Decoded, an_object, upstream_trace_id


def decode(event: Any) -> tuple[Decoded, ...]:
    """The job of the schedule's input *event*, the event as delivered.

    Raises:
        MalformedJobMessage: When the input is not an object.
    """
    payload = an_object(event, "the schedule's input")
    return (Decoded(payload=payload, trace_id=upstream_trace_id(payload)),)
