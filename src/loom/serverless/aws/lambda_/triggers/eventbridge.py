"""An EventBridge event: its ``detail`` is the payload of one job, its ``id`` the span's."""

from __future__ import annotations

from typing import Any

from loom.serverless.aws.lambda_.triggers import Decoded, an_id, an_object, upstream_trace_id


def decode(event: Any) -> tuple[Decoded, ...]:
    """The job of the EventBridge *event*.

    Raises:
        MalformedJobMessage: When the event or its ``detail`` is not an object.
    """
    event = an_object(event, "the EventBridge event")
    return (
        Decoded(
            payload=an_object(event.get("detail"), "the EventBridge event's detail"),
            event_id=an_id(event.get("id")),
            trace_id=upstream_trace_id(event),
        ),
    )
