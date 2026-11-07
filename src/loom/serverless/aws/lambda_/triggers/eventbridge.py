"""An EventBridge event: its ``detail`` is the payload of one job, its ``id`` the span's.

Only an event from an AWS service names its caller: ``PutEvents`` refuses a
``source`` starting with ``aws.``, so nobody else can write one, while any
publisher can write a ``userIdentity`` into a custom event's ``detail``.
"""

from __future__ import annotations

from typing import Any

from loom.serverless.aws.lambda_.triggers import Decoded, an_id, an_object

_AWS_SOURCE = "aws."


def decode(event: Any) -> tuple[Decoded, ...]:
    """The job of the EventBridge *event*.

    Raises:
        MalformedJobMessage: When the event or its ``detail`` is not an object.
    """
    event = an_object(event, "the EventBridge event")
    detail = an_object(event.get("detail"), "the EventBridge event's detail")
    return (
        Decoded(payload=detail, event_id=an_id(event.get("id")), requester=_caller(event, detail)),
    )


def _caller(event: dict[str, Any], detail: dict[str, Any]) -> str | None:
    """The ARN AWS recorded for an AWS service event's caller; ``None`` for any other event."""
    source = event.get("source")
    if not isinstance(source, str) or not source.startswith(_AWS_SOURCE):
        return None
    recorded = detail.get("userIdentity")
    arn = recorded.get("arn") if isinstance(recorded, dict) else None
    return arn if isinstance(arn, str) and arn else None
