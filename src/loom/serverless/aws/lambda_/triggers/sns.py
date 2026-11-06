"""An SNS notification: its ``Message``, a JSON object, is the payload of one job.

Only a ``Notification`` runs a job. Any other message, a subscription
confirmation included, is refused as malformed, and nothing it names, its
``SubscribeURL`` included, is ever fetched.
"""

from __future__ import annotations

from typing import Any

from loom.core.errors import MalformedJobMessage
from loom.serverless.aws.lambda_.triggers import (
    Decoded,
    a_list,
    an_id,
    an_object,
    parsed,
    upstream_trace_id,
)

_NOTIFICATION = "Notification"


def decode(event: Any) -> tuple[Decoded, ...]:
    """The jobs of the SNS event a Lambda subscription delivers, one per record.

    Raises:
        MalformedJobMessage: As :func:`decode_message`, or when the event
            holds no list of records.
    """
    records = a_list(an_object(event, "the SNS event").get("Records"), "the SNS event's Records")
    return tuple(
        job
        for record in records
        for job in decode_message(an_object(record, "an SNS record").get("Sns"))
    )


def decode_message(message: Any) -> tuple[Decoded, ...]:
    """The job of one SNS *message*, as an SQS subscription without raw delivery sends it.

    Raises:
        MalformedJobMessage: When the message is not a ``Notification`` or
            its ``Message`` is not a JSON object.
    """
    message = an_object(message, "the SNS message")
    if message.get("Type") != _NOTIFICATION:
        raise MalformedJobMessage("the SNS message is not a Notification")
    body = parsed(message.get("Message"), "the SNS Message")
    return (
        Decoded(
            payload=an_object(body, "the SNS Message"),
            event_id=an_id(message.get("MessageId")),
            trace_id=upstream_trace_id(message),
        ),
    )
