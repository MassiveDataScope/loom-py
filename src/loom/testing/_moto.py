"""The moto island: the one module of loom that imports ``moto``, loaded by ``LambdaWorker``.

The ``testing-aws`` extra brings moto and boto3 to it.
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from typing import Any

from botocore.awsrequest import AWSResponse  # type: ignore[import-untyped]
from moto import mock_aws

UNANSWERED = "LoomUnansweredRequest"
"""The error code of :func:`unanswered`."""

__all__ = ["UNANSWERED", "declare_queue", "start", "unanswered"]


def start() -> Any:
    """Enter moto's AWS mock, which also sets its dummy credentials until it stops.

    Moto counts nested mocks: the mock a user entered outside keeps running when this one stops.

    Returns:
        The started mock; its ``stop()`` leaves it once.
    """
    mock = mock_aws()
    mock.start()
    return mock


def declare_queue(
    sqs: Any, name: str, *, redrive: bool, max_receive_count: int
) -> tuple[str, str | None]:
    """Create the queue *name* in the mock, with its dead-letter queue when *redrive*.

    A name ending in ``.fifo`` makes a FIFO queue.

    Args:
        sqs: A boto3 ``sqs`` client served by the started mock.
        name: The queue's name in AWS.
        redrive: Whether to create ``<name>-dlq`` and attach a ``RedrivePolicy`` to the queue.
        max_receive_count: The ``maxReceiveCount`` of that policy.

    Returns:
        The queue's URL and its dead-letter queue's, or ``None`` without one.
    """
    fifo = {"FifoQueue": "true"} if name.endswith(".fifo") else {}
    dead_url: str | None = None
    attributes = dict(fifo)
    if redrive:
        stem = name.removesuffix(".fifo")
        dead_name = f"{stem}-dlq" + (".fifo" if fifo else "")
        dead_url = sqs.create_queue(QueueName=dead_name, Attributes=fifo)["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=dead_url, AttributeNames=["QueueArn"])
        policy = {
            "deadLetterTargetArn": arn["Attributes"]["QueueArn"],
            "maxReceiveCount": max_receive_count,
        }
        attributes["RedrivePolicy"] = json.dumps(policy)
    url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    return url, dead_url


class _Body(io.BytesIO):
    def stream(self, **_: Any) -> Iterator[bytes]:
        yield self.getvalue()


def unanswered(request: Any, message: str) -> Any:
    """The client error response for a request no handler answered.

    Args:
        request: The prepared request.
        message: The error message the caller reads.

    Returns:
        A 400 response of code :data:`UNANSWERED` carrying *message*.
    """
    body = json.dumps({"__type": UNANSWERED, "message": message}).encode()
    return AWSResponse(
        request.url, 400, {"Content-Type": "application/x-amz-json-1.0"}, _Body(body)
    )
