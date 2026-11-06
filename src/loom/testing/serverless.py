"""The event AWS delivers to a Lambda function, one typed constructor per trigger source.

Usage::

    handler(LambdaEvent.eventbridge({"invoice": 7}), None)
    handler(LambdaEvent.s3(["in/a.csv"]).via_sqs(group_id="tenant-1"), None)
    handler(LambdaEvent.jobs(SendEmailJob, {"to": "ada@example.com"}), None)
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Self
from urllib.parse import quote_plus

from loom.core.job.envelope import encode_job_message
from loom.jobs.aws import FIFO_SUFFIX
from loom.testing.gateway import EventFormat, api_gateway_event

if TYPE_CHECKING:
    from loom.core.identity import Identity
    from loom.core.job.job import Job

__all__ = ["EventFormat", "LambdaEvent"]

_REGION: Final = "us-east-1"
_ARN: Final = f"{_REGION}:000000000000"
_TIME: Final = "2026-01-01T00:00:00Z"


class LambdaEvent(dict[str, Any]):
    """The event AWS delivers for one trigger source; the handler takes it as is.

    Each constructor builds the event of a direct delivery. ``via_sqs`` turns it
    into the one SQS record the queue in between delivers: ``jobs`` events are
    already that record, and an ``http`` request is never queued.
    """

    def __init__(self, event: Mapping[str, Any], *, source: str, sqs_body: str | None) -> None:
        super().__init__(event)
        self._source = source
        self._sqs_body = sqs_body

    @classmethod
    def http(
        cls,
        method: str = "GET",
        path: str = "/",
        *,
        query: str = "",
        headers: Mapping[str, str] | None = None,
        json_body: Any = None,
        body: bytes | str | None = None,
        event_format: EventFormat = "v2",
    ) -> Self:
        """An API Gateway request; ``json_body`` is serialised and typed ``application/json``.

        Raises:
            TypeError: When both ``json_body`` and ``body`` are given.
        """
        if json_body is not None and body is not None:
            raise TypeError("an HTTP request takes json_body= or body=, not both")
        written = {name.lower(): value for name, value in (headers or {}).items()}
        if json_body is not None:
            body = json.dumps(json_body)
            written.setdefault("content-type", "application/json")
        content = body.encode() if isinstance(body, str) else body or b""
        event = api_gateway_event(method, path, query, written, content, event_format)
        return cls(event, source="http", sqs_body=None)

    @classmethod
    def jobs(
        cls,
        job: type[Job[Any]],
        payload: dict[str, Any] | None = None,
        *,
        params: dict[str, Any] | None = None,
        identity: Identity | None = None,
        message_id: str | None = None,
        group_id: str | None = None,
        queue_arn: str | None = None,
    ) -> Self:
        """One SQS record of the ``JobEnvelope`` ``SQSJobService`` sends for *job*."""
        body = encode_job_message(job, payload=payload, params=params, identity=identity).decode()
        return cls.sqs_record(body, message_id=message_id, group_id=group_id, queue_arn=queue_arn)

    @classmethod
    def sqs_record(
        cls,
        body: str,
        *,
        message_id: str | None = None,
        group_id: str | None = None,
        queue_arn: str | None = None,
    ) -> Self:
        """One SQS record whose body is *body* as written; ``group_id`` makes the queue FIFO."""
        record = _sqs_record(body, message_id=message_id, group_id=group_id, queue_arn=queue_arn)
        return cls({"Records": [record]}, source="sqs", sqs_body=None)

    @classmethod
    def eventbridge(
        cls, detail: dict[str, Any] | None = None, *, event_id: str | None = None
    ) -> Self:
        """An EventBridge event carrying *detail*."""
        event = {
            "version": "0",
            "id": event_id or str(uuid.uuid4()),
            "detail-type": "loom.test",
            "source": "loom.test",
            "account": "000000000000",
            "time": _TIME,
            "region": _REGION,
            "resources": [],
            "detail": detail or {},
        }
        return cls(event, source="eventbridge", sqs_body=json.dumps(event))

    @classmethod
    def s3(cls, objects: Sequence[str | Mapping[str, Any]], *, bucket: str = "loom-test") -> Self:
        """An S3 notification, one record per object: a key, or ``key``/``size``/``version_id``."""
        event = {"Records": [_s3_record(bucket, item) for item in objects]}
        return cls(event, source="s3", sqs_body=json.dumps(event))

    @classmethod
    def s3_test_event(cls, bucket: str = "loom-test") -> Self:
        """The ``s3:TestEvent`` S3 sends when a notification is configured."""
        event = {"Service": "Amazon S3", "Event": "s3:TestEvent", "Time": _TIME, "Bucket": bucket}
        return cls(event, source="s3", sqs_body=json.dumps(event))

    @classmethod
    def sns(
        cls,
        message: dict[str, Any] | None = None,
        *,
        notification_id: str | None = None,
        type: str = "Notification",  # noqa: A002 - SNS's own field name
    ) -> Self:
        """The event of a Lambda subscription; queued, the SNS envelope SQS stores."""
        envelope = {
            "Type": type,
            "MessageId": notification_id or str(uuid.uuid4()),
            "TopicArn": f"arn:aws:sns:{_ARN}:loom-topic",
            "Message": json.dumps(message or {}),
            "Timestamp": _TIME,
            "SignatureVersion": "1",
            "Signature": "test",
            "MessageAttributes": {},
        }
        event = {"Records": [{"EventSource": "aws:sns", "Sns": envelope}]}
        return cls(event, source="sns", sqs_body=json.dumps(envelope))

    @classmethod
    def schedule(cls, payload: dict[str, Any] | None = None) -> Self:
        """The input an EventBridge Scheduler schedule passes as is."""
        event = payload or {}
        return cls(event, source="schedule", sqs_body=json.dumps(event))

    def via_sqs(
        self,
        *,
        message_id: str | None = None,
        group_id: str | None = None,
        queue_arn: str | None = None,
    ) -> Self:
        """This event as the one SQS record the queue in between delivers.

        Raises:
            TypeError: For an ``http`` request, or an event that is already an SQS record.
        """
        if self._sqs_body is None:
            raise TypeError(f"{self._source} events are not delivered via SQS")
        return self.sqs_record(
            self._sqs_body, message_id=message_id, group_id=group_id, queue_arn=queue_arn
        )


def _sqs_record(
    body: str,
    *,
    message_id: str | None,
    group_id: str | None,
    queue_arn: str | None,
) -> dict[str, Any]:
    """One record of an SQS batch, of a FIFO queue when *group_id* is given."""
    attributes = {"ApproximateReceiveCount": "1", "SentTimestamp": "0"}
    if group_id is not None:
        attributes["MessageGroupId"] = group_id
    default_arn = f"arn:aws:sqs:{_ARN}:loom-jobs" + (FIFO_SUFFIX if group_id is not None else "")
    return {
        "messageId": message_id or str(uuid.uuid4()),
        "receiptHandle": "test-receipt",
        "body": body,
        "attributes": attributes,
        "messageAttributes": {},
        "eventSource": "aws:sqs",
        "eventSourceARN": queue_arn or default_arn,
        "awsRegion": _REGION,
    }


def _s3_record(bucket: str, item: str | Mapping[str, Any]) -> dict[str, Any]:
    """One ``ObjectCreated:Put`` record; the key URL-encoded, as S3 writes it."""
    found = {"key": item} if isinstance(item, str) else dict(item)
    written = {"key": quote_plus(found.pop("key")), "size": found.pop("size", 0)}
    if "version_id" in found:
        written["versionId"] = found.pop("version_id")
    if found:
        raise TypeError(f"an S3 object takes no {', '.join(sorted(found))}")
    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": _REGION,
        "eventTime": _TIME,
        "eventName": "ObjectCreated:Put",
        "s3": {
            "s3SchemaVersion": "1.0",
            "bucket": {"name": bucket, "arn": f"arn:aws:s3:::{bucket}"},
            "object": written | {"eTag": "0123456789abcdef", "sequencer": "0A1B2C3D4E5F6789"},
        },
    }
