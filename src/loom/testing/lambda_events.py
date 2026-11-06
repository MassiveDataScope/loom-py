"""Build the event AWS delivers to a Lambda function, for every trigger ``source``.

:func:`lambda_event` returns the exact shape the handler receives: an API
Gateway request, an SQS batch of one loom job, an EventBridge event, an S3
notification, an SNS Lambda-subscription event or a schedule's input; with
``via="sqs"`` the event as the queue in between delivers it.

Usage::

    handler(lambda_event("eventbridge", {"invoice": 7}), None)
    handler(lambda_event("jobs", {"to": "ada@example.com"}, job=SendEmailJob), None)
"""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal
from urllib.parse import parse_qsl, quote_plus

from loom.core.identity.wire import encode_identity
from loom.core.job.envelope import encode_job_message
from loom.jobs.aws import FIFO_SUFFIX

if TYPE_CHECKING:
    from loom.core.identity import Identity
    from loom.core.job.job import Job

Source = Literal["http", "jobs", "eventbridge", "s3", "sns", "schedule"]
EventFormat = Literal["v1", "v2"]

_REGION: Final = "us-east-1"
_ACCOUNT: Final = "000000000000"
_QUEUE_ARN: Final = f"arn:aws:sqs:{_REGION}:{_ACCOUNT}:loom-jobs"
_TOPIC_ARN: Final = f"arn:aws:sns:{_REGION}:{_ACCOUNT}:loom-topic"
_TIME: Final = "2026-01-01T00:00:00Z"


def lambda_event(
    source: Source,
    payload: dict[str, Any] | None = None,
    *,
    via: Literal["sqs"] | None = None,
    **meta: Any,
) -> dict[str, Any]:
    """The event AWS delivers for *source*, carrying *payload*.

    Args:
        source: The trigger's ``source``.
        payload: What the job receives: the job's payload for ``jobs``, the
            ``detail`` of an EventBridge event, the ``Message`` of an SNS
            notification, a schedule's input; the JSON body of an HTTP
            request. An S3 notification takes ``objects=`` instead.
        via: ``"sqs"`` wraps the event as the queue in between delivers it:
            the message body an SQS record carries. Not for ``http`` or ``jobs``.
        **meta: Per source:

            * ``http``: ``method`` (``"GET"``), ``path`` (``"/"``), ``query``,
              ``headers``, ``body`` (bytes or text, instead of *payload*),
              ``event_format`` (``"v2"`` HTTP API, ``"v1"`` REST API).
            * ``jobs``: ``job`` (the class, or its name), ``params``,
              ``identity`` (the caller; anonymous when omitted), ``trace_id``.
            * ``eventbridge``: ``event_id``, ``detail_type``, ``event_source``.
            * ``s3``: ``objects`` (keys, or mappings of ``key``, ``size``,
              ``etag``, ``version_id``, ``sequencer``), ``bucket``,
              ``event_name``; ``test_event=True`` for ``s3:TestEvent``.
            * ``sns``: ``notification_id``, ``topic_arn``, ``subject``,
              ``type`` (``"Notification"``).
            * Carried by SQS (``jobs`` or ``via="sqs"``): ``message_id``,
              ``group_id`` (a FIFO queue), ``queue_arn``, and ``body``, the
              message body as written instead of the one built.

    Raises:
        TypeError: When *meta* holds a key *source* does not take, or *via*
            is given to ``http`` or ``jobs``.
    """
    if source == "http":
        if via is not None:
            raise TypeError("an http event is not delivered via sqs")
        return _http(payload, **meta)
    queued = source == "jobs" or via == "sqs"
    if source == "jobs" and via is not None:
        raise TypeError("a jobs event is already an SQS batch")
    if not queued:
        return _SOURCES[source](payload, **meta)
    record = {key: meta.pop(key) for key in ("message_id", "group_id", "queue_arn") if key in meta}
    body = meta.pop("body", None)
    if body is None:
        body = (
            _jobs_body(payload, **meta)
            if source == "jobs"
            else json.dumps(_QUEUED_BODIES.get(source, _SOURCES[source])(payload, **meta))
        )
    return {"Records": [_sqs_record(body, **record)]}


def _jobs_body(
    payload: dict[str, Any] | None,
    *,
    job: type[Job[Any]] | str,
    params: dict[str, Any] | None = None,
    identity: Identity | None = None,
    trace_id: str | None = None,
) -> str:
    """The ``JobEnvelope`` ``SQSJobService`` sends; a job named by *job*, when a string."""
    if isinstance(job, str):
        written = {"job": job, "payload": payload or {}, "params": params, "trace_id": trace_id}
        return json.dumps(written | {"identity": encode_identity(identity)})
    body = encode_job_message(
        job, payload=payload, params=params, identity=identity, trace_id=trace_id
    )
    return body.decode()


def _sqs_record(
    body: str,
    *,
    message_id: str | None = None,
    group_id: str | None = None,
    queue_arn: str | None = None,
) -> dict[str, Any]:
    attributes = {"ApproximateReceiveCount": "1", "SentTimestamp": "0"}
    if group_id is not None:
        attributes["MessageGroupId"] = group_id
    default_arn = _QUEUE_ARN + (FIFO_SUFFIX if group_id is not None else "")
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


def _eventbridge(
    payload: dict[str, Any] | None,
    *,
    event_id: str | None = None,
    detail_type: str = "loom.test",
    event_source: str = "loom.test",
) -> dict[str, Any]:
    return {
        "version": "0",
        "id": event_id or str(uuid.uuid4()),
        "detail-type": detail_type,
        "source": event_source,
        "account": _ACCOUNT,
        "time": _TIME,
        "region": _REGION,
        "resources": [],
        "detail": payload if payload is not None else {},
    }


def _s3(
    payload: dict[str, Any] | None,
    *,
    objects: Sequence[str | Mapping[str, Any]] = (),
    bucket: str = "loom-test",
    event_name: str = "ObjectCreated:Put",
    test_event: bool = False,
) -> dict[str, Any]:
    if payload is not None:
        raise TypeError("an S3 notification takes objects=, not a payload")
    if test_event:
        return {
            "Service": "Amazon S3",
            "Event": "s3:TestEvent",
            "Time": _TIME,
            "Bucket": bucket,
            "RequestId": "test",
            "HostId": "test",
        }
    return {"Records": [_s3_record(bucket, event_name, item) for item in objects]}


def _s3_record(bucket: str, event_name: str, item: str | Mapping[str, Any]) -> dict[str, Any]:
    found = {"key": item} if isinstance(item, str) else dict(item)
    written = {
        "key": quote_plus(found.pop("key")),
        "size": found.pop("size", 0),
        "eTag": found.pop("etag", "0123456789abcdef"),
        "sequencer": found.pop("sequencer", "0A1B2C3D4E5F678901"),
    }
    if "version_id" in found:
        written["versionId"] = found.pop("version_id")
    if found:
        raise TypeError(f"an S3 object takes no {', '.join(sorted(found))}")
    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": _REGION,
        "eventTime": _TIME,
        "eventName": event_name,
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "loom-test",
            "bucket": {"name": bucket, "arn": f"arn:aws:s3:::{bucket}"},
            "object": written,
        },
    }


def _sns_message(
    payload: dict[str, Any] | None,
    *,
    notification_id: str | None = None,
    topic_arn: str = _TOPIC_ARN,
    subject: str | None = None,
    type: str = "Notification",  # noqa: A002 - SNS's own field name
) -> dict[str, Any]:
    return {
        "Type": type,
        "MessageId": notification_id or str(uuid.uuid4()),
        "TopicArn": topic_arn,
        "Subject": subject,
        "Message": json.dumps(payload if payload is not None else {}),
        "Timestamp": _TIME,
        "SignatureVersion": "1",
        "Signature": "test",
        "MessageAttributes": {},
    }


def _sns(payload: dict[str, Any] | None, **meta: Any) -> dict[str, Any]:
    record = {
        "EventSource": "aws:sns",
        "EventVersion": "1.0",
        "EventSubscriptionArn": f"{meta.get('topic_arn', _TOPIC_ARN)}:subscription",
        "Sns": _sns_message(payload, **meta),
    }
    return {"Records": [record]}


def _schedule(payload: dict[str, Any] | None) -> dict[str, Any]:
    return payload if payload is not None else {}


_SOURCES: Final[Mapping[str, Callable[..., dict[str, Any]]]] = {
    "eventbridge": _eventbridge,
    "s3": _s3,
    "sns": _sns,
    "schedule": _schedule,
}
_QUEUED_BODIES: Final[Mapping[str, Callable[..., dict[str, Any]]]] = {"sns": _sns_message}


def _http(
    payload: dict[str, Any] | None,
    *,
    method: str = "GET",
    path: str = "/",
    query: str = "",
    headers: Mapping[str, str] | None = None,
    body: bytes | str | None = None,
    event_format: EventFormat = "v2",
) -> dict[str, Any]:
    written = {name.lower(): value for name, value in (headers or {}).items()}
    if payload is not None:
        if body is not None:
            raise TypeError("an http event takes a payload or a body, not both")
        body = json.dumps(payload)
        written.setdefault("content-type", "application/json")
    content = body.encode() if isinstance(body, str) else body or b""
    return _http_request_event(method, path, query, written, content, event_format)


def _http_request_event(
    method: str,
    path: str,
    query: str,
    headers: Mapping[str, str],
    body: bytes,
    event_format: EventFormat,
) -> dict[str, Any]:
    """The API Gateway event of one request; *path* decoded, as API Gateway passes it."""
    event: dict[str, Any] = {
        "headers": dict(headers),
        "body": base64.b64encode(body).decode("ascii"),
        "isBase64Encoded": True,
    }
    if event_format == "v2":
        http = {"method": method, "path": path, "protocol": "HTTP/1.1", "sourceIp": "127.0.0.1"}
        return event | {
            "version": "2.0",
            "routeKey": "$default",
            "rawPath": path,
            "rawQueryString": query,
            "requestContext": {"http": http, "requestId": "test", "stage": "$default"},
        }
    multi: dict[str, list[str]] = {}
    for name, value in parse_qsl(query, keep_blank_values=True):
        multi.setdefault(name, []).append(value)
    # A REST event has "resource" and no "version" key: that is how Mangum tells them apart.
    return event | {
        "resource": "/{proxy+}",
        "path": path,
        "httpMethod": method,
        "multiValueQueryStringParameters": multi or None,
        "requestContext": {"identity": {"sourceIp": "127.0.0.1"}, "requestId": "test"},
    }
