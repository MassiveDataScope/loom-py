"""Build the event AWS delivers to a Lambda function, for every trigger ``source``.

Usage::

    handler(lambda_event("eventbridge", {"invoice": 7}), None)
    handler(lambda_event("jobs", {"to": "ada@example.com"}, job=SendEmailJob), None)
"""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal, NamedTuple
from urllib.parse import parse_qsl, quote_plus

from loom.core.job.envelope import encode_job_message
from loom.jobs.aws import FIFO_SUFFIX

if TYPE_CHECKING:
    from loom.core.identity import Identity
    from loom.core.job.job import Job

Source = Literal["http", "jobs", "eventbridge", "s3", "sns", "schedule"]
EventFormat = Literal["v1", "v2"]

_REGION: Final = "us-east-1"
_ARN: Final = f"{_REGION}:000000000000"
_TIME: Final = "2026-01-01T00:00:00Z"
_RECORD_KEYS: Final = ("message_id", "group_id", "queue_arn")


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
        payload: What the job receives; for ``http``, the JSON body. An S3
            notification takes ``objects=`` instead.
        via: ``"sqs"`` wraps the event as one record of the queue in between.
        **meta: ``http``: ``method``, ``path``, ``query``, ``headers``,
            ``body``. ``jobs``: ``job``, ``params``, ``identity``.
            ``eventbridge``: ``event_id``. ``s3``: ``objects`` (keys, or
            mappings of ``key``, ``size``, ``version_id``), ``bucket``,
            ``test_event``. ``sns``: ``notification_id``, ``type``. Carried by
            SQS: ``message_id``, ``group_id``, ``queue_arn``, and ``body``
            written instead of the one built.

    Raises:
        TypeError: When *meta* holds a key *source* does not take.
    """
    builders = _BUILDERS[source]
    if via is None and builders.direct is not None:
        return builders.direct(payload, **meta)
    if builders.body is None:
        raise TypeError(f"{source} events are not delivered via sqs")
    if via is not None and builders.direct is None:
        raise TypeError(f"{source} events are always via sqs; drop via=")
    return _via_sqs(builders.body, payload, meta)


def _via_sqs(build: Callable[..., str], payload: Any, meta: dict[str, Any]) -> dict[str, Any]:
    """One SQS record whose body *build* writes, or ``body=`` as written.

    Raises:
        TypeError: When ``body=`` comes with a payload or any other meta.
    """
    record = {key: meta.pop(key) for key in _RECORD_KEYS if key in meta}
    body = meta.pop("body", None)
    if body is None:
        body = build(payload, **meta)
    elif meta or payload is not None:
        unused = sorted([*meta, *(["payload"] if payload is not None else [])])
        raise TypeError(f"body= is sent as written; drop {', '.join(unused)}")
    return {"Records": [_sqs_record(body, **record)]}


def _jobs_body(
    payload: dict[str, Any] | None,
    *,
    job: type[Job[Any]],
    params: dict[str, Any] | None = None,
    identity: Identity | None = None,
) -> str:
    """The ``JobEnvelope`` ``SQSJobService`` sends."""
    return encode_job_message(job, payload=payload, params=params, identity=identity).decode()


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


def _eventbridge(payload: dict[str, Any] | None, *, event_id: str | None = None) -> dict[str, Any]:
    return {
        "version": "0",
        "id": event_id or str(uuid.uuid4()),
        "detail-type": "loom.test",
        "source": "loom.test",
        "account": "000000000000",
        "time": _TIME,
        "region": _REGION,
        "resources": [],
        "detail": payload or {},
    }


def _s3(
    payload: dict[str, Any] | None,
    *,
    objects: Sequence[str | Mapping[str, Any]] = (),
    bucket: str = "loom-test",
    test_event: bool = False,
) -> dict[str, Any]:
    if payload is not None:
        raise TypeError("an S3 notification takes objects=, not a payload")
    if test_event:
        return {"Service": "Amazon S3", "Event": "s3:TestEvent", "Time": _TIME, "Bucket": bucket}
    return {"Records": [_s3_record(bucket, item) for item in objects]}


def _s3_record(bucket: str, item: str | Mapping[str, Any]) -> dict[str, Any]:
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


def _sns_message(
    payload: dict[str, Any] | None,
    *,
    notification_id: str | None = None,
    type: str = "Notification",  # noqa: A002 - SNS's own field name
) -> dict[str, Any]:
    return {
        "Type": type,
        "MessageId": notification_id or str(uuid.uuid4()),
        "TopicArn": f"arn:aws:sns:{_ARN}:loom-topic",
        "Message": json.dumps(payload or {}),
        "Timestamp": _TIME,
        "SignatureVersion": "1",
        "Signature": "test",
        "MessageAttributes": {},
    }


def _sns(payload: dict[str, Any] | None, **meta: Any) -> dict[str, Any]:
    return {"Records": [{"EventSource": "aws:sns", "Sns": _sns_message(payload, **meta)}]}


def _schedule(payload: dict[str, Any] | None) -> dict[str, Any]:
    return payload or {}


def _http(
    payload: dict[str, Any] | None,
    *,
    method: str = "GET",
    path: str = "/",
    query: str = "",
    headers: Mapping[str, str] | None = None,
    body: bytes | str | None = None,
) -> dict[str, Any]:
    written = {name.lower(): value for name, value in (headers or {}).items()}
    if payload is not None:
        body = json.dumps(payload)
        written.setdefault("content-type", "application/json")
    content = body.encode() if isinstance(body, str) else body or b""
    return _http_request_event(method, path, query, written, content, "v2")


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


def _json_body(build: Callable[..., dict[str, Any]]) -> Callable[..., str]:
    """The SQS message body of the event *build* writes: its JSON."""

    def body(payload: dict[str, Any] | None, **meta: Any) -> str:
        return json.dumps(build(payload, **meta))

    return body


class _Builders(NamedTuple):
    """How a source's event is written: delivered directly, and as an SQS message body."""

    direct: Callable[..., dict[str, Any]] | None
    body: Callable[..., str] | None


_BUILDERS: Final[Mapping[str, _Builders]] = {
    "http": _Builders(direct=_http, body=None),
    "jobs": _Builders(direct=None, body=_jobs_body),
    "eventbridge": _Builders(direct=_eventbridge, body=_json_body(_eventbridge)),
    "s3": _Builders(direct=_s3, body=_json_body(_s3)),
    "sns": _Builders(direct=_sns, body=_json_body(_sns_message)),
    "schedule": _Builders(direct=_schedule, body=_json_body(_schedule)),
}
