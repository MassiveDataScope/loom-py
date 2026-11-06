"""An S3 event notification: one job per object, its payload an :class:`S3Object`.

The ``s3:TestEvent`` S3 sends when a notification is configured runs no job.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import unquote_plus

from loom.core.command import Command
from loom.core.errors import MalformedJobMessage
from loom.core.logger import get_logger
from loom.serverless.aws.lambda_.triggers import Decoded, a_list, an_object

_log = get_logger(__name__)
_TEST_EVENT = "s3:TestEvent"


class S3Object(Command, frozen=True, kw_only=True):
    """The object an S3 event notification names: the ``Input()`` of an s3 trigger's job.

    Attributes:
        bucket: The bucket's name.
        key: The object's key, decoded: S3 sends it URL-encoded. Whoever can
            write to the bucket chooses it: validate it before using it as a
            path or an identifier.
        size: Its size in bytes; ``None`` for a deletion.
        etag: Its ETag; ``None`` for a deletion.
        version_id: Its version, on a versioned bucket.
        sequencer: Orders the events of one key.
        event_name: What happened, such as ``ObjectCreated:Put``.
        event_time: When it happened.
    """

    bucket: str
    key: str
    size: int | None = None
    etag: str | None = None
    version_id: str | None = None
    sequencer: str | None = None
    event_name: str
    event_time: datetime


def decode(event: Any) -> tuple[Decoded, ...]:
    """The jobs of the S3 notification *event*, one per record; none for a test event.

    Raises:
        MalformedJobMessage: When the event holds no list of records, or a
            record no bucket name or object key.
    """
    event = an_object(event, "the S3 notification")
    if event.get("Event") == _TEST_EVENT:
        _log.info("S3 test event acknowledged; it runs no job")
        return ()
    records = a_list(event.get("Records"), "the S3 notification's Records")
    return tuple(_object(an_object(record, "an S3 record")) for record in records)


def _object(record: dict[str, Any]) -> Decoded:
    s3 = an_object(record.get("s3"), "an S3 record's s3")
    bucket = an_object(s3.get("bucket"), "an S3 record's bucket").get("name")
    found = an_object(s3.get("object"), "an S3 record's object")
    key = found.get("key")
    if not isinstance(bucket, str) or not isinstance(key, str):
        raise MalformedJobMessage("an S3 record names no bucket or object key")
    payload = {
        "bucket": bucket,
        "key": unquote_plus(key),
        "size": found.get("size"),
        "etag": found.get("eTag"),
        "version_id": found.get("versionId"),
        "sequencer": found.get("sequencer"),
        "event_name": record.get("eventName"),
        "event_time": record.get("eventTime"),
    }
    return Decoded(payload=payload)
