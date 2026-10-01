"""BSON-to-Loom normalization helpers for MongoDB CDC."""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, cast

import msgspec

from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.mongo._event import (
    MongoBsonTimestamp,
    MongoCDCEvent,
    MongoCDCNamespace,
    MongoDBRef,
    MongoObjectId,
)

_MONGO_MESSAGE_TYPE = "loom.mongo.cdc"
_MAX_BSON_DEPTH = 64
_JSON_SCALARS: frozenset[type] = frozenset({bool, int, float, str})


class _SupportsBytes(Protocol):
    def __bytes__(self) -> bytes:
        """Return a bytes representation."""
        ...


def normalize_bson_value(value: object, _depth: int = 0) -> object:
    """Normalize one MongoDB/BSON runtime value into Loom-safe builtins."""
    if _depth > _MAX_BSON_DEPTH:
        raise ValueError(f"BSON document exceeds maximum nesting depth of {_MAX_BSON_DEPTH}.")
    # Exact type, not isinstance: bson.Int64 subclasses int and msgspec refuses to encode
    # builtin subclasses, so it must reach its normalizer below.
    if value is None or type(value) in _JSON_SCALARS:
        return value
    if isinstance(value, datetime):
        return _datetime_to_epoch_ms(value)
    if isinstance(value, Mapping):
        return {str(key): normalize_bson_value(item, _depth + 1) for key, item in value.items()}
    if isinstance(value, tuple):
        return [normalize_bson_value(item, _depth + 1) for item in value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, memoryview, str)):
        return [normalize_bson_value(item, _depth + 1) for item in value]

    normalizer = _BSON_NORMALIZERS.get(type(value).__name__)
    return value if normalizer is None else normalizer(value)


def build_mongo_cdc_event(change: Mapping[str, object]) -> MongoCDCEvent:
    """Build a Loom-safe Mongo CDC event from one raw change-stream document."""
    resume_token = _normalize_resume_token(change.get("_id"))
    event_id = _resume_token_event_id(resume_token)
    operation_type = _required_str(change.get("operationType"), "operationType")
    namespace = _build_namespace(change.get("ns"))
    cluster_time = _build_cluster_time(change.get("clusterTime"))
    wall_time_ms = _build_wall_time_ms(change.get("wallTime"), cluster_time)
    lag_ms = _event_lag_ms(wall_time_ms, cluster_time)
    raw_payload = normalize_bson_value(change)
    if not isinstance(raw_payload, dict):
        raise TypeError("Normalized Mongo change event must be a mapping.")
    full_document_raw = raw_payload.get("fullDocument")
    full_document = full_document_raw if isinstance(full_document_raw, dict) else None
    update_description_raw = raw_payload.get("updateDescription")
    update_description = (
        update_description_raw if isinstance(update_description_raw, dict) else None
    )

    return MongoCDCEvent(
        event_id=event_id,
        operation_type=operation_type,
        namespace=namespace,
        resume_token=resume_token,
        document_id=_document_id(change.get("documentKey")),
        cluster_time=cluster_time,
        wall_time_ms=wall_time_ms,
        lag_ms=lag_ms,
        full_document=full_document,
        update_description=update_description,
        raw_json=msgspec.json.encode(raw_payload).decode("utf-8"),
    )


def _message_key(document_id: MongoObjectId | str | None) -> str | None:
    """Extract a partition key string from a normalized document identifier."""
    return document_id.id if isinstance(document_id, MongoObjectId) else document_id


def build_mongo_cdc_message(change: Mapping[str, object]) -> Message[MongoCDCEvent]:
    """Build a transport-neutral Loom message for one Mongo change event."""
    event = build_mongo_cdc_event(change)
    if event.wall_time_ms is not None:
        produced_at_ms: int | None = event.wall_time_ms
    elif event.cluster_time is not None:
        produced_at_ms = event.cluster_time.seconds * 1000
    else:
        produced_at_ms = None
    meta = MessageMeta(
        message_id=event.event_id,
        trace_id=event.event_id,
        produced_at_ms=produced_at_ms,
        message_type=_MONGO_MESSAGE_TYPE,
        key=_message_key(event.document_id),
    )
    return Message(payload=event, meta=meta)


def _normalize_resume_token(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("Mongo change event '_id' must be a mapping resume token.")
    normalized = normalize_bson_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("Normalized Mongo resume token must remain a mapping.")
    return normalized


def _resume_token_event_id(resume_token: Mapping[str, object]) -> str:
    token_data = resume_token.get("_data")
    if isinstance(token_data, str) and token_data:
        return token_data
    return msgspec.json.encode(dict(resume_token)).decode("utf-8")


def _required_str(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Mongo change event field '{field}' must be a non-empty string.")
    return value


def _build_namespace(value: object) -> MongoCDCNamespace:
    if not isinstance(value, Mapping):
        raise TypeError("Mongo change event field 'ns' must be a mapping.")
    db = _required_str(value.get("db"), "ns.db")
    coll = value.get("coll")
    if coll is not None and not isinstance(coll, str):
        raise TypeError("Mongo change event field 'ns.coll' must be a string when present.")
    return MongoCDCNamespace(db=db, coll=coll)


def _build_cluster_time(value: object) -> MongoBsonTimestamp | None:
    if value is None:
        return None
    if isinstance(value, MongoBsonTimestamp):
        return value
    if type(value).__name__ == "Timestamp":
        normalized = _normalize_timestamp_mapping(value)
        return MongoBsonTimestamp(
            seconds=_required_int(normalized.get("seconds"), "clusterTime.seconds"),
            increment=_required_int(normalized.get("increment"), "clusterTime.increment"),
        )
    raise TypeError("Mongo change event field 'clusterTime' must be a BSON Timestamp.")


def _build_wall_time_ms(
    value: object,
    cluster_time: MongoBsonTimestamp | None,
) -> int | None:
    if value is None:
        return cluster_time.seconds * 1000 if cluster_time is not None else None
    if isinstance(value, datetime):
        return _datetime_to_epoch_ms(value)
    if type(value).__name__ == "DatetimeMS":
        # DatetimeMS.__int__() returns milliseconds since Unix epoch; safe for out-of-range years.
        return int(cast(Any, value))
    normalized = normalize_bson_value(value)
    if isinstance(normalized, int):
        return normalized
    raise TypeError("Mongo change event field 'wallTime' must be a datetime when present.")


def _normalize_optional_mapping(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    normalized = normalize_bson_value(value)
    if not isinstance(normalized, dict):
        raise TypeError("Normalized Mongo nested document must remain a mapping.")
    return normalized


def _document_id(value: object) -> MongoObjectId | str | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("Mongo change event field 'documentKey' must be a mapping when present.")
    raw = normalize_bson_value(value.get("_id"))
    return raw if isinstance(raw, (MongoObjectId, str)) else None


def _normalize_timestamp_mapping(value: object) -> dict[str, object]:
    seconds = getattr(value, "time", None)
    increment = getattr(value, "inc", None)
    if not isinstance(seconds, int) or not isinstance(increment, int):
        raise TypeError("BSON Timestamp must expose integer 'time' and 'inc' attributes.")
    return {"seconds": seconds, "increment": increment}


def _normalize_decimal128(value: object) -> str:
    to_decimal = getattr(value, "to_decimal", None)
    if callable(to_decimal):
        return str(to_decimal())
    return str(value)


def _normalize_binary(value: object) -> str:
    raw = bytes(cast(_SupportsBytes, value))
    return base64.b64encode(raw).decode("ascii")


def _normalize_dbref(value: object) -> object:
    collection = getattr(value, "collection", None)
    database = getattr(value, "database", None)
    identifier = getattr(value, "id", None)
    if not isinstance(collection, str):
        raise TypeError("BSON DBRef must expose a string 'collection' attribute.")
    if database is not None and not isinstance(database, str):
        raise TypeError("BSON DBRef attribute 'database' must be a string when present.")
    if identifier is None:
        raise TypeError("BSON DBRef must have a non-None 'id' attribute.")
    normalized_id = normalize_bson_value(identifier)
    id_str = normalized_id.id if isinstance(normalized_id, MongoObjectId) else str(normalized_id)
    return MongoDBRef(id=id_str, collection=collection, database=database)


def _required_int(value: object, field: str) -> int:
    if not isinstance(value, int):
        raise TypeError(f"Mongo change event field '{field}' must be an integer.")
    return value


def _datetime_to_epoch_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.astimezone(UTC).timestamp() * 1000)


def _current_time_ms() -> int:
    return _datetime_to_epoch_ms(datetime.now(UTC))


def _event_time_ms(
    wall_time_ms: int | None,
    cluster_time: MongoBsonTimestamp | None,
) -> int | None:
    if wall_time_ms is not None:
        return wall_time_ms
    if cluster_time is not None:
        return cluster_time.seconds * 1000
    return None


def _event_lag_ms(
    wall_time_ms: int | None,
    cluster_time: MongoBsonTimestamp | None,
) -> int | None:
    event_time_ms = _event_time_ms(wall_time_ms, cluster_time)
    if event_time_ms is None:
        return None
    return max(0, _current_time_ms() - event_time_ms)


def _normalize_objectid(value: object) -> object:
    generation_time = getattr(value, "generation_time", None)
    created_at_ms = (
        _datetime_to_epoch_ms(generation_time) if isinstance(generation_time, datetime) else None
    )
    return MongoObjectId(id=str(value), created_at_ms=created_at_ms)


_BSON_NORMALIZERS: dict[str, Callable[[Any], object]] = {
    "ObjectId": _normalize_objectid,
    "Timestamp": _normalize_timestamp_mapping,
    "Decimal128": _normalize_decimal128,
    "Binary": _normalize_binary,
    "DBRef": _normalize_dbref,
    "DatetimeMS": int,
    # NumberLong fields and the txnNumber of every multi-document transaction event.
    "Int64": int,
}


__all__ = ["build_mongo_cdc_event", "build_mongo_cdc_message", "normalize_bson_value"]
