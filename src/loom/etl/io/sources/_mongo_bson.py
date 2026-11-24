"""ETL-context BSON normalizer.

Converts raw pymongo document values to Python builtins that
``pl.from_dicts()`` can ingest without producing ``Object`` dtype columns.

Differs intentionally from ``loom.streaming.mongo._normalize``:

- ``datetime``   → kept as ``datetime`` (Polars infers ``Datetime(us)`` natively)
- ``ObjectId``   → ``str`` (hex string)
- ``Decimal128`` → ``float`` (via ``to_decimal()``; precision ≤ 15 sig. digits)
- ``Binary``     → ``bytes`` (Polars ``Binary`` column — no base64 overhead)
- ``Timestamp``  → ``int`` (``time`` field in seconds; BSON Timestamp is oplog-internal)
- ``DBRef``      → ``str`` (``"collection/id"`` repr)
- ``Mapping``    → ``dict[str, …]`` (keys coerced to ``str``, values recursed)
- ``Sequence``   → ``list`` (items recursed)
- bson-origin unknown dict-like → ``dict[str, …]`` (duck-typed via ``.items()``; warns)
- bson-origin unknown scalar    → ``None`` (null; avoids non-JSON ``Object(…)`` strings)
- Non-bson unknown types        → ``str(value)`` (original fallback preserved)

Note on ``Decimal128`` precision: Decimal128 is IEEE 754-2008 128-bit decimal
(up to 34 significant digits).  Conversion to float64 retains ~15-17 digits.
For high-precision monetary columns declare an explicit schema and apply a
column-level cast after reading.
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.event import EtlEvent, EtlKey
from loom.etl.io.sources._serialization import _record_serialization_failure

_log = logging.getLogger(__name__)

_MAX_DEPTH = 64

_warned_bson_types: set[str] = set()


def _is_bson_type(value: object) -> bool:
    """Return True if *value* originates from the bson/pymongo package tree."""
    module = type(value).__module__
    return module == "bson" or module.startswith("bson.")


def normalize_bson_doc(
    doc: dict[str, Any], *, extended_json: bool = False, _depth: int = 0
) -> dict[str, Any]:
    """Return a copy of *doc* with all BSON-specific values replaced by builtins.

    Args:
        doc:           Raw pymongo document dict.
        extended_json: Flatten literal MongoDB Extended JSON wrappers stored as
                       data (``{"$date": ...}``, ``{"$oid": ...}``, ...) into
                       their plain values. Opt-in: reinterpreting stored data
                       is a consumer decision.

    Returns:
        New dict safe to pass to ``pl.from_dicts()``.

    Raises:
        InvalidValue: When document nesting exceeds :data:`_MAX_DEPTH`.
    """
    if _depth > _MAX_DEPTH:
        raise InvalidValue(
            f"normalize_bson_doc: document exceeds maximum nesting depth of {_MAX_DEPTH}."
        )
    return {str(k): _normalize(v, _depth + 1, extended_json) for k, v in doc.items()}


def _normalize_bson_type(value: object, depth: int, extjson: bool) -> object:
    """Handle unknown bson-origin types — dict-like via .items(), scalars to None."""
    type_name = type(value).__name__
    if callable(getattr(value, "items", None)):
        if type_name not in _warned_bson_types:
            _warned_bson_types.add(type_name)
            _log.warning(EtlEvent.BSON_MAPPING_UNKNOWN, extra={EtlKey.VALUE_TYPE: type_name})
        try:
            return {str(k): _normalize(v, depth + 1, extjson) for k, v in value.items()}  # type: ignore[attr-defined]
        except (TypeError, AttributeError, ValueError) as exc:
            _log.warning(
                EtlEvent.BSON_MAPPING_FAILED,
                extra={EtlKey.VALUE_TYPE: type_name},
                exc_info=exc,
            )
    elif type_name not in _warned_bson_types:
        _warned_bson_types.add(type_name)
        _log.warning(EtlEvent.BSON_TYPE_UNKNOWN, extra={EtlKey.VALUE_TYPE: type_name})
    return None


_EXTJSON_MISS = object()

_EXTJSON_DATE_MAX_LEN = 40
_EXTJSON_OID_RE = re.compile(r"^[0-9a-f]{24}$")


def _date_from_iso(v: str) -> object:
    if len(v) > _EXTJSON_DATE_MAX_LEN:
        return _EXTJSON_MISS
    try:
        parsed = datetime.fromisoformat(v)
    except ValueError:
        return _EXTJSON_MISS
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _date_from_canonical(v: Mapping[str, Any]) -> object:
    try:
        return datetime.fromtimestamp(int(v["$numberLong"]) / 1000, tz=UTC)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return _EXTJSON_MISS


def _date_from_epoch_millis(v: float) -> object:
    try:
        return datetime.fromtimestamp(v / 1000, tz=UTC)
    except (ValueError, OverflowError, OSError):
        return _EXTJSON_MISS


def _extjson_date(v: object) -> object:
    if isinstance(v, str):
        return _date_from_iso(v)
    if isinstance(v, Mapping):
        return _date_from_canonical(v)
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return _date_from_epoch_millis(v)
    return _EXTJSON_MISS


def _extjson_str(v: object) -> object:
    if isinstance(v, str) and _EXTJSON_OID_RE.match(v):
        return v
    return _EXTJSON_MISS


def _extjson_int(v: object) -> object:
    if isinstance(v, bool):
        return _EXTJSON_MISS
    try:
        return int(v)  # type: ignore[call-overload]
    except (TypeError, ValueError, OverflowError):
        return _EXTJSON_MISS


def _extjson_float(v: object) -> object:
    if isinstance(v, bool):
        return _EXTJSON_MISS
    try:
        parsed = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return _EXTJSON_MISS
    return parsed if math.isfinite(parsed) else _EXTJSON_MISS


_EXTJSON_HANDLERS: dict[str, Callable[[object], object]] = {
    "$date": _extjson_date,
    "$oid": _extjson_str,
    "$numberDecimal": _extjson_float,
    "$numberDouble": _extjson_float,
    "$numberLong": _extjson_int,
    "$numberInt": _extjson_int,
}


def _flatten_extended_json(value: Mapping[str, Any]) -> object:
    handler = _EXTJSON_HANDLERS.get(next(iter(value)))
    if handler is None:
        return _EXTJSON_MISS
    return handler(next(iter(value.values())))


def _normalize_mapping(value: Mapping[str, Any], depth: int, extjson: bool) -> object:
    if extjson and len(value) == 1:
        flattened = _flatten_extended_json(value)
        if flattened is not _EXTJSON_MISS:
            return flattened
    return {str(k): _normalize(v, depth + 1, extjson) for k, v in value.items()}


def _normalize_unknown(value: object) -> object:
    """Non-bson unknown type: str() fallback preserved (avoids Polars Object dtype columns)."""
    _log.warning(EtlEvent.SOURCE_TYPE_STRINGIFIED, extra={EtlKey.VALUE_TYPE: type(value).__name__})
    try:
        return str(value)
    except Exception as exc:  # noqa: BLE001
        _record_serialization_failure(exc, value)
        return None


def _normalize(value: object, depth: int, extjson: bool = False) -> object:
    if depth > _MAX_DEPTH:
        raise InvalidValue(
            f"normalize_bson_doc: nested value exceeds maximum depth of {_MAX_DEPTH}."
        )
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, Mapping):
        return _normalize_mapping(value, depth, extjson)
    if isinstance(value, Sequence) and not isinstance(value, (bytearray, memoryview)):
        return [_normalize(item, depth + 1, extjson) for item in value]
    normalizer = _NORMALIZERS.get(type(value).__name__)
    if normalizer is not None:
        return normalizer(value)
    if _is_bson_type(value):
        return _normalize_bson_type(value, depth, extjson)
    return _normalize_unknown(value)


def _normalize_objectid(value: object) -> str:
    return str(value)


def _normalize_decimal128(value: object) -> float:
    to_decimal = getattr(value, "to_decimal", None)
    if callable(to_decimal):
        return float(str(to_decimal()))
    return float(str(value))


def _normalize_binary(value: object) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    raise InvalidType(f"Expected bytes-like BSON Binary, got {type(value)!r}")


def _normalize_timestamp(value: object) -> int:
    t = getattr(value, "time", None)
    return int(t) if isinstance(t, (int, float)) else 0


def _normalize_dbref(value: object) -> str:
    coll = getattr(value, "collection", "?")
    oid = getattr(value, "id", "?")
    return f"{coll}/{oid}"


_NORMALIZERS: dict[str, Callable[[object], object]] = {
    "ObjectId": _normalize_objectid,
    "Decimal128": _normalize_decimal128,
    "Binary": _normalize_binary,
    "Timestamp": _normalize_timestamp,
    "DBRef": _normalize_dbref,
}


def deep_normalize_for_json(value: Any, *, _depth: int = 0) -> Any:
    """Normalize BSON types recursively in an arbitrary value for JSON serialization.

    For full documents, prefer :func:`normalize_bson_doc`. This function is intended
    for single-value normalization before JSON serialization (e.g., str-declared fields).
    """
    return _normalize(value, _depth)


__all__ = ["normalize_bson_doc", "deep_normalize_for_json"]
