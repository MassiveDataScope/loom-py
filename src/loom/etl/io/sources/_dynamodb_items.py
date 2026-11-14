"""ETL-context DynamoDB item normalizer.

Converts raw low-level AttributeValue items (``{"S": ...}``, ``{"N": ...}``,
...) to Python builtins that ``pl.from_dicts()`` can ingest without producing
``Object`` dtype columns.

Normalization rules (mirrors the spirit of ``normalize_bson_doc``):

- ``Decimal`` → ``int`` when integral and within int64 range, else ``float``
- ``set``/``frozenset`` (from SS/NS/BS) → sorted ``list``
- ``Binary``/``bytes`` → plain ``bytes`` (Polars ``Binary`` column)
- nested ``dict``/``list`` values are recursed
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

try:
    from boto3.dynamodb import types as _ddb_types  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    _ddb_types = None

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_DESERIALIZER = _ddb_types.TypeDeserializer() if _ddb_types is not None else None


def normalize_dynamo_item(item: dict[str, Any]) -> dict[str, Any]:
    """Return *item* deserialized and normalized to plain Python builtins.

    Args:
        item: Raw low-level AttributeValue dict as returned by the boto3
              DynamoDB client (``response["Items"][i]``).

    Returns:
        New dict safe to pass to ``pl.from_dicts()``.

    Raises:
        RuntimeError: When boto3 is not installed.
    """
    if _DESERIALIZER is None:
        raise RuntimeError(
            "normalize_dynamo_item requires boto3. "
            "Install it with: pip install 'loom-kernel[dynamodb]'"
        )
    return {str(k): _normalize(_DESERIALIZER.deserialize(v)) for k, v in item.items()}


def _decimal_to_number(value: Decimal) -> int | float:
    if value % 1 == 0:
        integral = int(value)
        if _INT64_MIN <= integral <= _INT64_MAX:
            return integral
    return float(value)


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_to_number(value)
    if isinstance(value, (set, frozenset)):
        return sorted(_normalize(item) for item in value)
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if _ddb_types is not None and isinstance(value, _ddb_types.Binary):
        return bytes(value.value)
    return value


__all__ = ["normalize_dynamo_item"]
