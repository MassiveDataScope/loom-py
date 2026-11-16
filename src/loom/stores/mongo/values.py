"""Storage form of model values in MongoDB documents.

BSON encodes ``datetime`` natively, at millisecond precision and without an
offset: a naive value is taken as UTC and a client built with ``tz_aware``
reads every datetime back as aware UTC. Every other rich type is stored as the
string msgspec emits for it: ``date`` and ``time`` in ISO 8601, ``Decimal``
and ``UUID`` through ``str``, an ``Enum`` as its value.

:func:`to_storage_value` applies that conversion to a document field and to a
filter value alike, so a filter compares against exactly what the document
holds. No driver import: the query compiler uses it without the extra.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from uuid import UUID

_MICROSECONDS_PER_MILLISECOND = 1000


def to_storage_datetime(value: datetime) -> datetime:
    """Return ``value`` as BSON keeps it: aware UTC at millisecond precision.

    Args:
        value: Naive (taken as UTC) or aware datetime.

    Returns:
        The equivalent aware UTC datetime with microseconds truncated to
        the millisecond.
    """
    utc = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    truncated = utc.microsecond - utc.microsecond % _MICROSECONDS_PER_MILLISECOND
    return utc.replace(microsecond=truncated)


def to_storage_value(value: object) -> object:
    """Return the form ``value`` takes inside a document.

    Args:
        value: A model field value or a filter value.

    Returns:
        ``value`` converted per its type; values of any other type are
        returned unchanged.
    """
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, datetime):
        return to_storage_datetime(value)
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, Decimal | UUID):
        return str(value)
    return value


__all__ = ["to_storage_datetime", "to_storage_value"]
