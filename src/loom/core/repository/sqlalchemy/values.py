"""Column form of model values in SQLAlchemy rows.

SQLAlchemy binds ``bytes``, ``datetime``, ``date``, ``time``, ``Decimal`` and
``UUID`` natively; every other value is reduced to msgspec builtins (an
``Enum`` to its value, a struct to a dict). A JSON column holds builtins only,
so its values are reduced completely.

:func:`to_column_value` is the conversion the repository applies to a value it
writes and the query compiler to a filter value, so a filter compares against
exactly what the row holds.

A ``time`` with a UTC offset is refused outside a JSON column: SQL ``TIME``
(Postgres ``time without time zone``, SQLite text) keeps no offset, so the
offset would be dropped on write and ignored by a filter.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec
from sqlalchemy import JSON

from loom.core.errors.codes import ErrorCode
from loom.core.model import BoundaryValidationError

# Values SQLAlchemy binds natively; encoding them to strings breaks typed columns.
_NATIVE_COLUMN_TYPES = (bytes, datetime, date, time, Decimal, UUID)


def to_column_value(value: object, *, json: bool = False) -> Any:
    """Return the form ``value`` takes in a column.

    Args:
        value: A model value or a filter value.
        json: ``True`` when the column is a JSON column.

    Returns:
        ``value`` reduced to msgspec builtins, keeping the natively bound
        types unless the column is a JSON column.
    """
    if json:
        return msgspec.to_builtins(value)
    return msgspec.to_builtins(value, builtin_types=_NATIVE_COLUMN_TYPES)


def refuse_aware_time(
    column: str, value: object, *, code: str = ErrorCode.BOUNDARY_VALIDATION
) -> None:
    """Raise when *value* is a ``time`` with a UTC offset bound to *column*.

    The message names the column, not the value.

    Args:
        column: The name of the column the value is bound to.
        value: A value about to be bound to a column other than JSON.
        code: The error code: ``boundary_validation`` for a written value,
            ``query_validation`` for a filter value.

    Raises:
        BoundaryValidationError: If *value* is a ``time`` whose ``tzinfo`` is
            set.
    """
    if isinstance(value, time) and value.tzinfo is not None:
        reason = (
            "a Time column stores naive times and would drop the UTC offset; pass a "
            "naive time, or declare a datetime field (DateTime(tz=True)) or a JSON "
            "column to keep the offset"
        )
        raise BoundaryValidationError(f"{column}: {reason}", ((column, reason),), code=code)


def is_json_column(column: object) -> bool:
    """Whether *column*, a SQLAlchemy column or mapped attribute, holds JSON.

    ``JSON`` and the dialect types derived from it (Postgres ``JSONB``) count.
    """
    return isinstance(getattr(column, "type", None), JSON)


__all__ = ["is_json_column", "refuse_aware_time", "to_column_value"]
