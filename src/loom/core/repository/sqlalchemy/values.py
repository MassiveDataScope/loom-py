"""Column form of model values in SQLAlchemy rows.

SQLAlchemy binds ``bytes``, ``datetime``, ``date``, ``time``, ``Decimal`` and
``UUID`` natively; every other value is reduced to msgspec builtins (an
``Enum`` to its value, a struct to a dict). A JSON column holds builtins only,
so its values are reduced completely.

:func:`to_column_value` is the conversion the repository applies to a value it
writes, and :func:`to_comparison_value` the one it applies to a value compared
with a column (a filter, ``get_by``, ``exists_by``), so a comparison matches
exactly what the row holds.

A ``time`` with a UTC offset is refused for a column that cannot hold the
offset: SQL ``TIME`` without time zone (Postgres ``time``, SQLite text) stores
``08:30+02:00`` as ``08:30``, and an aware value compared with it matches the
row holding ``08:30``. A JSON column, which keeps the ISO text, takes any
``time``. A ``Time`` column with ``timezone=True`` (Postgres ``timetz``) takes
only a ``time`` with an offset: asyncpg cannot encode a naive one for it. The
SQLAlchemy backend refuses such a column at startup on any dialect other than
Postgres.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec
from sqlalchemy import JSON, Time

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


_OFFSET_REFUSED = "time must not carry a UTC offset"
_OFFSET_REQUIRED = "time must carry a UTC offset"


def check_time_offset(column: Any, value: object, *, code: str) -> None:
    """Raise when *value* is a ``time`` whose offset *column* cannot take as it is.

    A JSON column takes any ``time``. A ``Time(timezone=True)`` column keeps the
    offset and takes only a ``time`` that carries one; every other column
    drops the offset and takes only a naive ``time``. A model that needs the
    offset declares a JSON column or ``ColumnType("Time", kwargs={"timezone":
    True})`` (Postgres only), or a ``datetime`` field for the instant. The
    error names the column, not the value.

    Args:
        column: The SQLAlchemy column or mapped attribute the value is bound to.
        value: A value about to be written to or compared with *column*.
        code: ``boundary_validation`` for a written value, ``query_validation``
            for a compared value.

    Raises:
        BoundaryValidationError: If *value* is a ``time`` with an offset for a
            column that drops it, or a naive ``time`` for a
            ``Time(timezone=True)`` column.
    """
    if not isinstance(value, time):
        return
    column_type = getattr(column, "type", None)
    if isinstance(column_type, JSON):
        return
    keeps_offset = isinstance(column_type, Time) and bool(column_type.timezone)
    if (value.tzinfo is not None) == keeps_offset:
        return
    reason = _OFFSET_REQUIRED if keeps_offset else _OFFSET_REFUSED
    raise BoundaryValidationError(f"{column.key}: {reason}", ((column.key, reason),), code=code)


def to_comparison_value(column: Any, value: object) -> Any:
    """Return *value* in the form it is compared with *column*.

    Refused as :func:`check_time_offset` refuses it, and converted as
    :func:`to_column_value` converts a written value. A refusal is raised under
    ``query_validation`` whatever the value's source, so ``get_by`` and
    ``exists_by`` called with a value from a command body raise it too.

    Args:
        column: A SQLAlchemy column or mapped attribute.
        value: A filter or lookup value.

    Raises:
        BoundaryValidationError: ``query_validation``, for a ``time`` whose
            offset *column* cannot take, as :func:`check_time_offset` decides.
    """
    check_time_offset(column, value, code=ErrorCode.QUERY_VALIDATION)
    return to_column_value(value, json=is_json_column(column))


def is_json_column(column: object) -> bool:
    """Whether *column*, a SQLAlchemy column or mapped attribute, holds JSON.

    ``JSON`` and the dialect types derived from it (Postgres ``JSONB``) count.
    """
    return isinstance(getattr(column, "type", None), JSON)


__all__ = ["check_time_offset", "is_json_column", "to_column_value", "to_comparison_value"]
