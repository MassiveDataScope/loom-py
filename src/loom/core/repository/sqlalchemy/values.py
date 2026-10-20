"""Column form of model values in SQLAlchemy rows.

SQLAlchemy binds ``bytes``, ``datetime``, ``date``, ``time``, ``Decimal`` and
``UUID`` natively; every other value is reduced to msgspec builtins (an
``Enum`` to its value, a struct to a dict). A JSON column holds builtins only,
so its values are reduced completely.

:func:`to_column_value` is the conversion the repository applies to a value it
writes and the query compiler to a filter value, so a filter compares against
exactly what the row holds.
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec

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


__all__ = ["to_column_value"]
