"""Value rendering: what a record's field values become before any handler writes them.

``makeRecord`` applies ``extra`` after the record factory, so the values are
rendered by :class:`LogFilter`, a filter loom puts on every handler it owns and
on the root handlers it finds.
"""

from __future__ import annotations

import logging
import math
from typing import Final, TypeGuard

from loom.core.logger.record import STAMP, RecordStamp
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered text value; a longer one ends in :data:`TRUNCATION_MARKER`."""

MAX_LIST_ITEMS: Final = 20
"""Items of a list value; a longer list is rendered as its type name."""

TRUNCATION_MARKER: Final = "...[truncated]"

STANDARD_ATTRIBUTES: Final = frozenset(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
}
"""The attributes the standard library gives every record; any other one is a field."""

_SCALARS: Final = (bool, int, float, str)
_MASKED: Final = (Secret, RedactedMap)


def _truncated(text: str) -> str:
    if len(text) <= MAX_VALUE_LENGTH:
        return text
    return text[: MAX_VALUE_LENGTH - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


def _scalar(value: object) -> object:
    """*value* when it is a scalar, a string cut, a non-finite float as text; else ``None``."""
    if isinstance(value, str):
        return _truncated(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def render_value(value: object) -> object:
    """What a field *value* is written as: the one owner of value rendering.

    ``None``, a boolean, a number or a string is written as it is, a string cut
    at :data:`MAX_VALUE_LENGTH`; a list or tuple of at most :data:`MAX_LIST_ITEMS`
    scalars is written as a list; a ``Secret`` is the mask and a ``Dsn`` its
    masked text; any other object is its type name, never its ``repr``.
    """
    if value is None:
        return None
    if isinstance(value, _SCALARS):
        return _scalar(value)
    if isinstance(value, _MASKED):
        return MASK
    if isinstance(value, Dsn):
        return _truncated(str(value))
    if _is_short_scalar_list(value):
        return [_scalar(item) for item in value]
    return type(value).__name__


def _is_short_scalar_list(value: object) -> TypeGuard[list[object] | tuple[object, ...]]:
    return (
        isinstance(value, list | tuple)
        and len(value) <= MAX_LIST_ITEMS
        and all(item is None or isinstance(item, _SCALARS) for item in value)
    )


class LogFilter(logging.Filter):
    """Render the field values of a record and write its stamp as fields; never drops one.

    A field whose value is ``None`` is not known, and is not written, so a
    stamped field such as ``trace_id`` fills it. The filter is total: it
    raises for no value. The stamp the record factory
    left is its mark: the first handler's filter renders the values, writes the
    stamp as fields and removes it, and every later one lets the record through
    unchanged. A record no :class:`~loom.core.logger.record.LoomRecordFactory`
    created carries no stamp and passes as it is.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Render *record*'s values in place and let it through."""
        fields = record.__dict__
        stamp = fields.pop(STAMP, None)
        if not isinstance(stamp, RecordStamp):
            return True
        for key in fields.keys() - STANDARD_ATTRIBUTES:
            value = fields[key]
            if value is None:
                del fields[key]
            else:
                fields[key] = render_value(value)
        stamp.write_into(fields, record, render_value)
        return True
