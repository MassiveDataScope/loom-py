"""Value rendering: what a field value becomes before anything writes it.

A log record, a span attribute or a failure's declared field holds a scalar or
a short list of scalars; anything else is written as its type name, never its
``repr``. :func:`render_value` is the one owner of that rule.
"""

from __future__ import annotations

import math
from typing import Final, TypeGuard

from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered text value; a longer one ends in :data:`TRUNCATION_MARKER`."""

MAX_LIST_ITEMS: Final = 20
"""Items of a list value; a longer list is rendered as its type name."""

TRUNCATION_MARKER: Final = "...[truncated]"

_SCALARS: Final = (bool, int, float, str)
_MASKED: Final = (Secret, RedactedMap)


def _truncated(text: str) -> str:
    if len(text) <= MAX_VALUE_LENGTH:
        return text
    return text[: MAX_VALUE_LENGTH - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


def _scalar(value: object) -> object:
    """*value*, a string cut and a non-finite float as text."""
    if isinstance(value, str):
        return _truncated(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def render_value(value: object) -> object:
    """What a field *value* is written as.

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
