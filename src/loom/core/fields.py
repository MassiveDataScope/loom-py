"""What a field is: its key, its event, its value and how a value is rendered.

A leaf module: it imports nothing from loom, so the errors, the logger and the
model can all depend on it. A loom log record is *(event, level, fields)*; an
error's declared fields and a span's attributes are fields too. Each of them
holds a :data:`Scalar` or a short list of scalars, under a :class:`LogKey`
member, and :func:`render_value` is the one owner of what any other value
becomes: an object that knows its safe form declares it as a :class:`FieldValue`.
"""

from __future__ import annotations

import copy
import math
import re
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Final, Generic, TypeGuard, TypeVar

T = TypeVar("T")

EVENT_FORM: Final = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+")
"""Dotted lowercase words, at least two: ``job.delivery_abandoned``."""

Scalar = str | int | float | bool
"""A field value that needs no rendering: what a span attribute may hold."""

MASK: Final = "***"
"""What a secret is written as."""

MAX_VALUE_LENGTH: Final = 2048
"""Characters of a rendered text value; a longer one ends in :data:`TRUNCATION_MARKER`."""

MAX_LIST_ITEMS: Final = 20
"""Items of a list value; a longer list is rendered as its type name."""

TRUNCATION_MARKER: Final = "...[truncated]"

_SCALARS: Final = (bool, int, float, str)


class LogEvent(StrEnum):
    """Base of an owner's log events.

    A record's message is the event, never a sentence with values in it: the
    values travel in the record's fields, under :class:`LogKey` members, so a
    collector can filter on both. A member's value is its full name, dotted
    lowercase words (:data:`EVENT_FORM`); an architecture test enforces it.

    Example::

        class BillingLogEvent(LogEvent):
            INVOICE_ISSUED = "billing.invoice_issued"

        log.info(BillingLogEvent.INVOICE_ISSUED, extra={LogField.COUNT: 3})
    """

    @property
    def is_well_formed(self) -> bool:
        """Whether the value is dotted lowercase words."""
        return EVENT_FORM.fullmatch(self.value) is not None


class LogKey(StrEnum):
    """Base of every vocabulary of field keys."""


class FieldValue(ABC):
    """A value that declares what a field holding it is written as.

    A credential, an address or a mapping with secrets knows its safe form;
    :func:`render_value` asks it, and writes any other object as its type name.
    """

    __slots__ = ()

    @abstractmethod
    def field_value(self) -> Scalar:
        """What a field holding this value is written as, with no secret in it."""


class Secret(FieldValue, Generic[T]):
    """A credential. Display is always ``***``; the value comes from :meth:`reveal`.

    Args:
        value: The credential. A blank string is refused.

    Raises:
        ValueError: When *value* is a blank string. The message does not include it.
    """

    __slots__ = ("_value",)

    def __init__(self, value: T) -> None:
        if isinstance(value, str) and not value.strip():
            raise ValueError("must not be empty")
        self._value = value

    def reveal(self) -> T:
        """Return the credential. A mapping or a sequence is a copy."""
        value = self._value
        if isinstance(value, (dict, list)):
            return copy.deepcopy(value)
        return value

    def field_value(self) -> Scalar:
        """The mask."""
        return MASK

    def __repr__(self) -> str:
        return MASK

    def __str__(self) -> str:
        return MASK

    def __format__(self, format_spec: str) -> str:
        return MASK

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Secret):
            return bool(self._value == other._value)
        return bool(self._value == other)

    def __hash__(self) -> int:
        return hash(self._value)


def render_value(value: object) -> object:
    """What a field *value* is written as.

    A pure transformation every writer of a field shares, so it stays a
    function. ``None``, a boolean, a number or a string is written as it is, a
    string cut at :data:`MAX_VALUE_LENGTH` and a non-finite float as text; a
    list or tuple of at most :data:`MAX_LIST_ITEMS` scalars is written as a
    list; a :class:`FieldValue` is the form it declares, a :class:`Secret`
    the mask; any other object is its type name, never its ``repr``.
    """
    if value is None:
        return None
    if isinstance(value, _SCALARS):
        return _rendered_scalar(value)
    if isinstance(value, FieldValue):
        return _rendered_scalar(value.field_value())
    if _is_short_scalar_list(value):
        return [_rendered_scalar(item) for item in value]
    return type(value).__name__


def _rendered_scalar(value: object) -> object:
    if isinstance(value, str) and len(value) > MAX_VALUE_LENGTH:
        return value[: MAX_VALUE_LENGTH - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def _is_short_scalar_list(value: object) -> TypeGuard[list[object] | tuple[object, ...]]:
    return (
        isinstance(value, list | tuple)
        and len(value) <= MAX_LIST_ITEMS
        and all(item is None or isinstance(item, _SCALARS) for item in value)
    )
