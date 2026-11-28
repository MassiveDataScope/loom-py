"""Fitting parsed JSON to a frame type: one rule per leaf type, two policies for a misfit."""

from __future__ import annotations

import decimal
import json
import re
import sys
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, Final, Protocol

import msgspec

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.schema._schema import (
    ArrayType,
    DatetimeType,
    DecimalType,
    EnumType,
    FrameType,
    ListType,
    LoomDtype,
    StructType,
)

__all__ = ["JsonFit", "JsonLeafUnsupported", "JsonMismatch", "JsonRefusal"]

_DECODER: Final = msgspec.json.Decoder()
_ENCODER: Final = msgspec.json.Encoder()
_EPOCH_DATE: Final = date(1970, 1, 1)
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final = timedelta(microseconds=1)
_ISO_TEXT: Final = re.compile(
    r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:\d{2})?)?"
)
_DECIMAL_TEXT: Final = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?", re.ASCII)
_NUMBERS: Final = (int, float)
_FLOAT32_MAX: Final = float.fromhex("0x1.fffffep127")
_MICROSECONDS_IN_INT64: Final[Mapping[str, int]] = {
    "ns": 2**63 // 1000,
    "us": 2**63,
    "ms": 2**63 * 1000,
}


class JsonRefusal(InvalidValue):
    """A JSON value that does not fit its declared type, under :attr:`JsonMismatch.FAIL`.

    The message names the path of the value and the rule it broke, never the value.
    """


class JsonLeafUnsupported(InvalidType):
    """A leaf type no JSON rule reads.

    Args:
        path: Path of the leaf below the root of the declared type.
        leaf: The type with no rule.

    Attributes:
        path: Path of the leaf below the root of the declared type.
        leaf: The type with no rule.
    """

    def __init__(self, path: str, leaf: FrameType) -> None:
        super().__init__(f"{path} is {leaf!r}, which JSON text cannot hold")
        self.path = path
        self.leaf = leaf


class Misfit(Exception):
    """A value that breaks the rule of its slot; ``expected`` says what the rule asks for."""

    def __init__(self, expected: str) -> None:
        super().__init__(expected)
        self.expected = expected


class JsonMismatch(StrEnum):
    """What a JSON column does with a value that does not fit its declared type.

    ``FAIL`` raises :class:`JsonRefusal`, naming the column and the path of the value.
    ``NULL`` nulls only that value and keeps its siblings; a document that is not
    JSON, or whose top level does not fit, nulls the column value.
    """

    FAIL = "fail"
    NULL = "null"

    def settle(self, path: str, misfit: Misfit) -> None:
        """Return ``None`` to null the value under ``NULL``; raise under ``FAIL``."""
        if self is JsonMismatch.NULL:
            return None
        raise JsonRefusal(f"{path} expected {misfit.expected}")


class Rule(Protocol):
    """What a slot of the document holds."""

    def canonical(self, value: Any) -> Any:
        """The canonical form of *value*, which Polars' JSON reader reads exactly.

        Raises:
            Misfit: *value* is not what the rule asks for.
        """
        ...


@dataclass(frozen=True, slots=True)
class Integer:
    """A JSON integer, not a boolean, within an inclusive range."""

    minimum: int
    maximum: int

    @classmethod
    def signed(cls, bits: int) -> Integer:
        """The integers of a two's-complement type *bits* wide."""
        return cls(-(2 ** (bits - 1)), 2 ** (bits - 1) - 1)

    @classmethod
    def unsigned(cls, bits: int) -> Integer:
        """The integers of an unsigned type *bits* wide."""
        return cls(0, 2**bits - 1)

    def canonical(self, value: Any) -> int:
        """*value* when it is an integer in range."""
        if type(value) is int and self.minimum <= value <= self.maximum:
            return value
        raise Misfit(f"an integer from {self.minimum} to {self.maximum}")


@dataclass(frozen=True, slots=True)
class Real:
    """A finite JSON number of at most a magnitude."""

    maximum: float

    def canonical(self, value: Any) -> float:
        """*value* as a float when it is a finite number in range."""
        if type(value) in _NUMBERS and abs(value) <= self.maximum:
            return float(value)
        raise Misfit(f"a finite number of at most {self.maximum}")


@dataclass(frozen=True, slots=True)
class Exact:
    """A JSON value of exactly one Python type."""

    kind: type

    def canonical(self, value: Any) -> Any:
        """*value* when its type is exactly :attr:`kind`."""
        if type(value) is self.kind:
            return value
        raise Misfit(f"a {self.kind.__name__}")


@dataclass(frozen=True, slots=True)
class Text:
    """Any JSON value, kept as text: a string as it is, anything else as its JSON text."""

    def canonical(self, value: Any) -> str:
        """*value* as text."""
        if type(value) is str:
            return value
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class IsoDate:
    """A string ``YYYY-MM-DD`` or a valid ISO datetime, of which the date is kept."""

    def canonical(self, value: Any) -> int:
        """The days from the epoch to the date of *value*."""
        moment = _iso_moment(value, "a date as YYYY-MM-DD, or an ISO datetime")
        return (moment.date() - _EPOCH_DATE).days


@dataclass(frozen=True, slots=True)
class IsoInstant:
    """A valid ISO datetime string, or a date at midnight, read as an instant in UTC.

    Text with no offset is UTC and an offset is converted to the instant it denotes.

    Args:
        limit: Microseconds either side of the epoch the timestamp type holds.
    """

    limit: int

    @classmethod
    def of_unit(cls, time_unit: str) -> IsoInstant:
        """The instants a timestamp of *time_unit* holds."""
        return cls(_MICROSECONDS_IN_INT64[time_unit])

    def canonical(self, value: Any) -> str:
        """The UTC instant of *value* as ``YYYY-MM-DDTHH:MM:SS.ffffffZ``."""
        moment = _iso_moment(value, "an ISO datetime, or a date")
        try:
            instant = (
                moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
            )
        except OverflowError:
            raise Misfit("an instant between the years 1 and 9999") from None
        if abs((instant - _EPOCH) // _MICROSECOND) > self.limit:
            raise Misfit("an instant its timestamp type holds")
        return instant.replace(tzinfo=None).isoformat(timespec="microseconds") + "Z"


@dataclass(frozen=True, slots=True)
class Decimal:
    """A JSON number or numeric text that fits the precision and scale, rounded half up."""

    precision: int
    scale: int
    quantum: decimal.Decimal
    context: decimal.Context

    @classmethod
    def of(cls, precision: int, scale: int) -> Decimal:
        """The decimals of *precision* digits, *scale* of them after the point."""
        context = decimal.Context(
            prec=precision,
            rounding=decimal.ROUND_HALF_UP,
            Emax=decimal.MAX_EMAX,
            Emin=decimal.MIN_EMIN,
            traps=[decimal.InvalidOperation],
        )
        return cls(precision, scale, decimal.Decimal(1).scaleb(-scale), context)

    def canonical(self, value: Any) -> str:
        """The exact text of *value* at this scale."""
        expected = f"a decimal of {self.precision} digits, {self.scale} after the point"
        if type(value) is str and _DECIMAL_TEXT.fullmatch(value):
            text = value
        elif type(value) in _NUMBERS:
            text = repr(value)
        else:
            raise Misfit(expected)
        exact = decimal.Decimal(text)
        try:
            fitted = exact.quantize(self.quantum, context=self.context)
        except decimal.InvalidOperation:
            raise Misfit(expected) from None
        if not fitted.is_finite():
            raise Misfit(expected)
        return format(fitted, "f")


@dataclass(frozen=True, slots=True)
class Member:
    """A string that is one of the members of an enum."""

    members: frozenset[str]

    def canonical(self, value: Any) -> str:
        """*value* when it is a member."""
        if type(value) is str and value in self.members:
            return value
        raise Misfit("a member of the enum")


@dataclass(frozen=True, slots=True)
class _Fields:
    slots: tuple[tuple[str, _Slot], ...]

    def canonical(self, value: Any) -> dict[str, Any]:
        if type(value) is not dict:
            raise Misfit("an object")
        return {name: slot.fit(value.get(name)) for name, slot in self.slots}


@dataclass(frozen=True, slots=True)
class _Items:
    item: _Slot
    width: int | None

    def canonical(self, value: Any) -> list[Any]:
        if type(value) is not list:
            raise Misfit("an array")
        if self.width is not None and len(value) != self.width:
            raise Misfit(f"an array of {self.width} items")
        return [self.item.fit(item) for item in value]


_LEAF_RULES: Final[Mapping[FrameType, Rule]] = {
    LoomDtype.INT8: Integer.signed(8),
    LoomDtype.INT16: Integer.signed(16),
    LoomDtype.INT32: Integer.signed(32),
    LoomDtype.INT64: Integer.signed(64),
    LoomDtype.UINT8: Integer.unsigned(8),
    LoomDtype.UINT16: Integer.unsigned(16),
    LoomDtype.UINT32: Integer.unsigned(32),
    LoomDtype.UINT64: Integer.unsigned(64),
    LoomDtype.FLOAT32: Real(_FLOAT32_MAX),
    LoomDtype.FLOAT64: Real(sys.float_info.max),
    LoomDtype.BOOLEAN: Exact(bool),
    LoomDtype.UTF8: Text(),
    LoomDtype.DATE: IsoDate(),
    LoomDtype.DATETIME: IsoInstant.of_unit("us"),
}


@dataclass(frozen=True, slots=True)
class _Slot:
    """A position in the document: the rule it holds, where it is, and what a misfit does."""

    rule: Rule
    path: str
    on_mismatch: JsonMismatch

    @classmethod
    def of(cls, frame_type: FrameType, path: str, on_mismatch: JsonMismatch) -> _Slot:
        """The slot at *path* for a *frame_type*, with a slot for each of its children.

        Raises:
            JsonLeafUnsupported: A leaf of *frame_type* has no rule.
        """
        match frame_type:
            case StructType(fields=fields):
                rule: Rule = _Fields(
                    tuple(
                        (f.name, cls.of(f.dtype, f"{path}.{f.name}", on_mismatch)) for f in fields
                    )
                )
            case ListType(inner=inner):
                rule = _Items(cls.of(inner, f"{path}[]", on_mismatch), None)
            case ArrayType(inner=inner, width=width):
                rule = _Items(cls.of(inner, f"{path}[]", on_mismatch), width)
            case EnumType(categories=categories):
                rule = Member(frozenset(categories))
            case DecimalType(precision=precision, scale=scale):
                rule = Decimal.of(precision, scale)
            case DatetimeType(time_unit=time_unit):
                rule = IsoInstant.of_unit(time_unit)
            case _ if frame_type in _LEAF_RULES:
                rule = _LEAF_RULES[frame_type]
            case _:
                raise JsonLeafUnsupported(path, frame_type)
        return cls(rule, path, on_mismatch)

    def fit(self, value: Any) -> Any:
        """The canonical form of *value*, or what the policy settles for a misfit."""
        if value is None:
            return None
        try:
            return self.rule.canonical(value)
        except Misfit as misfit:
            self.on_mismatch.settle(self.path, misfit)
            return None


def _iso_moment(value: Any, expected: str) -> datetime:
    if type(value) is str and _ISO_TEXT.fullmatch(value):
        with suppress(ValueError):
            return datetime.fromisoformat(value)
    raise Misfit(expected)


def _parsed(document: str) -> Any:
    try:
        return _DECODER.decode(document)
    except msgspec.DecodeError:
        return json.loads(document)


@dataclass(frozen=True, slots=True)
class JsonFit:
    """Fits JSON documents to a frame type, one rule per leaf, under a mismatch policy.

    A document becomes the canonical JSON text of its type: every date, timestamp and
    decimal in a form Polars' JSON reader reads exactly, every struct holding exactly
    its declared fields.

    Args:
        on_mismatch: What a value that does not fit does.
        root: The slot of the whole document, built by :meth:`of`.
    """

    on_mismatch: JsonMismatch
    root: _Slot

    @classmethod
    def of(cls, frame_type: FrameType, on_mismatch: JsonMismatch) -> JsonFit:
        """The fit of documents to *frame_type*; struct field names are the names in the documents.

        Raises:
            JsonLeafUnsupported: A leaf of *frame_type* has no rule.
        """
        return cls(on_mismatch, _Slot.of(frame_type, "$", on_mismatch))

    def document(self, text: str | None) -> bytes | None:
        """The canonical JSON text of the document *text*; ``None`` for a null document.

        Raises:
            JsonRefusal: Under :attr:`JsonMismatch.FAIL`, *text* is not JSON or a value
                in it does not fit its type.
        """
        if text is None:
            return None
        try:
            parsed = _parsed(text)
        except (ValueError, RecursionError):
            self.on_mismatch.settle("$", Misfit("a JSON document"))
            return None
        try:
            return _ENCODER.encode(self.root.fit(parsed))
        except (msgspec.EncodeError, UnicodeError):
            self.on_mismatch.settle("$", Misfit("text that encodes as UTF-8"))
            return None
