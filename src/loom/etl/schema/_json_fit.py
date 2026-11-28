"""Fitting parsed JSON to a frame type: one rule per leaf type, two policies for a misfit."""

from __future__ import annotations

import decimal
import json
import math
import re
import sys
from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, ClassVar, Final, Protocol, assert_never

import msgspec

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.schema._schema import (
    ArrayType,
    CategoricalType,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    FrameType,
    ListType,
    LoomDtype,
    StructType,
)

__all__ = ["JsonFit", "JsonLeafUnsupported", "JsonMismatch", "JsonRefusal"]

_DECODER: Final = msgspec.json.Decoder(float_hook=msgspec.Raw)
_ENCODER: Final = msgspec.json.Encoder()
_NON_JSON: Final = object()
_FLOAT32_MAXIMUM: Final = math.nextafter((2.0 - 2.0**-24) * 2.0**127, 0.0)
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final = timedelta(microseconds=1)
_MICROSECOND_REACH: Final = {"ms": 2**63 * 10**3, "us": 2**63, "ns": 2**63 // 10**3}
_DECIMAL_TEXT: Final = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_ISO_TEXT: Final = re.compile(
    r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])"
    r"(?:[T ](?:[01][0-9]|2[0-3]):[0-5][0-9]"
    r"(?::[0-5][0-9](?:\.[0-9]{1,6})?)?"
    r"(?:Z|[+-](?:[01][0-9]|2[0-3]):?[0-5][0-9])?)?"
)


class JsonRefusal(InvalidValue):
    """A JSON value that does not fit its declared type, under :attr:`JsonMismatch.FAIL`.

    The message names the path of the value and the rule it broke, never the value.
    """


class JsonLeafUnsupported(InvalidType):
    """A leaf type no JSON rule reads.

    Attributes:
        path: Path of the leaf below the root of the declared type.
    """

    def __init__(self, path: str, leaf: FrameType) -> None:
        super().__init__(f"{leaf} at {path}, which no JSON rule reads")
        self.path = path


class Misfit(Exception):
    """A value that breaks the rule of its leaf; ``expected`` says what the rule asks for."""

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


class Rule(Protocol):
    """What a leaf of the document holds."""

    def fit(self, value: Any) -> Any:
        """The fitted form of *value*.

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

    def fit(self, value: Any) -> int:
        """*value* when it is an integer in range."""
        with suppress(ValueError):
            number = int(bytes(value)) if type(value) is msgspec.Raw else value
            if type(number) is int and self.minimum <= number <= self.maximum:
                return number
        raise Misfit(f"an integer from {self.minimum} to {self.maximum}")


@dataclass(frozen=True, slots=True)
class Real:
    """A finite JSON number of at most a magnitude."""

    maximum: float

    def fit(self, value: Any) -> float:
        """*value* as a float when it is a finite number in range."""
        number = float(bytes(value)) if type(value) is msgspec.Raw else value
        if type(number) in (int, float) and abs(number) <= self.maximum:
            return float(number)
        raise Misfit(f"a finite number of at most {self.maximum}")


@dataclass(frozen=True, slots=True)
class Boolean:
    """A JSON ``true`` or ``false``."""

    def fit(self, value: Any) -> bool:
        """*value* when it is a boolean."""
        if type(value) is bool:
            return value
        raise Misfit("a boolean")


@dataclass(frozen=True, slots=True)
class Text:
    """Any JSON value, kept as text: a string as it is, anything else as its JSON text."""

    def fit(self, value: Any) -> str:
        """*value* as text that encodes as UTF-8."""
        try:
            match value:
                case str():
                    if not value.isascii():
                        value.encode()
                    return value
                case bool():
                    return "true" if value else "false"
                case int():
                    return str(value)
                case msgspec.Raw():
                    return bytes(value).decode()
                case _:
                    return _ENCODER.encode(value).decode()
        except (ValueError, TypeError, RecursionError, msgspec.EncodeError):
            raise Misfit("a JSON value whose text is UTF-8") from None


@dataclass(frozen=True, slots=True)
class _IsoText:
    expected: ClassVar[str]

    def moment(self, value: Any) -> datetime:
        """The datetime of the ISO text *value*.

        Raises:
            Misfit: *value* is not ISO text of a valid datetime or date.
        """
        if type(value) is str and _ISO_TEXT.fullmatch(value):
            with suppress(ValueError):
                return datetime.fromisoformat(value)
        raise Misfit(self.expected)


@dataclass(frozen=True, slots=True)
class IsoDate(_IsoText):
    """A string ``YYYY-MM-DD`` or a valid ISO datetime, of which the date is kept."""

    expected: ClassVar[str] = "a date as YYYY-MM-DD, or an ISO datetime"

    def fit(self, value: Any) -> date:
        """The date of *value*."""
        return self.moment(value).date()


@dataclass(frozen=True, slots=True)
class IsoInstant(_IsoText):
    """A valid ISO datetime string, or a date at midnight, read as an instant in UTC.

    Text with no offset is UTC and an offset is converted to the instant it denotes.

    Args:
        zoned: Whether the instant keeps its UTC zone; otherwise it is naive.
        reach: The microseconds before and after the epoch the instants may be.
    """

    expected: ClassVar[str] = "an ISO datetime, or a date"
    unheld: ClassVar[str] = "an instant its timestamp type holds"
    zoned: bool
    reach: int

    @classmethod
    def of(cls, frame_type: DatetimeType) -> IsoInstant:
        """The instants a signed 64-bit count of the time units of *frame_type* holds."""
        return cls(frame_type.time_zone is not None, _MICROSECOND_REACH[frame_type.time_unit])

    def fit(self, value: Any) -> datetime:
        """The UTC instant of *value*."""
        moment = self.moment(value)
        try:
            instant = (
                moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
            )
        except OverflowError:
            raise Misfit(self.unheld) from None
        if abs((instant - _EPOCH) // _MICROSECOND) > self.reach:
            raise Misfit(self.unheld)
        return instant if self.zoned else instant.replace(tzinfo=None)


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

    @property
    def expected(self) -> str:
        """What this rule asks for."""
        return f"a decimal of {self.precision} digits, {self.scale} after the point"

    def fit(self, value: Any) -> decimal.Decimal:
        """*value* at this scale."""
        try:
            fitted = self._exact(value).quantize(self.quantum, context=self.context)
        except decimal.InvalidOperation:
            raise Misfit(self.expected) from None
        if not fitted.is_finite():
            raise Misfit(self.expected)
        return fitted

    def _exact(self, value: Any) -> decimal.Decimal:
        if type(value) is str and _DECIMAL_TEXT.fullmatch(value):
            return decimal.Decimal(value)
        if type(value) is int:
            return decimal.Decimal(value)
        if type(value) is msgspec.Raw:
            return decimal.Decimal(bytes(value).decode())
        raise Misfit(self.expected)


@dataclass(frozen=True, slots=True)
class Member:
    """A string that is one of the members of an enum."""

    members: frozenset[str]

    def fit(self, value: Any) -> str:
        """*value* when it is a member."""
        if type(value) is str and value in self.members:
            return value
        raise Misfit("a member of the enum")


_LEAF_RULES: Final[Mapping[LoomDtype, Rule]] = {
    LoomDtype.INT8: Integer.signed(8),
    LoomDtype.INT16: Integer.signed(16),
    LoomDtype.INT32: Integer.signed(32),
    LoomDtype.INT64: Integer.signed(64),
    LoomDtype.UINT8: Integer.unsigned(8),
    LoomDtype.UINT16: Integer.unsigned(16),
    LoomDtype.UINT32: Integer.unsigned(32),
    LoomDtype.UINT64: Integer.unsigned(64),
    LoomDtype.FLOAT32: Real(_FLOAT32_MAXIMUM),
    LoomDtype.FLOAT64: Real(sys.float_info.max),
    LoomDtype.BOOLEAN: Boolean(),
    LoomDtype.UTF8: Text(),
    LoomDtype.DATE: IsoDate(),
    LoomDtype.DATETIME: IsoInstant.of(DatetimeType()),
}


@dataclass(frozen=True, slots=True)
class _Node(ABC):
    """A position in the declared type, at a path; it fits the value found there."""

    path: str

    @classmethod
    def of(cls, frame_type: FrameType, path: str) -> _Node:
        """The node at *path* for a *frame_type*, with a node for each of its children.

        Raises:
            JsonLeafUnsupported: A leaf of *frame_type* has no rule.
        """
        match frame_type:
            case StructType(fields=fields):
                children = tuple((f.name, cls.of(f.dtype, f"{path}.{f.name}")) for f in fields)
                return _Fields(path, children)
            case ListType(inner=inner):
                return _Items(path, cls.of(inner, f"{path}[]"), None)
            case ArrayType(inner=inner, width=width):
                return _Items(path, cls.of(inner, f"{path}[]"), width)
            case EnumType(categories=categories):
                rule: Rule = Member(frozenset(categories))
            case DecimalType(precision=precision, scale=scale):
                rule = Decimal.of(precision, scale)
            case DatetimeType(time_unit=unit) if unit in _MICROSECOND_REACH:
                rule = IsoInstant.of(frame_type)
            case LoomDtype() if frame_type in _LEAF_RULES:
                rule = _LEAF_RULES[frame_type]
            case LoomDtype() | DatetimeType() | DurationType() | CategoricalType():
                raise JsonLeafUnsupported(path, frame_type)
            case _:
                assert_never(frame_type)
        return _Leaf(path, rule)

    def fit(self, value: Any, refusals: list[JsonRefusal]) -> Any:
        """The fitted form of *value*; a misfit adds its refusal to *refusals* and fits to null."""
        if value is None:
            return None
        try:
            return self._fitted(value, refusals)
        except Misfit as misfit:
            refusals.append(JsonRefusal(f"{self.path} expected {misfit.expected}"))
            return None

    @abstractmethod
    def _fitted(self, value: Any, refusals: list[JsonRefusal]) -> Any:
        """The fitted form of the value that is not null.

        Raises:
            Misfit: *value* is not what this node asks for.
        """


@dataclass(frozen=True, slots=True)
class _Leaf(_Node):
    rule: Rule

    def _fitted(self, value: Any, refusals: list[JsonRefusal]) -> Any:
        return self.rule.fit(value)


@dataclass(frozen=True, slots=True)
class _Fields(_Node):
    children: tuple[tuple[str, _Node], ...]

    def _fitted(self, value: Any, refusals: list[JsonRefusal]) -> Any:
        if type(value) is not dict:
            raise Misfit("an object")
        return {name: child.fit(value.get(name), refusals) for name, child in self.children}


@dataclass(frozen=True, slots=True)
class _Items(_Node):
    item: _Node
    width: int | None

    def _fitted(self, value: Any, refusals: list[JsonRefusal]) -> Any:
        if type(value) is not list:
            raise Misfit("an array")
        if self.width is not None and len(value) != self.width:
            raise Misfit(f"an array of {self.width} items")
        return [self.item.fit(item, refusals) for item in value]


@dataclass(frozen=True, slots=True)
class JsonFit:
    """Fits JSON documents to a frame type, one rule per leaf, under a mismatch policy.

    A document becomes the structure of its type: every struct a dict holding exactly its
    declared fields, every list a list and every leaf the Python value its rule returns.

    Args:
        on_mismatch: What a value that does not fit does.
        root: The node of the whole document.
    """

    on_mismatch: JsonMismatch
    root: _Node

    @classmethod
    def of(cls, frame_type: FrameType, on_mismatch: JsonMismatch) -> JsonFit:
        """The fit of documents to *frame_type* under *on_mismatch*.

        Raises:
            JsonLeafUnsupported: A leaf of *frame_type* has no rule.
        """
        return cls(on_mismatch, _Node.of(frame_type, "$"))

    def document(self, text: str | None) -> Any:
        """The structure of the document *text*; ``None`` for a null document.

        Raises:
            JsonRefusal: Under :attr:`JsonMismatch.FAIL`, *text* is not JSON or a value
                in it does not fit its type.
        """
        if text is None:
            return None
        refusals: list[JsonRefusal] = []
        try:
            parsed = self._parsed(text)
        except (ValueError, RecursionError):
            refusals.append(JsonRefusal("$ expected a JSON document"))
            fitted = None
        else:
            fitted = self.root.fit(parsed, refusals)
        if refusals and self.on_mismatch is JsonMismatch.FAIL:
            raise refusals[0]
        return fitted

    def _parsed(self, text: str) -> Any:
        try:
            return _DECODER.decode(text)
        except msgspec.DecodeError:
            return json.loads(
                text,
                parse_int=lambda text: msgspec.Raw("0" if text == "-0" else text),
                parse_float=msgspec.Raw,
                parse_constant=lambda _token: _NON_JSON,
            )
