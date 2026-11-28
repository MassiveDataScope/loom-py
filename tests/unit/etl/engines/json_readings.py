"""What a JSON value under a declared type reads as, as rows every JSON test shares.

A row's document sits under field ``v`` of a probe struct that also has a field ``w``,
so every row also shows that a misfit nulls only its own value.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from math import inf, nan
from operator import attrgetter
from typing import Any

from pytest import approx

from loom.etl.schema import (
    ArrayType,
    DatetimeType,
    DecimalType,
    EnumType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)

NAME = attrgetter("name")
SIBLING = 9


@dataclass(frozen=True)
class Fit:
    """A document both policies read as *value*."""

    name: str
    frame_type: FrameType
    document: str
    value: Any


@dataclass(frozen=True)
class Unfit:
    """A document ``NULL`` reads as *null* and ``FAIL`` refuses at *path*, naming *rule*."""

    name: str
    frame_type: FrameType
    document: str
    rule: str
    null: Any = None
    path: str = "$.v"


def probe(frame_type: FrameType) -> StructType:
    """The struct holding *frame_type* under ``v`` beside the integer ``w``."""
    return StructType((StructField("v", frame_type), StructField("w", LoomDtype.INT64)))


def probe_document(row: Fit | Unfit) -> str:
    """The JSON document of the probe struct whose ``v`` is the document of *row*."""
    return f'{{"v": {row.document}, "w": {SIBLING}}}'


PRICE = DecimalType(10, 2)
SMALL_PRICE = DecimalType(4, 2)
WIDE_PRICE = DecimalType(20, 2)
NINES = DecimalType(38, 0)
NINES_NUMBER = "99999999999999999999999999999999999999"
NINES_TEXT = '"99999999999999999999999999999999999999"'
UTC_TYPE = DatetimeType("us", "UTC")
MADRID = DatetimeType("us", "Europe/Madrid")
NAIVE = DatetimeType("us")
MILLIS = DatetimeType("ms", "UTC")
NANOS = DatetimeType("ns")
DAY = date(2026, 11, 28)
INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
MIDNIGHT = datetime(2026, 11, 28, tzinfo=UTC)
MICROS = datetime(2026, 11, 28, 10, 0, 0, 123456, tzinfo=UTC)
MILLIS_FRACTION = datetime(2026, 11, 28, 10, 0, 0, 123000, tzinfo=UTC)
HALF_SECOND = datetime(2026, 11, 28, 10, 0, 0, 500000, tzinfo=UTC)
NANOS_EARLIEST = datetime(1677, 9, 21, 0, 12, 43, 145225)
NANOS_LATEST = datetime(2262, 4, 11, 23, 47, 16, 854775)
FLOAT32_MAXIMUM = 3.4028234663852886e38
NAIVE_INSTANT = datetime(2026, 11, 28, 10)
LONG_INTEGER = "1" + "0" * 500
HUGE_INTEGER = "1" + "0" * 5000
PAIR = StructType((StructField("a", LoomDtype.INT64), StructField("b", LoomDtype.INT64)))
INT_RULE = "an integer from -9223372036854775808 to 9223372036854775807"
SMALL_DECIMAL_RULE = "a decimal of 4 digits, 2 after the point"
DECIMAL_RULE = "a decimal of 10 digits, 2 after the point"
DATE_RULE = "a date as YYYY-MM-DD, or an ISO datetime"
INSTANT_RULE = "an ISO datetime, or a date"
FLOAT64_RULE = "a finite number of at most 1.7976931348623157e+308"
FLOAT32_RULE = "a finite number of at most 3.4028235677973362e+38"
HELD_RULE = "an instant its timestamp type holds"
TEXT_RULE = "a JSON value whose text is UTF-8"
LEVEL = EnumType(("low", "high"))
AT = StructType((StructField("at", UTC_TYPE),))

COMMON_FITS = [
    Fit("int8-maximum", LoomDtype.INT8, "127", 127),
    Fit("int8-minimum", LoomDtype.INT8, "-128", -128),
    Fit("uint8-maximum", LoomDtype.UINT8, "255", 255),
    Fit("unsigned-minimum", LoomDtype.UINT8, "0", 0),
    Fit("int64-minimum", LoomDtype.INT64, "-9223372036854775808", -9223372036854775808),
    Fit("int64-maximum", LoomDtype.INT64, "9223372036854775807", 9223372036854775807),
    Fit("float-from-int", LoomDtype.FLOAT64, "2", 2.0),
    Fit("bool", LoomDtype.BOOLEAN, "true", True),
    Fit("str-from-bool", LoomDtype.UTF8, "true", "true"),
    Fit("str-from-object", LoomDtype.UTF8, '{"a": 1}', '{"a":1}'),
    Fit("decimal-number", PRICE, "12.5", Decimal("12.50")),
    Fit("decimal-rounds-beyond-its-scale", PRICE, "12.555", Decimal("12.56")),
    Fit("decimal-at-its-precision", PRICE, "99999999.99", Decimal("99999999.99")),
    Fit("decimal-text", PRICE, '"12.5"', Decimal("12.50")),
    Fit("decimal-signed-text", PRICE, '"+5"', Decimal("5.00")),
    Fit("decimal-exponent-text", PRICE, '"1e3"', Decimal("1000.00")),
    Fit(
        "decimal-keeps-every-digit",
        WIDE_PRICE,
        "123456789012345678.91",
        Decimal("123456789012345678.91"),
    ),
    Fit("decimal-38-nines", NINES, NINES_NUMBER, Decimal(NINES_NUMBER)),
    Fit("decimal-38-nines-text", NINES, NINES_TEXT, Decimal(NINES_NUMBER)),
    Fit("date", LoomDtype.DATE, '"2026-11-28"', DAY),
    Fit("date-from-datetime", LoomDtype.DATE, '"2026-11-28T10:00:00"', DAY),
    Fit("date-from-spaced-datetime", LoomDtype.DATE, '"2026-11-28 10:00:00"', DAY),
    Fit("date-from-datetime-with-offset", LoomDtype.DATE, '"2026-11-28T23:00:00-05:00"', DAY),
    Fit("instant-with-z", UTC_TYPE, '"2026-11-28T10:00:00Z"', INSTANT),
    Fit("instant-without-offset", UTC_TYPE, '"2026-11-28T10:00:00"', INSTANT),
    Fit("instant-with-space", UTC_TYPE, '"2026-11-28 10:00:00"', INSTANT),
    Fit("instant-with-offset", UTC_TYPE, '"2026-11-28T12:00:00+02:00"', INSTANT),
    Fit("instant-from-date", UTC_TYPE, '"2026-11-28"', MIDNIGHT),
    Fit("instant-with-fraction", UTC_TYPE, '"2026-11-28T10:00:00.123456Z"', MICROS),
    Fit("instant-in-a-named-zone", MADRID, '"2026-11-28T12:00:00+02:00"', INSTANT),
    Fit("naive-instant", NAIVE, '"2026-11-28T10:00:00"', NAIVE_INSTANT),
    Fit("struct-fields", PAIR, '{"a": 1, "b": 2}', {"a": 1, "b": 2}),
]

COMMON_UNFITS = [
    Unfit("int8-below-its-minimum", LoomDtype.INT8, "-129", "an integer from -128 to 127"),
    Unfit("int-from-float", LoomDtype.INT64, "1.5", INT_RULE),
    Unfit("int-from-bool", LoomDtype.INT64, "true", INT_RULE),
    Unfit("int-from-string", LoomDtype.INT64, '"oops"', INT_RULE),
    Unfit("int-beyond-500-digits", LoomDtype.INT64, LONG_INTEGER, INT_RULE),
    Unfit("float-from-string", LoomDtype.FLOAT64, '"oops"', FLOAT64_RULE),
    Unfit("bool-from-number", LoomDtype.BOOLEAN, "1", "a boolean"),
    Unfit("decimal-beyond-its-precision", PRICE, "100000000", DECIMAL_RULE),
    Unfit("decimal-from-word", PRICE, '"oops"', DECIMAL_RULE),
    Unfit("decimal-underscored", PRICE, '"1_0"', DECIMAL_RULE),
    Unfit("decimal-spaced", PRICE, '" 5"', DECIMAL_RULE),
    Unfit("decimal-not-a-number", PRICE, '"NaN"', DECIMAL_RULE),
    Unfit("decimal-huge-exponent", PRICE, '"1e1000000000"', DECIMAL_RULE),
    Unfit("date-from-number", LoomDtype.DATE, "5", DATE_RULE),
    Unfit("date-from-word", LoomDtype.DATE, '"yesterday"', DATE_RULE),
    Unfit("instant-from-word", UTC_TYPE, '"yesterday"', INSTANT_RULE),
    Unfit("struct-from-list", PAIR, "[1]", "an object"),
    Unfit("list-from-object", ListType(LoomDtype.INT64), '{"a": 1}', "an array"),
]

POLARS_FITS = [
    Fit("str-keeps-the-number-text", LoomDtype.UTF8, "1.00", "1.00"),
    Fit("uint64-maximum", LoomDtype.UINT64, "18446744073709551615", 18446744073709551615),
    Fit("str-reads-an-integer-minus-zero-as-zero", LoomDtype.UTF8, "-0", "0"),
    Fit("str-keeps-an-integer-of-any-length", LoomDtype.UTF8, HUGE_INTEGER, HUGE_INTEGER),
    Fit("float-underflow-is-zero", LoomDtype.FLOAT64, "1e-400", 0.0),
    Fit("float32-maximum", LoomDtype.FLOAT32, "3.4028235e38", FLOAT32_MAXIMUM),
    Fit("float32-below-the-boundary", LoomDtype.FLOAT32, "3.4028235677973362e38", FLOAT32_MAXIMUM),
    Fit("str-keeps-the-nested-number-text", LoomDtype.UTF8, '{"a": 1.50}', '{"a":1.50}'),
    Fit("date-with-a-compact-offset", LoomDtype.DATE, '"2026-11-28T23:00:00-0500"', DAY),
    Fit("instant-with-a-compact-offset", UTC_TYPE, '"2026-11-28T12:00:00+0200"', INSTANT),
    Fit("instant-with-one-fraction-digit", UTC_TYPE, '"2026-11-28T10:00:00.5Z"', HALF_SECOND),
    Fit("instant-without-seconds", UTC_TYPE, '"2026-11-28T10:00Z"', INSTANT),
    Fit("naive-instant-with-offset", NAIVE, '"2026-11-28T12:00:00+02:00"', NAIVE_INSTANT),
    Fit("coarse-datetime", LoomDtype.DATETIME, '"2026-11-28T12:00:00+02:00"', NAIVE_INSTANT),
    Fit("nanosecond-instant", NANOS, '"2026-11-28T10:00:00"', NAIVE_INSTANT),
    Fit("nanosecond-instant-earliest", NANOS, '"1677-09-21T00:12:43.145225Z"', NANOS_EARLIEST),
    Fit("nanosecond-instant-latest", NANOS, '"2262-04-11T23:47:16.854775Z"', NANOS_LATEST),
    Fit(
        "millisecond-instant-drops-microseconds",
        MILLIS,
        '"2026-11-28T10:00:00.123456Z"',
        MILLIS_FRACTION,
    ),
    Fit("enum-member", LEVEL, '"low"', "low"),
    Fit("array-of-dates", ArrayType(LoomDtype.DATE, 2), '["2026-11-28", "2026-11-28"]', [DAY, DAY]),
    Fit("instant-in-a-struct", AT, '{"at": "2026-11-28 10:00:00"}', {"at": INSTANT}),
]

POLARS_UNFITS = [
    Unfit("uint8-above-its-maximum", LoomDtype.UINT8, "256", "an integer from 0 to 255"),
    Unfit("int8-above-its-maximum", LoomDtype.INT8, "128", "an integer from -128 to 127"),
    Unfit("unsigned-negative", LoomDtype.UINT8, "-1", "an integer from 0 to 255"),
    Unfit("float32-beyond-its-maximum", LoomDtype.FLOAT32, "1e300", FLOAT32_RULE),
    Unfit("float32-at-the-boundary", LoomDtype.FLOAT32, "3.4028235677973366e38", FLOAT32_RULE),
    Unfit("decimal-rounding-past-its-precision", SMALL_PRICE, "99.995", SMALL_DECIMAL_RULE),
    Unfit("nan", LoomDtype.FLOAT64, "NaN", FLOAT64_RULE),
    Unfit("infinity", LoomDtype.FLOAT64, "Infinity", FLOAT64_RULE),
    Unfit("int-beyond-4300-digits", LoomDtype.INT64, HUGE_INTEGER, INT_RULE),
    Unfit("str-from-a-lone-surrogate", LoomDtype.UTF8, '"\\ud800"', TEXT_RULE),
    Unfit("str-from-nan", LoomDtype.UTF8, "NaN", TEXT_RULE),
    Unfit("str-from-a-nested-infinity", LoomDtype.UTF8, '{"a": -Infinity}', TEXT_RULE),
    Unfit("decimal-thousands", PRICE, '"1,000"', DECIMAL_RULE),
    Unfit(
        "decimal-number-exponent-beyond-any-context",
        PRICE,
        "1e99999999999999999999999",
        DECIMAL_RULE,
    ),
    Unfit("date-at-hour-24", LoomDtype.DATE, '"2026-11-28T24:00:00"', DATE_RULE),
    Unfit("date-with-an-invalid-time", LoomDtype.DATE, '"2026-11-28 99:99:99"', DATE_RULE),
    Unfit("date-with-a-garbage-suffix", LoomDtype.DATE, '"2026-11-28Tgarbage"', DATE_RULE),
    Unfit("date-that-does-not-exist", LoomDtype.DATE, '"2026-02-30"', DATE_RULE),
    Unfit("instant-with-seven-digits", UTC_TYPE, '"2026-11-28T10:00:00.1234567Z"', INSTANT_RULE),
    Unfit("instant-with-an-hour-only-offset", UTC_TYPE, '"2026-11-28T10:00:00+02"', INSTANT_RULE),
    Unfit("instant-from-number", UTC_TYPE, "5", INSTANT_RULE),
    Unfit("instant-before-the-year-1", UTC_TYPE, '"0001-01-01T00:00:00+02:00"', HELD_RULE),
    Unfit("nanosecond-instant-beyond-its-range", NANOS, '"9999-12-31T23:59:59Z"', HELD_RULE),
    Unfit(
        "nanosecond-instant-before-its-earliest", NANOS, '"1677-09-21T00:12:43.145224Z"', HELD_RULE
    ),
    Unfit("nanosecond-instant-after-its-latest", NANOS, '"2262-04-11T23:47:16.854776Z"', HELD_RULE),
    Unfit("enum-unknown-member", LEVEL, '"medium"', "a member of the enum"),
    Unfit("enum-from-number", LEVEL, "1", "a member of the enum"),
    Unfit("array-too-short", ArrayType(LoomDtype.DATE, 2), '["2026-11-28"]', "an array of 2 items"),
    Unfit("array-too-long", ArrayType(LoomDtype.INT64, 2), "[1, 2, 3]", "an array of 2 items"),
    Unfit("list-element", ListType(LoomDtype.INT64), '[1, "x"]', INT_RULE, [1, None], "$.v[]"),
    Unfit("struct-field", PAIR, '{"a": 1, "b": "x"}', INT_RULE, {"a": 1, "b": None}, "$.v.b"),
]

SPARK_FITS = [
    Fit("int8-above-its-maximum", LoomDtype.INT8, "128", -128),
    Fit("uint8-above-its-maximum", LoomDtype.UINT8, "256", 256),
    Fit("unsigned-negative", LoomDtype.UINT8, "-1", -1),
    Fit("float32-beyond-its-maximum", LoomDtype.FLOAT32, "1e300", inf),
    Fit("nan", LoomDtype.FLOAT64, "NaN", approx(nan, nan_ok=True)),
    Fit("infinity", LoomDtype.FLOAT64, "Infinity", inf),
    Fit("decimal-thousands", PRICE, '"1,000"', Decimal("1000.00")),
    Fit("date-with-a-garbage-suffix", LoomDtype.DATE, '"2026-11-28Tgarbage"', DAY),
    Fit("date-with-an-invalid-time", LoomDtype.DATE, '"2026-11-28 99:99:99"', DAY),
    Fit("instant-from-number", UTC_TYPE, "5", datetime(1970, 1, 1, 0, 0, 5, tzinfo=UTC)),
]

SPARK_UNFITS = [
    Unfit("naive-instant-with-offset", NAIVE, '"2026-11-28T12:00:00+02:00"', INSTANT_RULE),
    Unfit("list-element", ListType(LoomDtype.INT64), '[1, "x"]', INT_RULE, None, "$.v[]"),
]
