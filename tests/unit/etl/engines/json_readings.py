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
class Misfit:
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


def probe_document(row: Fit | Misfit) -> str:
    """The JSON document of the probe struct whose ``v`` is the document of *row*."""
    return f'{{"v": {row.document}, "w": {SIBLING}}}'


PRICE = DecimalType(10, 2)
WIDE_PRICE = DecimalType(20, 2)
NINES = DecimalType(38, 0)
UTC_TYPE = DatetimeType("us", "UTC")
MADRID = DatetimeType("us", "Europe/Madrid")
NAIVE = DatetimeType("us")
DAY = date(2026, 11, 28)
INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
PAIR = StructType((StructField("a", LoomDtype.INT64), StructField("b", LoomDtype.INT64)))
INT_RULE = "an integer from -9223372036854775808 to 9223372036854775807"
DECIMAL_RULE = "a decimal of 10 digits, 2 after the point"
DATE_RULE = "a date as YYYY-MM-DD, or an ISO datetime"
INSTANT_RULE = "an ISO datetime, or a date"
FLOAT64_RULE = "a finite number of at most 1.7976931348623157e+308"
FLOAT32_RULE = "a finite number of at most 3.4028234663852886e+38"
LEVEL = EnumType(("low", "high"))
AT = StructType((StructField("at", UTC_TYPE),))

COMMON_FITS = [
    Fit("int8-maximum", LoomDtype.INT8, "127", 127),
    Fit("int8-minimum", LoomDtype.INT8, "-128", -128),
    Fit("uint8-maximum", LoomDtype.UINT8, "255", 255),
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
    Fit("decimal-38-nines", NINES, "9" * 38, Decimal("9" * 38)),
    Fit("decimal-38-nines-text", NINES, f'"{"9" * 38}"', Decimal("9" * 38)),
    Fit("date", LoomDtype.DATE, '"2026-11-28"', DAY),
    Fit("date-from-datetime", LoomDtype.DATE, '"2026-11-28T10:00:00"', DAY),
    Fit("date-from-spaced-datetime", LoomDtype.DATE, '"2026-11-28 10:00:00"', DAY),
    Fit("date-from-datetime-with-offset", LoomDtype.DATE, '"2026-11-28T23:00:00-05:00"', DAY),
    Fit("instant-with-z", UTC_TYPE, '"2026-11-28T10:00:00Z"', INSTANT),
    Fit("instant-without-offset", UTC_TYPE, '"2026-11-28T10:00:00"', INSTANT),
    Fit("instant-with-space", UTC_TYPE, '"2026-11-28 10:00:00"', INSTANT),
    Fit("instant-with-offset", UTC_TYPE, '"2026-11-28T12:00:00+02:00"', INSTANT),
    Fit("instant-from-date", UTC_TYPE, '"2026-11-28"', INSTANT.replace(hour=0)),
    Fit(
        "instant-with-fraction",
        UTC_TYPE,
        '"2026-11-28T10:00:00.123456Z"',
        INSTANT.replace(microsecond=123456),
    ),
    Fit("instant-in-a-named-zone", MADRID, '"2026-11-28T12:00:00+02:00"', INSTANT),
    Fit("naive-instant", NAIVE, '"2026-11-28T10:00:00"', INSTANT.replace(tzinfo=None)),
    Fit("struct-fields", PAIR, '{"a": 1, "b": 2}', {"a": 1, "b": 2}),
]

COMMON_MISFITS = [
    Misfit("int8-below-its-minimum", LoomDtype.INT8, "-129", "an integer from -128 to 127"),
    Misfit("int-from-float", LoomDtype.INT64, "1.5", INT_RULE),
    Misfit("int-from-bool", LoomDtype.INT64, "true", INT_RULE),
    Misfit("int-from-string", LoomDtype.INT64, '"oops"', INT_RULE),
    Misfit("int-beyond-500-digits", LoomDtype.INT64, "1" + "0" * 500, INT_RULE),
    Misfit("float-from-string", LoomDtype.FLOAT64, '"oops"', FLOAT64_RULE),
    Misfit("bool-from-number", LoomDtype.BOOLEAN, "1", "a boolean"),
    Misfit("decimal-beyond-its-precision", PRICE, "100000000", DECIMAL_RULE),
    Misfit("decimal-from-word", PRICE, '"oops"', DECIMAL_RULE),
    Misfit("decimal-underscored", PRICE, '"1_0"', DECIMAL_RULE),
    Misfit("decimal-spaced", PRICE, '" 5"', DECIMAL_RULE),
    Misfit("decimal-not-a-number", PRICE, '"NaN"', DECIMAL_RULE),
    Misfit("decimal-huge-exponent", PRICE, '"1e1000000000"', DECIMAL_RULE),
    Misfit("date-from-number", LoomDtype.DATE, "5", DATE_RULE),
    Misfit("date-from-word", LoomDtype.DATE, '"yesterday"', DATE_RULE),
    Misfit("instant-from-word", UTC_TYPE, '"yesterday"', INSTANT_RULE),
    Misfit("struct-from-list", PAIR, "[1]", "an object"),
    Misfit("list-from-object", ListType(LoomDtype.INT64), '{"a": 1}', "an array"),
]

POLARS_FITS = [
    Fit("str-keeps-the-number-text", LoomDtype.UTF8, "1.00", "1.00"),
    Fit("unsigned-minimum", LoomDtype.UINT8, "0", 0),
    Fit("str-keeps-the-nested-number-text", LoomDtype.UTF8, '{"a": 1.50}', '{"a":1.50}'),
    Fit("date-with-a-compact-offset", LoomDtype.DATE, '"2026-11-28T23:00:00-0500"', DAY),
    Fit("instant-with-a-compact-offset", UTC_TYPE, '"2026-11-28T12:00:00+0200"', INSTANT),
    Fit(
        "instant-with-one-fraction-digit",
        UTC_TYPE,
        '"2026-11-28T10:00:00.5Z"',
        INSTANT.replace(microsecond=500000),
    ),
    Fit("instant-without-seconds", UTC_TYPE, '"2026-11-28T10:00Z"', INSTANT),
    Fit(
        "naive-instant-with-offset",
        NAIVE,
        '"2026-11-28T12:00:00+02:00"',
        INSTANT.replace(tzinfo=None),
    ),
    Fit(
        "nanosecond-instant",
        DatetimeType("ns"),
        '"2026-11-28T10:00:00"',
        INSTANT.replace(tzinfo=None),
    ),
    Fit("enum-member", LEVEL, '"low"', "low"),
    Fit("array-of-dates", ArrayType(LoomDtype.DATE, 2), '["2026-11-28", "2026-11-28"]', [DAY, DAY]),
    Fit("instant-in-a-struct", AT, '{"at": "2026-11-28 10:00:00"}', {"at": INSTANT}),
]

POLARS_MISFITS = [
    Misfit("uint8-above-its-maximum", LoomDtype.UINT8, "256", "an integer from 0 to 255"),
    Misfit("int8-above-its-maximum", LoomDtype.INT8, "128", "an integer from -128 to 127"),
    Misfit("unsigned-negative", LoomDtype.UINT8, "-1", "an integer from 0 to 255"),
    Misfit("float32-beyond-its-maximum", LoomDtype.FLOAT32, "1e300", FLOAT32_RULE),
    Misfit("nan", LoomDtype.FLOAT64, "NaN", FLOAT64_RULE),
    Misfit("infinity", LoomDtype.FLOAT64, "Infinity", FLOAT64_RULE),
    Misfit("int-beyond-4300-digits", LoomDtype.INT64, "1" + "0" * 5000, INT_RULE),
    Misfit(
        "str-from-a-lone-surrogate",
        LoomDtype.UTF8,
        '"\\ud800"',
        "a value whose text encodes as UTF-8",
    ),
    Misfit("decimal-thousands", PRICE, '"1,000"', DECIMAL_RULE),
    Misfit(
        "decimal-exponent-beyond-any-context", PRICE, '"1e99999999999999999999999"', DECIMAL_RULE
    ),
    Misfit(
        "decimal-negative-exponent-beyond-any-context",
        PRICE,
        '"1e-99999999999999999999999"',
        DECIMAL_RULE,
    ),
    Misfit(
        "decimal-number-exponent-beyond-any-context",
        PRICE,
        "1e99999999999999999999999",
        DECIMAL_RULE,
    ),
    Misfit("date-at-hour-24", LoomDtype.DATE, '"2026-11-28T24:00:00"', DATE_RULE),
    Misfit("date-with-an-invalid-time", LoomDtype.DATE, '"2026-11-28 99:99:99"', DATE_RULE),
    Misfit("date-with-a-garbage-suffix", LoomDtype.DATE, '"2026-11-28Tgarbage"', DATE_RULE),
    Misfit("date-that-does-not-exist", LoomDtype.DATE, '"2026-02-30"', DATE_RULE),
    Misfit(
        "instant-with-seven-fraction-digits",
        UTC_TYPE,
        '"2026-11-28T10:00:00.1234567Z"',
        INSTANT_RULE,
    ),
    Misfit("instant-with-an-hour-only-offset", UTC_TYPE, '"2026-11-28T10:00:00+02"', INSTANT_RULE),
    Misfit("instant-from-number", UTC_TYPE, "5", INSTANT_RULE),
    Misfit(
        "instant-before-the-year-1",
        UTC_TYPE,
        '"0001-01-01T00:00:00+02:00"',
        "an instant between the years 1 and 9999",
    ),
    Misfit(
        "nanosecond-instant-beyond-its-range",
        DatetimeType("ns"),
        '"9999-12-31T23:59:59Z"',
        "an instant its timestamp type holds",
    ),
    Misfit("enum-unknown-member", LEVEL, '"medium"', "a member of the enum"),
    Misfit("enum-from-number", LEVEL, "1", "a member of the enum"),
    Misfit(
        "array-too-short", ArrayType(LoomDtype.DATE, 2), '["2026-11-28"]', "an array of 2 items"
    ),
    Misfit("array-too-long", ArrayType(LoomDtype.INT64, 2), "[1, 2, 3]", "an array of 2 items"),
    Misfit("list-element", ListType(LoomDtype.INT64), '[1, "x"]', INT_RULE, [1, None], "$.v[]"),
    Misfit("struct-field", PAIR, '{"a": 1, "b": "x"}', INT_RULE, {"a": 1, "b": None}, "$.v.b"),
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

SPARK_MISFITS = [
    Misfit("naive-instant-with-offset", NAIVE, '"2026-11-28T12:00:00+02:00"', INSTANT_RULE),
    Misfit("list-element", ListType(LoomDtype.INT64), '[1, "x"]', INT_RULE, None, "$.v[]"),
]
