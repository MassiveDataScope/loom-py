"""A JSON column decodes by one rule table under two policies, on every engine."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from math import inf, nan
from typing import Any

import polars as pl
import pytest
from pytest import approx

from loom.core.errors import InvalidValue
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import JsonMismatch
from loom.etl.executor import ETLExecutor
from loom.etl.schema import (
    DatetimeType,
    DecimalType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.engines.doubles import ModelEngine, NoParams, PolarsEngine
from tests.unit.etl.models import Buyer

_POLARS = "polars_engine"
_SPARK = "spark_engine"
_NAMED = r"buyer as StructType"
_NAN = approx(nan, nan_ok=True)


class _Refused:
    def __repr__(self) -> str:
        return "REFUSED"


REFUSED = _Refused()


@dataclass(frozen=True)
class Reading:
    """What a JSON value under a frame type reads as under each policy, on the engines it names."""

    frame_type: FrameType
    document: str
    null: Any
    fail: Any
    engines: tuple[str, ...] = (_POLARS, _SPARK)


def agreed(frame_type: FrameType, document: str, value: Any) -> Reading:
    """A reading both policies give as *value*, on both engines."""
    return Reading(frame_type, document, value, value)


def misfit(frame_type: FrameType, document: str) -> Reading:
    """A reading where NULL nulls the value and FAIL refuses it, on both engines."""
    return Reading(frame_type, document, None, REFUSED)


def on_polars(frame_type: FrameType, document: str, null: Any, fail: Any) -> Reading:
    """A reading that holds on Polars only."""
    return Reading(frame_type, document, null, fail, (_POLARS,))


def on_spark(frame_type: FrameType, document: str, null: Any, fail: Any) -> Reading:
    """A reading that holds on Spark only."""
    return Reading(frame_type, document, null, fail, (_SPARK,))


_PRICE = DecimalType(10, 2)
_NINES = DecimalType(38, 0)
_UTC = DatetimeType("us", "UTC")
_MADRID = DatetimeType("us", "Europe/Madrid")
_NAIVE = DatetimeType("us")
_DAY = date(2026, 11, 28)
_INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
_NAIVE_INSTANT = datetime(2026, 11, 28, 10)
_EPOCH_PLUS_FIVE = datetime(1970, 1, 1, 0, 0, 5, tzinfo=UTC)
_NINES_VALUE = Decimal("9" * 38)
_AB = StructType((StructField("a", LoomDtype.INT64), StructField("b", LoomDtype.INT64)))

_READINGS = {
    "int-in-range": agreed(LoomDtype.INT8, "5", 5),
    "int-over-range": misfit(LoomDtype.INT8, "300"),
    "int-under-range": misfit(LoomDtype.INT8, "-300"),
    "int-from-float": misfit(LoomDtype.INT64, "1.5"),
    "int-from-bool": misfit(LoomDtype.INT64, "true"),
    "int-from-string": misfit(LoomDtype.INT64, '"oops"'),
    "int-beyond-500-digits": misfit(LoomDtype.INT64, "1" + "0" * 500),
    "unsigned-negative-polars": on_polars(LoomDtype.UINT8, "-1", None, REFUSED),
    "unsigned-negative-spark": on_spark(LoomDtype.UINT8, "-1", -1, -1),
    "float-from-int": agreed(LoomDtype.FLOAT64, "2", 2.0),
    "float-from-string": misfit(LoomDtype.FLOAT64, '"oops"'),
    "float32-beyond-polars": on_polars(LoomDtype.FLOAT32, "1e300", None, REFUSED),
    "float32-beyond-spark": on_spark(LoomDtype.FLOAT32, "1e300", inf, inf),
    "nan-polars": on_polars(LoomDtype.FLOAT64, "NaN", None, REFUSED),
    "nan-spark": on_spark(LoomDtype.FLOAT64, "NaN", _NAN, _NAN),
    "infinity-polars": on_polars(LoomDtype.FLOAT64, "Infinity", None, REFUSED),
    "infinity-spark": on_spark(LoomDtype.FLOAT64, "Infinity", inf, inf),
    "bool": agreed(LoomDtype.BOOLEAN, "true", True),
    "bool-from-number": misfit(LoomDtype.BOOLEAN, "1"),
    "str-from-bool": agreed(LoomDtype.UTF8, "true", "true"),
    "str-from-object": agreed(LoomDtype.UTF8, '{"a": 1}', '{"a":1}'),
    "decimal-number": agreed(_PRICE, "12.5", Decimal("12.50")),
    "decimal-rounds-beyond-its-scale": agreed(_PRICE, "12.555", Decimal("12.56")),
    "decimal-text": agreed(_PRICE, '"12.5"', Decimal("12.50")),
    "decimal-signed-text": agreed(_PRICE, '"+5"', Decimal("5.00")),
    "decimal-exponent-text": agreed(_PRICE, '"1e3"', Decimal("1000.00")),
    "decimal-38-nines": agreed(_NINES, "9" * 38, _NINES_VALUE),
    "decimal-38-nines-text": agreed(_NINES, f'"{"9" * 38}"', _NINES_VALUE),
    "decimal-overflowing-precision": misfit(_PRICE, "99999999999"),
    "decimal-from-word": misfit(_PRICE, '"oops"'),
    "decimal-underscored": misfit(_PRICE, '"1_0"'),
    "decimal-spaced": misfit(_PRICE, '" 5"'),
    "decimal-not-a-number": misfit(_PRICE, '"NaN"'),
    "decimal-huge-exponent": misfit(_PRICE, '"1e1000000000"'),
    "decimal-thousands-polars": on_polars(_PRICE, '"1,000"', None, REFUSED),
    "decimal-thousands-spark": on_spark(_PRICE, '"1,000"', Decimal("1000.00"), Decimal("1000.00")),
    "date": agreed(LoomDtype.DATE, '"2026-11-28"', _DAY),
    "date-from-datetime": agreed(LoomDtype.DATE, '"2026-11-28T10:00:00"', _DAY),
    "date-from-spaced-datetime": agreed(LoomDtype.DATE, '"2026-11-28 10:00:00"', _DAY),
    "date-from-datetime-with-offset": agreed(LoomDtype.DATE, '"2026-11-28T23:00:00-05:00"', _DAY),
    "date-from-number": misfit(LoomDtype.DATE, "5"),
    "date-from-word": misfit(LoomDtype.DATE, '"yesterday"'),
    "date-garbage-suffix-polars": on_polars(LoomDtype.DATE, '"2026-11-28Tgarbage"', None, REFUSED),
    "date-garbage-suffix-spark": on_spark(LoomDtype.DATE, '"2026-11-28Tgarbage"', _DAY, _DAY),
    "date-invalid-time-polars": on_polars(LoomDtype.DATE, '"2026-11-28 99:99:99"', None, REFUSED),
    "date-invalid-time-spark": on_spark(LoomDtype.DATE, '"2026-11-28 99:99:99"', _DAY, _DAY),
    "instant-with-z": agreed(_UTC, '"2026-11-28T10:00:00Z"', _INSTANT),
    "instant-without-offset": agreed(_UTC, '"2026-11-28T10:00:00"', _INSTANT),
    "instant-with-space": agreed(_UTC, '"2026-11-28 10:00:00"', _INSTANT),
    "instant-with-offset": agreed(_UTC, '"2026-11-28T12:00:00+02:00"', _INSTANT),
    "instant-from-date": agreed(_UTC, '"2026-11-28"', _INSTANT.replace(hour=0)),
    "instant-with-fraction": agreed(
        _UTC, '"2026-11-28T10:00:00.123456Z"', _INSTANT.replace(microsecond=123456)
    ),
    "instant-in-a-named-zone": agreed(_MADRID, '"2026-11-28T12:00:00+02:00"', _INSTANT),
    "instant-from-word": misfit(_UTC, '"yesterday"'),
    "instant-from-number-polars": on_polars(_UTC, "5", None, REFUSED),
    "instant-from-number-spark": on_spark(_UTC, "5", _EPOCH_PLUS_FIVE, _EPOCH_PLUS_FIVE),
    "naive-instant": agreed(_NAIVE, '"2026-11-28T10:00:00"', _NAIVE_INSTANT),
    "naive-instant-with-offset-polars": on_polars(
        _NAIVE, '"2026-11-28T12:00:00+02:00"', _NAIVE_INSTANT, _NAIVE_INSTANT
    ),
    "naive-instant-with-offset-spark": on_spark(
        _NAIVE, '"2026-11-28T12:00:00+02:00"', None, REFUSED
    ),
    "struct-from-list": misfit(_AB, "[1]"),
    "struct-field-mismatch": agreed(_AB, '{"a": 1, "b": 2}', {"a": 1, "b": 2}),
    "list-from-object": misfit(ListType(LoomDtype.INT64), '{"a": 1}'),
    "list-element-polars": on_polars(ListType(LoomDtype.INT64), '[1, "x"]', [1, None], REFUSED),
    "list-element-spark": on_spark(ListType(LoomDtype.INT64), '[1, "x"]', None, REFUSED),
}


def _params(readings: Iterable[tuple[str, Reading]]) -> list[Any]:
    return [
        pytest.param(engine, reading, id=f"{name}-{engine.removesuffix('_engine')}")
        for name, reading in readings
        for engine in reading.engines
    ]


_ALL = _params(_READINGS.items())
_FITTING = _params((name, r) for name, r in _READINGS.items() if r.fail is not REFUSED)
_REFUSED = _params((name, r) for name, r in _READINGS.items() if r.fail is REFUSED)


def _probe(frame_type: FrameType) -> StructType:
    return StructType((StructField("v", frame_type), StructField("w", LoomDtype.INT64)))


def _document(reading: Reading) -> str:
    return f'{{"v": {reading.document}, "w": 9}}'


def _seed_customers(engine: ModelEngine, *documents: str) -> None:
    engine.writer.write(
        engine.frame({"buyer": list(documents)}),
        IntoTable("raw.customers").append()._to_spec(),
        None,
    )


def _step_decoding(contract: FrameType | type) -> type[ETLStep[NoParams]]:
    class _Decode(ETLStep[NoParams]):
        customers: FromTable = FromTable("raw.customers").parse_json("buyer", contract)  # type: ignore[assignment]
        target = IntoTable("staging.customers").replace()

        def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
            return customers

    return _Decode


def _run(engine: ModelEngine, step: type[ETLStep[Any]]) -> None:
    ETLExecutor(engine.reader, engine.writer).run_step(ETLCompiler().compile_step(step), NoParams())


def decode(
    engine: ModelEngine, contract: FrameType, on_mismatch: JsonMismatch, *documents: str
) -> list[Any]:
    """The rows *documents* decode to under *contract*, read and written back by *engine*."""
    _seed_customers(engine, *documents)
    source = FromTable("raw.customers").parse_json("buyer", contract, on_mismatch=on_mismatch)
    decoded = engine.reader.read(source._to_spec("customers"), None)
    engine.writer.write(decoded, IntoTable("staging.customers").replace()._to_spec(), None)
    return [row["buyer"] for row in engine.stored("staging.customers")]


class TestJsonFitParity:
    @pytest.mark.parametrize(("engine_name", "reading"), _ALL)
    def test_null_policy_gives_the_value_and_keeps_its_sibling(
        self, request: pytest.FixtureRequest, engine_name: str, reading: Reading
    ) -> None:
        engine: ModelEngine = request.getfixturevalue(engine_name)

        (buyer,) = decode(engine, _probe(reading.frame_type), JsonMismatch.NULL, _document(reading))

        assert buyer == {"v": reading.null, "w": 9}

    @pytest.mark.parametrize(("engine_name", "reading"), _FITTING)
    def test_fail_policy_reads_what_fits(
        self, request: pytest.FixtureRequest, engine_name: str, reading: Reading
    ) -> None:
        engine: ModelEngine = request.getfixturevalue(engine_name)

        (buyer,) = decode(engine, _probe(reading.frame_type), JsonMismatch.FAIL, _document(reading))

        assert buyer == {"v": reading.fail, "w": 9}

    @pytest.mark.parametrize(("engine_name", "reading"), _REFUSED)
    def test_fail_policy_refuses_naming_the_column(
        self, request: pytest.FixtureRequest, engine_name: str, reading: Reading
    ) -> None:
        engine: ModelEngine = request.getfixturevalue(engine_name)
        _seed_customers(engine, _document(reading))

        with pytest.raises(InvalidValue, match=_NAMED):
            _run(engine, _step_decoding(_probe(reading.frame_type)))


class _OwnJsonFailure(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers")
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        raise pl.exceptions.ComputeError("error deserializing JSON: json parsing error")


class _OwnJsonFailureOverDecodedColumn(_OwnJsonFailure):
    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]


class TestJsonDecodeFailure:
    @pytest.mark.parametrize(
        "document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"]
    )
    def test_a_document_that_does_not_decode_names_the_declared_column(
        self, engine: ModelEngine, document: str
    ) -> None:
        _seed_customers(engine, document)

        with pytest.raises(InvalidValue, match=_NAMED):
            _run(engine, _step_decoding(Buyer))

    @pytest.mark.parametrize(
        "step",
        [_OwnJsonFailure, _OwnJsonFailureOverDecodedColumn],
        ids=["no-json-column", "decoded-column"],
    )
    def test_a_failure_of_the_steps_own_code_is_kept(
        self, polars_engine: PolarsEngine, step: type[ETLStep[Any]]
    ) -> None:
        _seed_customers(polars_engine, '{"name": "Ada", "order_count": 1}')

        with pytest.raises(pl.exceptions.ComputeError, match="error deserializing JSON"):
            _run(polars_engine, step)
