"""A JSON column that does not decode fails naming the declared column, on every engine."""

from __future__ import annotations

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
from loom.etl.engines.polars import decode_json_columns
from loom.etl.engines.polars._json_text import JsonTextDecoder
from loom.etl.executor import ETLExecutor
from loom.etl.schema import (
    ArrayType,
    DatetimeType,
    DecimalType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.engines.doubles import (
    ModelEngine,
    NoParams,
    PolarsEngine,
    SparkEngine,
)
from tests.unit.etl.models import Buyer

_IN = "\n\nThis error occurred in the following expression:\n\t"
_AT_DECODE = _IN + 'col("buyer").str.json_decode()'
_AT_CAST = _IN + 'col("note").strict_cast(Date)'
_TO_DATE = "conversion from `str` to `date` failed in column 'v'"
_TO_INTEGER = "conversion from `str` to `i64` failed in column 'v'"
_WIDTH = "not all elements have the specified width 2"
_RECOGNISED = {
    "json-document": (pl.exceptions.ComputeError("error deserializing JSON: parsing"), True),
    "parquet-metadata": (pl.exceptions.ComputeError("error deserializing parquet"), False),
    "not-a-polars-error": (ValueError("error deserializing JSON"), False),
    "date-of-a-decoded-column": (pl.exceptions.InvalidOperationError(_TO_DATE + _AT_DECODE), True),
    "width-of-a-decoded-column": (pl.exceptions.ComputeError(_WIDTH + _AT_DECODE), True),
    "date-of-a-step-cast": (pl.exceptions.InvalidOperationError(_TO_DATE + _AT_CAST), False),
    "integer-of-a-decoded-column": (
        pl.exceptions.InvalidOperationError(_TO_INTEGER + _AT_DECODE),
        False,
    ),
}


class _OwnJsonFailure(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers")
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        raise pl.exceptions.ComputeError("error deserializing JSON: json parsing error")


class _OwnJsonFailureOverTolerantColumn(_OwnJsonFailure):
    customers: FromTable = FromTable("raw.customers").parse_json(  # type: ignore[assignment]
        "buyer", Buyer, on_mismatch=JsonMismatch.NULL
    )


def _run(engine: ModelEngine, step: type[ETLStep[Any]]) -> None:
    ETLExecutor(engine.reader, engine.writer).run_step(ETLCompiler().compile_step(step), NoParams())


def _seed_customers(engine: ModelEngine, *documents: str) -> None:
    engine.writer.write(
        engine.frame({"buyer": list(documents)}),
        IntoTable("raw.customers").append()._to_spec(),
        None,
    )


class TestJsonDecodeFailure:
    @pytest.mark.parametrize(
        "document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"]
    )
    def test_a_document_that_does_not_decode_names_the_declared_column(
        self, engine: ModelEngine, document: str
    ) -> None:
        _seed_customers(engine, document)

        with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
            _run(engine, _step_decoding(Buyer))

    @pytest.mark.parametrize(
        "step",
        [_OwnJsonFailure, _OwnJsonFailureOverTolerantColumn],
        ids=["no-json-column", "only-null-mode-column"],
    )
    def test_a_step_with_no_json_column_that_can_fail_keeps_the_failure_of_its_own_code(
        self, polars_engine: PolarsEngine, step: type[ETLStep[Any]]
    ) -> None:
        _seed_customers(polars_engine, "{}")

        with pytest.raises(pl.exceptions.ComputeError):
            _run(polars_engine, step)

    @pytest.mark.parametrize(("error", "recognised"), _RECOGNISED.values(), ids=list(_RECOGNISED))
    def test_polars_recognises_only_a_json_document_refusal(
        self, polars_engine: PolarsEngine, error: Exception, recognised: bool
    ) -> None:
        assert polars_engine.reader.is_json_decode_failure(error) is recognised


_PRICE = DecimalType(10, 2)
_UTC = DatetimeType("us", "UTC")
_NAIVE = DatetimeType("us")


def _probe(frame_type: FrameType) -> StructType:
    return StructType((StructField("v", frame_type), StructField("w", LoomDtype.INT64)))


def _decode(
    engine: ModelEngine, contract: FrameType, on_mismatch: JsonMismatch, *documents: str
) -> list[Any]:
    _seed_customers(engine, *documents)
    source = FromTable("raw.customers").parse_json("buyer", contract, on_mismatch=on_mismatch)
    decoded = engine.reader.read(source._to_spec("customers"), None)
    engine.writer.write(decoded, IntoTable("staging.customers").replace()._to_spec(), None)
    return [row["buyer"] for row in engine.stored("staging.customers")]


def _decode_field(
    engine: ModelEngine, frame_type: FrameType, document: str, on_mismatch: JsonMismatch
) -> Any:
    (decoded,) = _decode(engine, _probe(frame_type), on_mismatch, f'{{"v": {document}, "w": 9}}')
    return decoded["v"]


_NULLED = {
    "int-from-string": (LoomDtype.INT64, '"oops"'),
    "int-from-float": (LoomDtype.INT64, "1.5"),
    "int-from-bool": (LoomDtype.INT64, "true"),
    "int-beyond-500-digits": (LoomDtype.INT64, "1" + "0" * 500),
    "int-beyond-its-width": (LoomDtype.INT8, "300"),
    "float-from-string": (LoomDtype.FLOAT64, '"oops"'),
    "bool-from-number": (LoomDtype.BOOLEAN, "1"),
    "decimal-from-string": (_PRICE, '"oops"'),
    "decimal-from-underscored-text": (_PRICE, '"1_0"'),
    "decimal-overflowing-precision": (_PRICE, "99999999999"),
}
_KEPT = {
    "decimal-in-range": (_PRICE, "12.5", Decimal("12.50")),
    "date-from-iso-string": (LoomDtype.DATE, '"2024-01-02"', date(2024, 1, 2)),
    "str-from-bool": (LoomDtype.UTF8, "true", "true"),
}


class TestNullModeDecoding:
    @pytest.mark.parametrize(("frame_type", "document"), _NULLED.values(), ids=list(_NULLED))
    def test_a_wrongly_typed_field_is_nulled_and_its_siblings_are_kept(
        self, engine: ModelEngine, frame_type: FrameType, document: str
    ) -> None:
        buyers = _decode(
            engine, _probe(frame_type), JsonMismatch.NULL, f'{{"v": {document}, "w": 9}}'
        )

        assert buyers == [{"v": None, "w": 9}]

    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _KEPT.values(), ids=list(_KEPT)
    )
    def test_a_well_typed_field_is_kept(
        self, engine: ModelEngine, frame_type: FrameType, document: str, expected: object
    ) -> None:
        assert _decode_field(engine, frame_type, document, JsonMismatch.NULL) == expected


_ENGINE_READS = {
    "nan": (LoomDtype.FLOAT64, "NaN", {PolarsEngine: None, SparkEngine: approx(nan, nan_ok=True)}),
    "infinity": (LoomDtype.FLOAT64, "Infinity", {PolarsEngine: None, SparkEngine: inf}),
    "beyond-float32": (LoomDtype.FLOAT32, "1e300", {PolarsEngine: None, SparkEngine: inf}),
    "negative-unsigned": (LoomDtype.UINT8, "-1", {PolarsEngine: None, SparkEngine: -1}),
}


class TestNumbersBeyondTheirType:
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _ENGINE_READS.values(), ids=list(_ENGINE_READS)
    )
    def test_the_row_reads_as_each_engine_holds_the_field_and_keeps_its_sibling(
        self, engine: ModelEngine, frame_type: FrameType, document: str, expected: dict[type, Any]
    ) -> None:
        (buyer,) = _decode(
            engine, _probe(frame_type), JsonMismatch.NULL, f'{{"v": {document}, "w": 9}}'
        )

        assert buyer == {"v": expected[type(engine)], "w": 9}


_INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
_VISIT = StructType(fields=(StructField("seen_at", _UTC),))
_DAY = date(2026, 11, 28)
_TEMPORAL_READ = {
    "iso-date": (LoomDtype.DATE, '"2026-11-28"', _DAY),
    "date-from-datetime": (LoomDtype.DATE, '"2026-11-28T10:00:00"', _DAY),
    "date-from-spaced-datetime": (LoomDtype.DATE, '"2026-11-28 10:00:00"', _DAY),
    "date-from-datetime-with-offset": (LoomDtype.DATE, '"2026-11-28T23:00:00-05:00"', _DAY),
    "instant-with-z": (_UTC, '"2026-11-28T10:00:00Z"', _INSTANT),
    "instant-without-offset": (_UTC, '"2026-11-28T10:00:00"', _INSTANT),
    "instant-with-space": (_UTC, '"2026-11-28 10:00:00"', _INSTANT),
    "instant-with-offset": (_UTC, '"2026-11-28T12:00:00+02:00"', _INSTANT),
    "instant-from-date": (_UTC, '"2026-11-28"', _INSTANT.replace(hour=0)),
    "instant-with-fraction": (
        _UTC,
        '"2026-11-28T10:00:00.123456Z"',
        _INSTANT.replace(microsecond=123456),
    ),
    "instant-in-struct": (_VISIT, '{"seen_at": "2026-11-28 10:00:00"}', {"seen_at": _INSTANT}),
    "instants-in-list": (
        ListType(_UTC),
        '["2026-11-28T10:00:00", "2026-11-28T10:00:00Z"]',
        [_INSTANT, _INSTANT],
    ),
}
_INVALID_TEMPORAL = {
    "date": (LoomDtype.DATE, '"yesterday"', None),
    "date-from-number": (LoomDtype.DATE, "5", None),
    "instant": (_UTC, '"yesterday"', None),
    "instant-in-struct": (_VISIT, '{"seen_at": "yesterday"}', {"seen_at": None}),
}


def _step_decoding(contract: FrameType | type) -> type[ETLStep[NoParams]]:
    class _Decode(ETLStep[NoParams]):
        customers: FromTable = FromTable("raw.customers").parse_json("buyer", contract)  # type: ignore[assignment]
        target = IntoTable("staging.customers").replace()

        def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
            return customers

    return _Decode


class TestTemporalText:
    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _TEMPORAL_READ.values(), ids=list(_TEMPORAL_READ)
    )
    def test_reads_dates_and_instants_from_iso_text_as_utc(
        self,
        engine: ModelEngine,
        frame_type: FrameType,
        document: str,
        expected: object,
        on_mismatch: JsonMismatch,
    ) -> None:
        assert _decode_field(engine, frame_type, document, on_mismatch) == expected

    def test_text_with_an_offset_under_a_naive_timestamp_is_its_utc_wall_time(
        self, polars_engine: PolarsEngine
    ) -> None:
        decoded = _decode_field(
            polars_engine, _NAIVE, '"2026-11-28T12:00:00+02:00"', JsonMismatch.NULL
        )

        assert decoded == _INSTANT.replace(tzinfo=None)

    @pytest.mark.parametrize(
        ("frame_type", "document", "nulled"),
        _INVALID_TEMPORAL.values(),
        ids=list(_INVALID_TEMPORAL),
    )
    def test_invalid_temporal_text_is_null_in_null_mode(
        self, engine: ModelEngine, frame_type: FrameType, document: str, nulled: object
    ) -> None:
        assert _decode_field(engine, frame_type, document, JsonMismatch.NULL) == nulled

    @pytest.mark.parametrize(
        ("frame_type", "document"),
        [(frame_type, document) for frame_type, document, _ in _INVALID_TEMPORAL.values()],
        ids=list(_INVALID_TEMPORAL),
    )
    def test_invalid_temporal_text_fails_naming_the_column_in_fail_mode(
        self, engine: ModelEngine, frame_type: FrameType, document: str
    ) -> None:
        _seed_customers(engine, f'{{"v": {document}, "w": 9}}')

        with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
            _run(engine, _step_decoding(_probe(frame_type)))


def _read_field(
    engine: PolarsEngine, frame_type: FrameType, document: str, on_mismatch: JsonMismatch
) -> Any:
    _seed_customers(engine, f'{{"v": {document}, "w": 9}}')
    source = FromTable("raw.customers").parse_json(
        "buyer", _probe(frame_type), on_mismatch=on_mismatch
    )
    frame = engine.reader.read(source._to_spec("customers"), None).collect()
    return frame["buyer"].to_list()[0]


_ARRAYS = {
    "date": (
        ArrayType(LoomDtype.DATE, 2),
        '["2026-11-28", "2026-11-29"]',
        [_DAY, date(2026, 11, 29)],
    ),
    "instant": (
        ArrayType(_UTC, 2),
        '["2026-11-28T10:00:00Z", "2026-11-28 10:00:00"]',
        [_INSTANT, _INSTANT],
    ),
}
_WRONG_WIDTH = {
    "date": (ArrayType(LoomDtype.DATE, 2), '["2026-11-28"]'),
    "instant": (ArrayType(_UTC, 2), '["2026-11-28T10:00:00Z", "2026-11-28", "2026-11-28"]'),
    "integer": (ArrayType(LoomDtype.INT64, 2), "[1, 2, 3]"),
}


class TestFixedWidthArrays:
    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _ARRAYS.values(), ids=list(_ARRAYS)
    )
    def test_an_array_of_its_width_decodes(
        self,
        polars_engine: PolarsEngine,
        frame_type: FrameType,
        document: str,
        expected: object,
        on_mismatch: JsonMismatch,
    ) -> None:
        assert _read_field(polars_engine, frame_type, document, on_mismatch)["v"] == expected

    @pytest.mark.parametrize(
        ("frame_type", "document"), _WRONG_WIDTH.values(), ids=list(_WRONG_WIDTH)
    )
    def test_an_array_of_another_width_is_null_in_null_mode(
        self, polars_engine: PolarsEngine, frame_type: FrameType, document: str
    ) -> None:
        buyer = _read_field(polars_engine, frame_type, document, JsonMismatch.NULL)

        assert buyer == {"v": None, "w": 9}

    @pytest.mark.parametrize(
        ("frame_type", "document"), _WRONG_WIDTH.values(), ids=list(_WRONG_WIDTH)
    )
    def test_an_array_of_another_width_fails_naming_the_column_in_fail_mode(
        self, polars_engine: PolarsEngine, frame_type: FrameType, document: str
    ) -> None:
        _seed_customers(polars_engine, f'{{"v": {document}, "w": 9}}')

        with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
            _run(polars_engine, _step_decoding(_probe(frame_type)))


class _UnrelatedCast(ETLStep[NoParams]):
    customers: FromTable = FromTable("raw.customers").parse_json(  # type: ignore[assignment]
        "buyer", _probe(LoomDtype.DATE)
    )
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: object) -> object:  # type: ignore[override]
        return customers.with_columns(pl.col("note").str.to_date("%Y-%m-%d"))  # type: ignore[attr-defined]


class TestNullModeBatches:
    def test_a_bad_row_past_the_chunk_boundary_is_nulled_alone(
        self, polars_engine: PolarsEngine
    ) -> None:
        good = '{"v": 1, "w": 9}'
        documents = [good] * 8_500 + ['{"v": "x", "w": 9}'] + [good] * 499

        buyers = _decode(polars_engine, _probe(LoomDtype.INT64), JsonMismatch.NULL, *documents)

        assert (
            buyers == [{"v": 1, "w": 9}] * 8_500 + [{"v": None, "w": 9}] + [{"v": 1, "w": 9}] * 499
        )

    def test_an_empty_batch_decodes_to_the_declared_type(self) -> None:
        source = FromTable("raw.customers").parse_json(
            "buyer", _probe(LoomDtype.INT64), on_mismatch=JsonMismatch.NULL
        )
        empty = pl.DataFrame({"buyer": []}, schema={"buyer": pl.String})

        decoded = decode_json_columns(empty, source.json_columns)

        assert (decoded.height, decoded.schema["buyer"]) == (
            0,
            pl.Struct({"v": pl.Int64, "w": pl.Int64}),
        )

    def test_a_step_cast_of_another_column_keeps_its_own_failure(
        self, polars_engine: PolarsEngine
    ) -> None:
        polars_engine.writer.write(
            polars_engine.frame({"buyer": ['{"v": "2026-11-28", "w": 9}'], "note": ["oops"]}),
            IntoTable("raw.customers").append()._to_spec(),
            None,
        )

        with pytest.raises(pl.exceptions.InvalidOperationError):
            _run(polars_engine, _UnrelatedCast)


class TestJsonTextDecoder:
    @pytest.mark.parametrize("dtype", [pl.Date, pl.Date()], ids=["class", "instance"])
    def test_a_date_dtype_decodes_the_same_as_a_class_or_an_instance(
        self, dtype: pl.DataType
    ) -> None:
        column = JsonTextDecoder.refusing().decode(pl.first(), dtype)

        assert pl.Series(['"2026-11-28"']).to_frame().select(column).to_series().to_list() == [_DAY]
