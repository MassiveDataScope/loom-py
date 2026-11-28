"""What only Polars reads: enums, fixed-width arrays, timestamp ranges and whole documents."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
import pytest

from loom.etl import FromTable
from loom.etl.declarative.source import JsonMismatch
from loom.etl.engines.polars import decode_json_columns
from loom.etl.schema import (
    ArrayType,
    DatetimeType,
    EnumType,
    FrameType,
    JsonRefusal,
    LoomDtype,
    StructField,
    StructType,
)

_UTC = DatetimeType("us", "UTC")
_DAY = date(2026, 11, 28)
_INSTANT = datetime(2026, 11, 28, 10, tzinfo=UTC)
_LEVEL = EnumType(("low", "high"))
_PAIR = StructType((StructField("v", LoomDtype.INT64), StructField("w", LoomDtype.INT64)))
_OUT = object()

_FIELDS = {
    "enum-member": (_LEVEL, '"low"', "low", "low"),
    "enum-unknown-member": (_LEVEL, '"medium"', None, _OUT),
    "enum-from-number": (_LEVEL, "1", None, _OUT),
    "array-of-dates": (
        ArrayType(LoomDtype.DATE, 2),
        '["2026-11-28", "2026-11-28"]',
        [_DAY, _DAY],
        [_DAY, _DAY],
    ),
    "array-of-instants": (
        ArrayType(_UTC, 2),
        '["2026-11-28T10:00:00Z", "2026-11-28 10:00:00"]',
        [_INSTANT, _INSTANT],
        [_INSTANT, _INSTANT],
    ),
    "array-of-dates-too-short": (ArrayType(LoomDtype.DATE, 2), '["2026-11-28"]', None, _OUT),
    "array-of-integers-too-long": (ArrayType(LoomDtype.INT64, 2), "[1, 2, 3]", None, _OUT),
    "nanosecond-instant": (
        DatetimeType("ns"),
        '"2026-11-28T10:00:00"',
        datetime(2026, 11, 28, 10),
        datetime(2026, 11, 28, 10),
    ),
    "nanosecond-instant-beyond-its-range": (
        DatetimeType("ns"),
        '"9999-12-31T23:59:59Z"',
        None,
        _OUT,
    ),
    "instant-before-the-year-1": (_UTC, '"0001-01-01T00:00:00+02:00"', None, _OUT),
    "instant-in-a-struct": (
        StructType((StructField("at", _UTC),)),
        '{"at": "2026-11-28 10:00:00"}',
        {"at": _INSTANT},
        {"at": _INSTANT},
    ),
}


def _decode(
    frame_type: FrameType, documents: Sequence[str | None], on_mismatch: JsonMismatch
) -> list[Any]:
    source = FromTable("raw.docs").parse_json("doc", frame_type, on_mismatch=on_mismatch)
    frame = pl.DataFrame({"doc": documents}, schema={"doc": pl.String})
    return decode_json_columns(frame, source.json_columns)["doc"].to_list()


def _field(frame_type: FrameType, value: str) -> tuple[StructType, str]:
    return StructType((StructField("v", frame_type),)), f'{{"v": {value}}}'


def _params(rows: Iterable[tuple[str, tuple[Any, ...]]]) -> list[Any]:
    return [pytest.param(*row, id=name) for name, row in rows]


_ALL = _params(_FIELDS.items())
_FITTING = _params((name, row) for name, row in _FIELDS.items() if row[3] is not _OUT)
_REFUSED = _params((name, row[:2]) for name, row in _FIELDS.items() if row[3] is _OUT)


class TestFieldsOnlyPolarsReads:
    @pytest.mark.parametrize(("frame_type", "value", "null", "_fail"), _ALL)
    def test_null_policy_gives_the_value(
        self, frame_type: FrameType, value: str, null: Any, _fail: Any
    ) -> None:
        shape, document = _field(frame_type, value)

        assert _decode(shape, [document], JsonMismatch.NULL) == [{"v": null}]

    @pytest.mark.parametrize(("frame_type", "value", "_null", "fail"), _FITTING)
    def test_fail_policy_reads_what_fits(
        self, frame_type: FrameType, value: str, _null: Any, fail: Any
    ) -> None:
        shape, document = _field(frame_type, value)

        assert _decode(shape, [document], JsonMismatch.FAIL) == [{"v": fail}]

    @pytest.mark.parametrize(("frame_type", "value"), _REFUSED)
    def test_fail_policy_refuses_naming_the_path(self, frame_type: FrameType, value: str) -> None:
        shape, document = _field(frame_type, value)

        with pytest.raises(JsonRefusal, match=r"\$\.v expected .* in JSON column doc"):
            _decode(shape, [document], JsonMismatch.FAIL)


class TestWholeDocuments:
    @pytest.mark.parametrize("document", ["not json", "[1]", "null", None], ids=str)
    def test_null_policy_nulls_a_document_that_is_not_an_object(self, document: str | None) -> None:
        assert _decode(_PAIR, [document], JsonMismatch.NULL) == [None]

    @pytest.mark.parametrize("document", ["not json", "[1]"])
    def test_fail_policy_refuses_a_document_that_is_not_an_object(self, document: str) -> None:
        with pytest.raises(JsonRefusal, match=r"\$ expected"):
            _decode(_PAIR, [document], JsonMismatch.FAIL)

    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    def test_a_null_document_is_null_under_both_policies(self, on_mismatch: JsonMismatch) -> None:
        assert _decode(_PAIR, ["null", None], on_mismatch) == [None, None]

    def test_a_document_without_a_field_leaves_it_null_and_drops_an_extra_one(self) -> None:
        assert _decode(_PAIR, ['{"w": 2, "extra": 3}'], JsonMismatch.FAIL) == [{"v": None, "w": 2}]


class TestBatches:
    def test_a_bad_row_among_many_is_nulled_alone(self) -> None:
        good = '{"v": 1, "w": 9}'
        documents = [good] * 10_000 + ['{"v": "x", "w": 9}'] + [good] * 10_000

        rows = _decode(_PAIR, documents, JsonMismatch.NULL)

        assert (
            rows
            == [{"v": 1, "w": 9}] * 10_000 + [{"v": None, "w": 9}] + [{"v": 1, "w": 9}] * 10_000
        )

    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    def test_an_empty_column_decodes_to_the_declared_type(self, on_mismatch: JsonMismatch) -> None:
        source = FromTable("raw.docs").parse_json("doc", _PAIR, on_mismatch=on_mismatch)
        empty = pl.DataFrame({"doc": []}, schema={"doc": pl.String})

        decoded = decode_json_columns(empty, source.json_columns)

        assert (decoded.height, decoded.schema["doc"]) == (
            0,
            pl.Struct({"v": pl.Int64, "w": pl.Int64}),
        )
