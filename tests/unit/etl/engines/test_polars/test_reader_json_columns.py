"""Unit tests for JSON column decoding with decode_json_columns."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.declarative.source import JsonColumnSpec, JsonMismatch
from loom.etl.engines.polars import decode_json_columns, frame_type_to_polars
from loom.etl.schema import FrameType, ListType, LoomDtype, StructField, StructType

_ORDER = StructType(
    (StructField("order_id", LoomDtype.INT64), StructField("amount", LoomDtype.FLOAT64))
)
_EVENT = StructType((StructField("kind", LoomDtype.UTF8), StructField("count", LoomDtype.INT64)))
_POINT = StructType((StructField("lat", LoomDtype.FLOAT64), StructField("lon", LoomDtype.FLOAT64)))
_PLACE = StructType((StructField("name", LoomDtype.UTF8), StructField("point", _POINT)))
_SHAPES = {
    "struct": (_ORDER, '{"order_id": 10, "amount": 99.5}', {"order_id": 10, "amount": 99.5}),
    "list-of-primitives": (ListType(LoomDtype.UTF8), '["a", "b"]', ["a", "b"]),
    "list-of-structs": (
        ListType(_EVENT),
        '[{"kind": "click", "count": 3}]',
        [{"kind": "click", "count": 3}],
    ),
    "nested-struct": (
        _PLACE,
        '{"name": "HQ", "point": {"lat": 40.5, "lon": -74.0}}',
        {"name": "HQ", "point": {"lat": 40.5, "lon": -74.0}},
    ),
    "json-null": (_ORDER, "null", None),
}


def _decoded(frame_type: FrameType, texts: pl.Series) -> pl.Series:
    column = JsonColumnSpec(column="payload", frame_type=frame_type)
    return decode_json_columns(texts.alias("payload").to_frame(), (column,))["payload"]


class TestDecodeJsonColumnShapes:
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _SHAPES.values(), ids=list(_SHAPES)
    )
    def test_a_column_decodes_to_its_declared_dtype_and_values(
        self, frame_type: FrameType, document: str, expected: object
    ) -> None:
        decoded = _decoded(frame_type, pl.Series([document]))

        assert (decoded.dtype, decoded.to_list()) == (frame_type_to_polars(frame_type), [expected])


class TestNullPolicyDecoding:
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"),
        [
            (LoomDtype.UTF8, '{"source": "web"}', '{"source":"web"}'),
            (
                StructType(
                    (StructField("name", LoomDtype.UTF8), StructField("meta", LoomDtype.UTF8))
                ),
                '{"name": "a", "meta": {"page": "/home"}}',
                {"name": "a", "meta": '{"page":"/home"}'},
            ),
            (
                ListType(
                    StructType(
                        (StructField("key", LoomDtype.UTF8), StructField("val", LoomDtype.UTF8))
                    )
                ),
                '[{"key": "k", "val": {"x": 1}}]',
                [{"key": "k", "val": '{"x":1}'}],
            ),
            (StructType((StructField("user_id", LoomDtype.INT64),)), "[1, 2, 3]", None),
            (StructType((StructField("user_id", LoomDtype.INT64),)), "not json", None),
            (ListType(LoomDtype.UTF8), '"single"', None),
            (StructType((StructField("s", LoomDtype.UTF8),)), '{"s": "\\ud800"}', None),
            (StructType((StructField("s", LoomDtype.UTF8),)), '{"s": {"a": "\\ud800"}}', None),
            (ListType(LoomDtype.UTF8), "[" * 100_000 + "]" * 100_000, None),
        ],
        ids=[
            "object-under-str",
            "object-under-nested-str",
            "object-under-str-in-list-item",
            "array-for-struct",
            "malformed",
            "scalar-for-list",
            "lone-surrogate-under-str",
            "lone-surrogate-nested-under-str",
            "nesting-too-deep",
        ],
    )
    def test_a_mismatched_document_decodes_to_text_or_null(
        self, frame_type: FrameType, document: str, expected: object
    ) -> None:
        jc = JsonColumnSpec("payload", frame_type, JsonMismatch.NULL)

        result = decode_json_columns(pl.DataFrame({"payload": [document]}), (jc,))

        assert result["payload"].to_list() == [expected]


_NO_TEXT = {
    "struct-from-null-text": (_ORDER, pl.Series([None, None], dtype=pl.String)),
    "struct-from-null-dtype": (_ORDER, pl.Series([None, None], dtype=pl.Null)),
    "list-from-null-text": (ListType(LoomDtype.UTF8), pl.Series([None, None], dtype=pl.String)),
    "text-from-null-dtype": (LoomDtype.UTF8, pl.Series([None], dtype=pl.Null)),
}


class TestNullColumnTypeSafety:
    @pytest.mark.parametrize(("frame_type", "texts"), _NO_TEXT.values(), ids=list(_NO_TEXT))
    def test_a_column_with_no_text_keeps_the_declared_dtype(
        self, frame_type: FrameType, texts: pl.Series
    ) -> None:
        assert _decoded(frame_type, texts).dtype == frame_type_to_polars(frame_type)
