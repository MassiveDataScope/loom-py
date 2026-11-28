"""JSON column decoding with decode_json_columns: shapes, whole documents, batches and empties."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.declarative.source import JsonColumnSpec, JsonMismatch
from loom.etl.engines.polars import decode_json_columns, frame_type_to_polars
from loom.etl.schema import FrameType, JsonRefusal, ListType, LoomDtype, StructField, StructType

_PAIR = StructType((StructField("v", LoomDtype.INT64), StructField("w", LoomDtype.INT64)))
_ORDER = StructType(
    (StructField("order_id", LoomDtype.INT64), StructField("amount", LoomDtype.FLOAT64))
)
_EVENT = StructType((StructField("kind", LoomDtype.UTF8), StructField("count", LoomDtype.INT64)))
_POINT = StructType((StructField("lat", LoomDtype.FLOAT64), StructField("lon", LoomDtype.FLOAT64)))
_PLACE = StructType((StructField("name", LoomDtype.UTF8), StructField("point", _POINT)))
_SHAPES = {
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
}


def _decoded(
    frame_type: FrameType, texts: pl.Series, on_mismatch: JsonMismatch = JsonMismatch.FAIL
) -> pl.Series:
    column = JsonColumnSpec("payload", frame_type, on_mismatch)
    return decode_json_columns(texts.alias("payload").to_frame(), (column,))["payload"]


def _documents(*documents: str | None) -> pl.Series:
    return pl.Series(documents, dtype=pl.String)


class TestDecodeJsonColumnShapes:
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _SHAPES.values(), ids=list(_SHAPES)
    )
    def test_a_column_decodes_to_its_declared_dtype_and_values(
        self, frame_type: FrameType, document: str, expected: object
    ) -> None:
        decoded = _decoded(frame_type, pl.Series([document]))

        assert (decoded.dtype, decoded.to_list()) == (frame_type_to_polars(frame_type), [expected])


_TEXT_FIELD = StructType((StructField("s", LoomDtype.UTF8),))
_TOO_DEEP = "[" * 100_000 + "]" * 100_000
_NULLED = {
    "object-under-nested-str": (_TEXT_FIELD, '{"s": {"page": "/home"}}', {"s": '{"page":"/home"}'}),
    "lone-surrogate-nested-nulls-only-its-field": (
        _TEXT_FIELD,
        '{"s": {"a": "\\ud800"}}',
        {"s": None},
    ),
    "nesting-too-deep": (ListType(LoomDtype.UTF8), _TOO_DEEP, None),
}


class TestNullPolicyDecoding:
    @pytest.mark.parametrize(
        ("frame_type", "document", "expected"), _NULLED.values(), ids=list(_NULLED)
    )
    def test_a_mismatched_document_decodes_to_text_or_null(
        self, frame_type: FrameType, document: str, expected: object
    ) -> None:
        decoded = _decoded(frame_type, _documents(document), JsonMismatch.NULL)

        assert decoded.to_list() == [expected]


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


class TestWholeDocuments:
    @pytest.mark.parametrize("document", ["not json", "[1]", "null", None], ids=str)
    def test_null_policy_nulls_a_document_that_is_not_an_object(self, document: str | None) -> None:
        decoded = _decoded(_PAIR, _documents(document), JsonMismatch.NULL)

        assert decoded.to_list() == [None]

    @pytest.mark.parametrize(
        ("document", "rule"),
        [("not json", "a JSON document"), ("[1]", "an object")],
        ids=["not-json", "not-an-object"],
    )
    def test_fail_policy_refuses_a_document_naming_the_root(self, document: str, rule: str) -> None:
        with pytest.raises(JsonRefusal, match=rf"\$ expected {rule}, in JSON column payload"):
            _decoded(_PAIR, _documents(document))

    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    def test_a_null_document_is_null_under_both_policies(self, on_mismatch: JsonMismatch) -> None:
        assert _decoded(_PAIR, _documents("null", None), on_mismatch).to_list() == [None, None]

    def test_a_document_without_a_field_leaves_it_null(self) -> None:
        assert _decoded(_PAIR, _documents('{"w": 2}')).to_list() == [{"v": None, "w": 2}]

    def test_a_document_drops_a_field_the_type_does_not_declare(self) -> None:
        assert _decoded(_PAIR, _documents('{"v": 1, "w": 2, "extra": 3}')).to_list() == [
            {"v": 1, "w": 2}
        ]


class TestBatches:
    def test_a_bad_row_among_many_is_nulled_alone(self) -> None:
        good = '{"v": 1, "w": 9}'
        documents = _documents(*[good] * 10_000, '{"v": "x", "w": 9}', *[good] * 10_000)

        rows = _decoded(_PAIR, documents, JsonMismatch.NULL).to_list()

        assert rows.count({"v": None, "w": 9}) == 1

    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.name)
    def test_an_empty_column_decodes_to_the_declared_type(self, on_mismatch: JsonMismatch) -> None:
        assert _decoded(_PAIR, _documents(), on_mismatch).dtype == pl.Struct(
            {"v": pl.Int64, "w": pl.Int64}
        )
