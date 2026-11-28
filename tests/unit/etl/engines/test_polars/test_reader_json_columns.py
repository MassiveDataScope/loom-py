"""JSON column decoding with decode_json_columns: whole documents, batches and empty columns."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.declarative.source import JsonColumnSpec, JsonMismatch
from loom.etl.engines.polars import decode_json_columns, frame_type_to_polars
from loom.etl.schema import (
    FrameType,
    JsonLeafUnsupported,
    JsonRefusal,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.engines.json_readings import ARRAY_OF_LEVELS

_PAIR = StructType((StructField("v", LoomDtype.INT64), StructField("w", LoomDtype.INT64)))


def _decoded(
    frame_type: FrameType, texts: pl.Series, on_mismatch: JsonMismatch = JsonMismatch.FAIL
) -> pl.Series:
    column = JsonColumnSpec("payload", frame_type, on_mismatch)
    return decode_json_columns(texts.alias("payload").to_frame(), (column,))["payload"]


def _documents(*documents: str | None) -> pl.Series:
    return pl.Series(documents, dtype=pl.String)


_TOO_DEEP = "[" * 100_000 + "]" * 100_000


_NO_TEXT = {
    "struct-from-null-text": (_PAIR, pl.Series([None, None], dtype=pl.String)),
    "struct-from-null-dtype": (_PAIR, pl.Series([None, None], dtype=pl.Null)),
    "list-from-null-text": (ListType(LoomDtype.UTF8), pl.Series([None, None], dtype=pl.String)),
    "text-from-null-dtype": (LoomDtype.UTF8, pl.Series([None], dtype=pl.Null)),
    "struct-from-no-rows": (_PAIR, pl.Series([], dtype=pl.String)),
}


class TestNullColumnTypeSafety:
    @pytest.mark.parametrize(("frame_type", "texts"), _NO_TEXT.values(), ids=list(_NO_TEXT))
    def test_a_column_with_no_text_keeps_the_declared_dtype(
        self, frame_type: FrameType, texts: pl.Series
    ) -> None:
        assert _decoded(frame_type, texts).dtype == frame_type_to_polars(frame_type)


class TestUnsupportedLeaf:
    def test_a_leaf_no_json_rule_reads_is_refused_naming_its_path(self) -> None:
        with pytest.raises(JsonLeafUnsupported, match=r"at \$"):
            _decoded(LoomDtype.LIST, _documents("[1]"))


class TestWholeDocuments:
    @pytest.mark.parametrize(
        "document",
        ["not json", "[1]", _TOO_DEEP],
        ids=["not-json", "not-an-object", "nesting-too-deep"],
    )
    def test_null_policy_nulls_a_document_unreadable_as_an_object(self, document: str) -> None:
        decoded = _decoded(_PAIR, _documents(document), JsonMismatch.NULL)

        assert decoded.to_list() == [None]

    @pytest.mark.parametrize(
        ("document", "rule"),
        [
            ("not json", "a JSON document"),
            ("[1]", "an object"),
            (_TOO_DEEP, "a JSON document"),
        ],
        ids=["not-json", "not-an-object", "nesting-too-deep"],
    )
    def test_fail_policy_refuses_a_document_naming_the_root(self, document: str, rule: str) -> None:
        with pytest.raises(JsonRefusal, match=rf"\$ expected {rule}, in JSON column payload"):
            _decoded(_PAIR, _documents(document))

    @pytest.mark.parametrize("on_mismatch", list(JsonMismatch), ids=lambda mode: mode.value)
    def test_a_null_document_is_null_under_both_policies(self, on_mismatch: JsonMismatch) -> None:
        assert _decoded(_PAIR, _documents("null", None), on_mismatch).to_list() == [None, None]

    def test_a_document_without_a_field_leaves_it_null(self) -> None:
        assert _decoded(_PAIR, _documents('{"w": 2}')).to_list() == [{"v": None, "w": 2}]

    def test_a_document_drops_a_field_the_type_does_not_declare(self) -> None:
        assert _decoded(_PAIR, _documents('{"v": 1, "w": 2, "extra": 3}')).to_list() == [
            {"v": 1, "w": 2}
        ]


_GOOD = '{"v": 1, "w": 9}'
_BAD = '{"v": "x", "w": 9}'


class TestBatches:
    def test_a_valued_array_of_structs_beside_a_null_row_reads_both(self) -> None:
        rows = _decoded(ARRAY_OF_LEVELS, _documents('[{"e": "low"}]', None)).to_list()

        assert rows == [[{"e": "low"}], None]

    def test_a_bad_row_among_many_is_nulled_alone(self) -> None:
        documents = _documents(*[_GOOD] * 10_000, _BAD, *[_GOOD] * 10_000)

        rows = _decoded(_PAIR, documents, JsonMismatch.NULL).to_list()

        assert (
            rows
            == [{"v": 1, "w": 9}] * 10_000 + [{"v": None, "w": 9}] + [{"v": 1, "w": 9}] * 10_000
        )

    def test_fail_policy_refuses_a_bad_row_in_a_later_chunk(self) -> None:
        documents = _documents(*[_GOOD] * 10_000, _BAD)

        with pytest.raises(
            JsonRefusal, match=r"\$\.v expected an integer from .*, in JSON column payload"
        ):
            _decoded(_PAIR, documents)
