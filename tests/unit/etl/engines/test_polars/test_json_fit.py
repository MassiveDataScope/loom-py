"""What only Polars decodes of a JSON column: whole documents, batches and empty columns."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import polars as pl
import pytest

from loom.etl import FromTable
from loom.etl.declarative.source import JsonMismatch
from loom.etl.engines.polars import decode_json_columns
from loom.etl.schema import FrameType, JsonRefusal, LoomDtype, StructField, StructType

_PAIR = StructType((StructField("v", LoomDtype.INT64), StructField("w", LoomDtype.INT64)))


def _decode(
    frame_type: FrameType, documents: Sequence[str | None], on_mismatch: JsonMismatch
) -> list[Any]:
    source = FromTable("raw.docs").parse_json("doc", frame_type, on_mismatch=on_mismatch)
    frame = pl.DataFrame({"doc": documents}, schema={"doc": pl.String})
    return decode_json_columns(frame, source.json_columns)["doc"].to_list()


class TestWholeDocuments:
    @pytest.mark.parametrize("document", ["not json", "[1]", "null", None], ids=str)
    def test_null_policy_nulls_a_document_that_is_not_an_object(self, document: str | None) -> None:
        assert _decode(_PAIR, [document], JsonMismatch.NULL) == [None]

    @pytest.mark.parametrize(
        ("document", "rule"),
        [("not json", "a JSON document"), ("[1]", "an object")],
        ids=["not-json", "not-an-object"],
    )
    def test_fail_policy_refuses_a_document_naming_the_root(self, document: str, rule: str) -> None:
        with pytest.raises(JsonRefusal, match=rf"\$ expected {rule}, in JSON column doc"):
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
