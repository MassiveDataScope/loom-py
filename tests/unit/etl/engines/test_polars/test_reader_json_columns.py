"""Unit tests for JSON column decoding with decode_json_columns."""

from __future__ import annotations

import json

import polars as pl
import pytest

from loom.etl.declarative.source import JsonColumnSpec, JsonMismatch
from loom.etl.engines.polars import decode_json_columns
from loom.etl.schema._schema import FrameType, ListType, LoomDtype, StructField, StructType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _frame(*rows: dict) -> pl.LazyFrame:
    """Build a lazy frame from a list of row dicts with string-encoded JSON columns."""
    return pl.DataFrame(rows).lazy()


def _json_str(obj: object) -> str:
    return json.dumps(obj)


class TestDecodeJsonColumnShapes:
    # ---------------------------------------------------------------------------
    # No json_columns → passthrough
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_no_columns_returns_same_frame(self) -> None:
        frame = _frame({"id": 1, "payload": '{"x": 1}'})
        result = decode_json_columns(frame, ())
        assert result is frame

    # ---------------------------------------------------------------------------
    # Simple struct
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_simple_struct(self) -> None:
        struct_type = StructType(
            fields=(
                StructField("order_id", LoomDtype.INT64),
                StructField("amount", LoomDtype.FLOAT64),
            )
        )
        jc = JsonColumnSpec(column="payload", frame_type=struct_type)

        rows = [
            {"id": 1, "payload": _json_str({"order_id": 10, "amount": 99.5})},
            {"id": 2, "payload": _json_str({"order_id": 20, "amount": 49.5})},
        ]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["payload"].dtype == pl.Struct(
            [pl.Field("order_id", pl.Int64), pl.Field("amount", pl.Float64)]
        )
        decoded = result["payload"].to_list()
        assert decoded[0]["order_id"] == 10
        assert decoded[1]["amount"] == pytest.approx(49.5)

    # ---------------------------------------------------------------------------
    # List of primitives
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_list_of_primitives(self) -> None:
        jc = JsonColumnSpec(column="tags", frame_type=ListType(inner=LoomDtype.UTF8))

        rows = [
            {"id": 1, "tags": _json_str(["a", "b"])},
            {"id": 2, "tags": _json_str(["x"])},
        ]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["tags"].dtype == pl.List(pl.String)
        assert result["tags"][0].to_list() == ["a", "b"]

    # ---------------------------------------------------------------------------
    # List of structs
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_list_of_structs(self) -> None:
        inner = StructType(
            fields=(
                StructField("kind", LoomDtype.UTF8),
                StructField("count", LoomDtype.INT64),
            )
        )
        jc = JsonColumnSpec(column="events", frame_type=ListType(inner=inner))

        rows = [
            {"id": 1, "events": _json_str([{"kind": "click", "count": 3}])},
        ]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["events"].dtype == pl.List(
            pl.Struct([pl.Field("kind", pl.String), pl.Field("count", pl.Int64)])
        )
        first = result["events"][0].to_list()
        assert first[0]["kind"] == "click"
        assert first[0]["count"] == 3

    # ---------------------------------------------------------------------------
    # Multiple json columns decoded independently
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_multiple_columns(self) -> None:
        jc_payload = JsonColumnSpec(
            column="payload",
            frame_type=StructType(fields=(StructField("x", LoomDtype.INT64),)),
        )
        jc_tags = JsonColumnSpec(column="tags", frame_type=ListType(inner=LoomDtype.UTF8))

        rows = [{"payload": '{"x": 5}', "tags": '["a","b"]'}]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc_payload, jc_tags)).collect()

        assert result["payload"][0]["x"] == 5
        assert result["tags"][0].to_list() == ["a", "b"]

    # ---------------------------------------------------------------------------
    # Other columns are untouched
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_preserves_non_json_columns(self) -> None:
        jc = JsonColumnSpec(
            column="meta",
            frame_type=StructType(fields=(StructField("k", LoomDtype.UTF8),)),
        )
        rows = [{"id": 42, "name": "alice", "meta": '{"k": "v"}'}]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["id"][0] == 42
        assert result["name"][0] == "alice"
        assert result["meta"][0]["k"] == "v"

    # ---------------------------------------------------------------------------
    # Nested struct
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_nested_struct(self) -> None:
        inner_struct = StructType(
            fields=(
                StructField("lat", LoomDtype.FLOAT64),
                StructField("lon", LoomDtype.FLOAT64),
            )
        )
        outer_struct = StructType(
            fields=(
                StructField("name", LoomDtype.UTF8),
                StructField("point", inner_struct),
            )
        )
        jc = JsonColumnSpec(column="loc", frame_type=outer_struct)

        rows = [{"loc": _json_str({"name": "HQ", "point": {"lat": 40.7, "lon": -74.0}})}]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        loc = result["loc"][0]
        assert loc["name"] == "HQ"
        assert loc["point"]["lat"] == pytest.approx(40.7)
        assert loc["point"]["lon"] == pytest.approx(-74.0)

    # ---------------------------------------------------------------------------
    # Null JSON value is handled gracefully (Polars returns null struct)
    # ---------------------------------------------------------------------------

    def test_decode_json_columns_null_json_value(self) -> None:
        jc = JsonColumnSpec(
            column="payload",
            frame_type=StructType(fields=(StructField("x", LoomDtype.INT64),)),
        )
        rows = [{"payload": "null"}, {"payload": '{"x": 1}'}]
        frame = pl.DataFrame(rows).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["payload"][0] is None or result["payload"][0]["x"] is None
        assert result["payload"][1]["x"] == 1


class TestTolerantJsonDecoding:
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


# ---------------------------------------------------------------------------
# Null-dtype and all-null column handling — must always emit the declared dtype.
# Prevents "Invalid data type for Delta Lake: Null" downstream.
# ---------------------------------------------------------------------------


class TestNullColumnTypeSafety:
    """json_decode must produce the declared dtype even when all values are null."""

    def test_all_null_string_column_struct_has_declared_dtype(self) -> None:
        struct_type = StructType(fields=(StructField("user_id", LoomDtype.INT64),))
        jc = JsonColumnSpec(column="payload", frame_type=struct_type)
        frame = pl.DataFrame({"payload": pl.Series([None, None], dtype=pl.String)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["payload"].dtype == pl.Struct([pl.Field("user_id", pl.Int64)])
        assert result["payload"][0] is None
        assert result["payload"][1] is None

    def test_null_dtype_column_struct_has_declared_dtype(self) -> None:
        struct_type = StructType(fields=(StructField("amount", LoomDtype.FLOAT64),))
        jc = JsonColumnSpec(column="data", frame_type=struct_type)
        # Null dtype — simulates optional_col()/pl.lit(None) columns from pipelines
        frame = pl.DataFrame({"data": pl.Series([None, None], dtype=pl.Null)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["data"].dtype == pl.Struct([pl.Field("amount", pl.Float64)])

    def test_all_null_string_column_list_has_declared_dtype(self) -> None:
        jc = JsonColumnSpec(column="tags", frame_type=ListType(inner=LoomDtype.UTF8))
        frame = pl.DataFrame({"tags": pl.Series([None, None], dtype=pl.String)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["tags"].dtype == pl.List(pl.String)

    def test_null_dtype_column_primitive_has_declared_dtype(self) -> None:
        jc = JsonColumnSpec(column="label", frame_type=LoomDtype.UTF8)
        frame = pl.DataFrame({"label": pl.Series([None], dtype=pl.Null)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["label"].dtype == pl.String

    def test_mixed_null_and_valid_rows_declared_dtype_preserved(self) -> None:
        jc = JsonColumnSpec(column="payload", frame_type=LoomDtype.UTF8)
        rows = [{"payload": None}, {"payload": '"active"'}, {"payload": None}]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["payload"].dtype == pl.String
        assert result["payload"][0] is None
        assert result["payload"][1] == "active"
        assert result["payload"][2] is None
