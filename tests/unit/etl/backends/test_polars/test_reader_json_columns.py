"""Unit tests for JSON column decoding with decode_json_columns."""

from __future__ import annotations

import json

import polars as pl
import pytest

from loom.etl.backends.polars import decode_json_columns
from loom.etl.declarative.source import JsonColumnSpec
from loom.etl.schema._schema import ListType, LoomDtype, StructField, StructType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _frame(*rows: dict) -> pl.LazyFrame:
    """Build a lazy frame from a list of row dicts with string-encoded JSON columns."""
    return pl.DataFrame(rows).lazy()


def _json_str(obj: object) -> str:
    return json.dumps(obj)


# ---------------------------------------------------------------------------
# No json_columns → passthrough
# ---------------------------------------------------------------------------


def test_decode_json_columns_no_columns_returns_same_frame() -> None:
    frame = _frame({"id": 1, "payload": '{"x": 1}'})
    result = decode_json_columns(frame, ())
    assert result is frame


# ---------------------------------------------------------------------------
# Simple struct
# ---------------------------------------------------------------------------


def test_decode_json_columns_simple_struct() -> None:
    struct_type = StructType(
        fields=(
            StructField("order_id", LoomDtype.INT64),
            StructField("amount", LoomDtype.FLOAT64),
        )
    )
    jc = JsonColumnSpec(column="payload", loom_type=struct_type)

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


def test_decode_json_columns_list_of_primitives() -> None:
    jc = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))

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


def test_decode_json_columns_list_of_structs() -> None:
    inner = StructType(
        fields=(
            StructField("kind", LoomDtype.UTF8),
            StructField("count", LoomDtype.INT64),
        )
    )
    jc = JsonColumnSpec(column="events", loom_type=ListType(inner=inner))

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


def test_decode_json_columns_multiple_columns() -> None:
    jc_payload = JsonColumnSpec(
        column="payload",
        loom_type=StructType(fields=(StructField("x", LoomDtype.INT64),)),
    )
    jc_tags = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))

    rows = [{"payload": '{"x": 5}', "tags": '["a","b"]'}]
    frame = pl.DataFrame(rows).lazy()
    result = decode_json_columns(frame, (jc_payload, jc_tags)).collect()

    assert result["payload"][0]["x"] == 5
    assert result["tags"][0].to_list() == ["a", "b"]


# ---------------------------------------------------------------------------
# Other columns are untouched
# ---------------------------------------------------------------------------


def test_decode_json_columns_preserves_non_json_columns() -> None:
    jc = JsonColumnSpec(
        column="meta",
        loom_type=StructType(fields=(StructField("k", LoomDtype.UTF8),)),
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


def test_decode_json_columns_nested_struct() -> None:
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
    jc = JsonColumnSpec(column="loc", loom_type=outer_struct)

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


def test_decode_json_columns_null_json_value() -> None:
    jc = JsonColumnSpec(
        column="payload",
        loom_type=StructType(fields=(StructField("x", LoomDtype.INT64),)),
    )
    rows = [{"payload": "null"}, {"payload": '{"x": 1}'}]
    frame = pl.DataFrame(rows).lazy()
    result = decode_json_columns(frame, (jc,)).collect()

    assert result["payload"][0] is None or result["payload"][0]["x"] is None
    assert result["payload"][1]["x"] == 1


# ---------------------------------------------------------------------------
# Polymorphic JSON normalization
# ---------------------------------------------------------------------------


class TestPolymorphicJsonNormalization:
    """Verify that decode_json_columns tolerates polymorphic JSON fields in the same batch."""

    def test_t1_string_field_with_json_object_coerces_to_string(self) -> None:
        jc = JsonColumnSpec(column="origin", loom_type=LoomDtype.UTF8)
        rows = [
            {"id": 1, "origin": '"web"'},
            {"id": 2, "origin": '{"source": "web", "medium": "referral"}'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["origin"][0] == "web"
        assert json.loads(result["origin"][1]) == {"source": "web", "medium": "referral"}

    def test_t2_string_field_with_json_array_coerces_to_string(self) -> None:
        jc = JsonColumnSpec(column="tag", loom_type=LoomDtype.UTF8)
        rows = [
            {"id": 1, "tag": '"featured"'},
            {"id": 2, "tag": '["sale", "new"]'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["tag"][0] == "featured"
        assert json.loads(result["tag"][1]) == ["sale", "new"]

    def test_t3_struct_field_with_array_value_produces_null(self) -> None:
        struct_type = StructType(fields=(StructField("user_id", LoomDtype.INT64),))
        jc = JsonColumnSpec(column="profile", loom_type=struct_type)
        rows = [
            {"id": 1, "profile": '{"user_id": 1}'},
            {"id": 2, "profile": "[1, 2, 3]"},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["profile"][0]["user_id"] == 1
        assert result["profile"][1] is None or result["profile"][1]["user_id"] is None

    def test_t4_struct_subfield_declared_string_receives_object(self) -> None:
        struct_type = StructType(
            fields=(
                StructField("name", LoomDtype.UTF8),
                StructField("meta", LoomDtype.UTF8),
            )
        )
        jc = JsonColumnSpec(column="event", loom_type=struct_type)
        rows = [
            {"id": 1, "event": '{"name": "click", "meta": "button_1"}'},
            {
                "id": 2,
                "event": '{"name": "pageview", "meta": {"page": "/home", "section": "hero"}}',
            },
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["event"][0]["name"] == "click"
        assert result["event"][0]["meta"] == "button_1"
        assert result["event"][1]["name"] == "pageview"
        assert json.loads(result["event"][1]["meta"]) == {"page": "/home", "section": "hero"}

    def test_t5_deeply_nested_struct_string_subfield_receives_object(self) -> None:
        inner_type = StructType(
            fields=(
                StructField("label", LoomDtype.UTF8),
                StructField("value", LoomDtype.UTF8),
            )
        )
        outer_type = StructType(
            fields=(
                StructField("event_id", LoomDtype.INT64),
                StructField("context", inner_type),
            )
        )
        jc = JsonColumnSpec(column="record", loom_type=outer_type)
        rows = [
            {
                "id": 1,
                "record": '{"event_id": 1, "context": {"label": "login", "value": "success"}}',
            },
            {
                "id": 2,
                "record": (
                    '{"event_id": 2, "context": {"label": "purchase",'
                    ' "value": {"amount": 99, "currency": "EUR"}}}'
                ),
            },
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["record"][0]["context"]["label"] == "login"
        assert result["record"][0]["context"]["value"] == "success"
        assert result["record"][1]["context"]["label"] == "purchase"
        assert json.loads(result["record"][1]["context"]["value"]) == {
            "amount": 99,
            "currency": "EUR",
        }

    def test_t6_list_field_with_scalar_value_produces_null(self) -> None:
        jc = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))
        rows = [
            {"id": 1, "tags": '["python", "data"]'},
            {"id": 2, "tags": '"single_tag"'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["tags"][0].to_list() == ["python", "data"]
        assert result["tags"][1] is None

    def test_t7_list_of_structs_with_polymorphic_subfield(self) -> None:
        item_type = StructType(
            fields=(
                StructField("key", LoomDtype.UTF8),
                StructField("val", LoomDtype.UTF8),
            )
        )
        jc = JsonColumnSpec(column="attrs", loom_type=ListType(inner=item_type))
        rows = [
            {"id": 1, "attrs": '[{"key": "color", "val": "blue"}]'},
            {"id": 2, "attrs": '[{"key": "config", "val": {"theme": "dark", "lang": "en"}}]'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["attrs"][0].to_list()[0]["val"] == "blue"
        assert json.loads(result["attrs"][1].to_list()[0]["val"]) == {"theme": "dark", "lang": "en"}

    def test_t8_null_rows_propagate_in_polymorphic_batch(self) -> None:
        jc = JsonColumnSpec(column="status", loom_type=LoomDtype.UTF8)
        rows = [
            {"id": 1, "status": None},
            {"id": 2, "status": '"active"'},
            {"id": 3, "status": '{"state": "pending", "since": "2024-01-01"}'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["status"][0] is None
        assert result["status"][1] == "active"
        assert json.loads(result["status"][2]) == {"state": "pending", "since": "2024-01-01"}

    def test_t9_multiple_columns_one_polymorphic(self) -> None:
        jc_origin = JsonColumnSpec(column="origin", loom_type=LoomDtype.UTF8)
        jc_tags = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))
        rows = [
            {"id": 1, "origin": '"web"', "tags": '["a"]'},
            {"id": 2, "origin": '{"source": "email", "campaign": "q4"}', "tags": '["b", "c"]'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc_origin, jc_tags)).collect()

        assert result["origin"][0] == "web"
        assert json.loads(result["origin"][1]) == {"source": "email", "campaign": "q4"}
        assert result["tags"][0].to_list() == ["a"]
        assert result["tags"][1].to_list() == ["b", "c"]

    def test_t10_well_formed_rows_unchanged_regression(self) -> None:
        struct_type = StructType(
            fields=(
                StructField("product_id", LoomDtype.INT64),
                StructField("name", LoomDtype.UTF8),
                StructField("price", LoomDtype.FLOAT64),
            )
        )
        jc = JsonColumnSpec(column="product", loom_type=struct_type)
        rows = [
            {"id": 1, "product": '{"product_id": 10, "name": "Widget", "price": 9.99}'},
            {"id": 2, "product": '{"product_id": 20, "name": "Gadget", "price": 49.0}'},
        ]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["product"][0]["product_id"] == 10
        assert result["product"][0]["name"] == "Widget"
        assert result["product"][0]["price"] == pytest.approx(9.99)
        assert result["product"][1]["product_id"] == 20
        assert result["product"][1]["name"] == "Gadget"


# ---------------------------------------------------------------------------
# Null-dtype and all-null column handling — must always emit the declared dtype.
# Prevents "Invalid data type for Delta Lake: Null" downstream.
# ---------------------------------------------------------------------------


class TestNullColumnTypeSafety:
    """json_decode must produce the declared dtype even when all values are null."""

    def test_all_null_string_column_struct_has_declared_dtype(self) -> None:
        struct_type = StructType(fields=(StructField("user_id", LoomDtype.INT64),))
        jc = JsonColumnSpec(column="payload", loom_type=struct_type)
        frame = pl.DataFrame({"payload": pl.Series([None, None], dtype=pl.String)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["payload"].dtype == pl.Struct([pl.Field("user_id", pl.Int64)])
        assert result["payload"][0] is None
        assert result["payload"][1] is None

    def test_null_dtype_column_struct_has_declared_dtype(self) -> None:
        struct_type = StructType(fields=(StructField("amount", LoomDtype.FLOAT64),))
        jc = JsonColumnSpec(column="data", loom_type=struct_type)
        # Null dtype — simulates optional_col()/pl.lit(None) columns from pipelines
        frame = pl.DataFrame({"data": pl.Series([None, None], dtype=pl.Null)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["data"].dtype == pl.Struct([pl.Field("amount", pl.Float64)])

    def test_all_null_string_column_list_has_declared_dtype(self) -> None:
        jc = JsonColumnSpec(column="tags", loom_type=ListType(inner=LoomDtype.UTF8))
        frame = pl.DataFrame({"tags": pl.Series([None, None], dtype=pl.String)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["tags"].dtype == pl.List(pl.String)

    def test_null_dtype_column_primitive_has_declared_dtype(self) -> None:
        jc = JsonColumnSpec(column="label", loom_type=LoomDtype.UTF8)
        frame = pl.DataFrame({"label": pl.Series([None], dtype=pl.Null)}).lazy()
        result = decode_json_columns(frame, (jc,)).collect()

        assert result["label"].dtype == pl.String

    def test_mixed_null_and_valid_rows_declared_dtype_preserved(self) -> None:
        jc = JsonColumnSpec(column="payload", loom_type=LoomDtype.UTF8)
        rows = [{"payload": None}, {"payload": '"active"'}, {"payload": None}]
        result = decode_json_columns(pl.DataFrame(rows).lazy(), (jc,)).collect()

        assert result["payload"].dtype == pl.String
        assert result["payload"][0] is None
        assert result["payload"][1] == "active"
        assert result["payload"][2] is None
