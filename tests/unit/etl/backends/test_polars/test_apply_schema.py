"""Unit tests for apply_schema — native pl.Schema contract."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.backends.polars._schema import SchemaNotFoundError, apply_schema_polars
from loom.etl.declarative.target import SchemaMode
from loom.etl.schema import SchemaError


def _frame(**cols: object) -> pl.LazyFrame:
    return pl.DataFrame(cols).lazy()


@pytest.mark.parametrize("mode", [SchemaMode.STRICT, SchemaMode.EVOLVE])
def test_apply_schema_none_raises_schema_not_found(mode: SchemaMode) -> None:
    frame = _frame(id=[1])
    with pytest.raises(SchemaNotFoundError):
        apply_schema_polars(frame, None, mode)


def test_overwrite_passthrough_identity() -> None:
    frame = _frame(id=[1], amount=["1.2"], extra=["x"])
    assert apply_schema_polars(frame, None, SchemaMode.OVERWRITE) is frame


def test_strict_casts_fills_missing_and_drops_extra_columns() -> None:
    schema = pl.Schema({"id": pl.Int64, "amount": pl.Float64, "label": pl.String})
    frame = _frame(id=[1], amount=["1.5"], extra=["drop-me"])

    out = apply_schema_polars(frame, schema, SchemaMode.STRICT).collect()

    assert out.columns == ["id", "amount", "label"]
    assert out["id"].dtype == pl.Int64
    assert out["amount"].dtype == pl.Float64
    assert out["label"].dtype == pl.String
    assert out["label"][0] is None


def test_evolve_casts_and_keeps_extra_columns() -> None:
    schema = pl.Schema({"id": pl.Int64, "amount": pl.Float64, "label": pl.String})
    frame = _frame(id=[1], amount=["2.5"], extra=["keep-me"])

    out = apply_schema_polars(frame, schema, SchemaMode.EVOLVE).collect()

    assert "extra" in out.columns
    assert out["id"].dtype == pl.Int64
    assert out["amount"].dtype == pl.Float64
    assert out["label"].dtype == pl.String


def test_datetime_with_timezone_cast_preserves_exact_type() -> None:
    schema = pl.Schema({"ts": pl.Datetime("ns", "UTC")})
    frame = (
        pl.DataFrame({"ts": [1_000_000]}).select(pl.col("ts").cast(pl.Datetime("us", "UTC"))).lazy()
    )

    out = apply_schema_polars(frame, schema, SchemaMode.STRICT).collect()
    assert out["ts"].dtype == pl.Datetime("ns", "UTC")


def test_struct_cast_is_recursive() -> None:
    schema = pl.Schema({"point": pl.Struct({"x": pl.Float64, "meta": pl.Struct({"z": pl.Int64})})})
    frame = pl.DataFrame({"point": [{"x": 1, "meta": {"z": "2"}}]}).lazy()

    out = apply_schema_polars(frame, schema, SchemaMode.STRICT)
    flat = out.select(
        pl.col("point").struct.field("x").alias("x"),
        pl.col("point").struct.field("meta").struct.field("z").alias("z"),
    ).collect()

    assert flat["x"].dtype == pl.Float64
    assert flat["z"].dtype == pl.Int64
    assert flat.row(0) == (1.0, 2)


def test_missing_struct_column_is_null_filled_recursively() -> None:
    schema = pl.Schema({"id": pl.Int64, "point": pl.Struct({"x": pl.Float64, "y": pl.Float64})})
    out = apply_schema_polars(_frame(id=[1]), schema, SchemaMode.STRICT).collect()

    assert out["point"][0] is None
    assert out["point"].dtype == pl.Struct({"x": pl.Float64, "y": pl.Float64})


def test_missing_list_column_is_null_filled_with_exact_type() -> None:
    schema = pl.Schema({"id": pl.Int64, "tags": pl.List(pl.String)})
    out = apply_schema_polars(_frame(id=[1]), schema, SchemaMode.STRICT).collect()

    assert out["tags"][0] is None
    assert out["tags"].dtype == pl.List(pl.String)


_POINT = pl.Struct({"x": pl.Float64})


@pytest.mark.parametrize("mode", [SchemaMode.STRICT, SchemaMode.EVOLVE])
@pytest.mark.parametrize(
    ("values", "frame_dtype", "table_dtype"),
    [
        ([{"x": 1.0}], _POINT, pl.String()),
        ([[1]], pl.List(pl.Int64), pl.String()),
        (['{"x": 1}'], pl.String(), _POINT),
        (["[1]"], pl.String(), pl.List(pl.Int64)),
        ([{"p": {"x": 1.0}}], pl.Struct({"p": _POINT}), pl.Struct({"p": pl.String})),
        ([[{"x": 1.0}]], pl.List(_POINT), pl.List(pl.String)),
    ],
    ids=[
        "struct to string",
        "list to string",
        "string to struct",
        "string to list",
        "nested struct to string",
        "list of structs to list of strings",
    ],
)
def test_a_cast_between_text_and_a_struct_or_list_is_refused_naming_the_column(
    mode: SchemaMode, values: list[object], frame_dtype: pl.DataType, table_dtype: pl.DataType
) -> None:
    frame = pl.DataFrame(
        {"id": [1], "payload": values}, schema={"id": pl.Int64, "payload": frame_dtype}
    )
    schema = pl.Schema({"id": pl.Int64, "payload": table_dtype})

    with pytest.raises(SchemaError, match=r"'payload'.*SchemaMode\.OVERWRITE"):
        apply_schema_polars(frame.lazy(), schema, mode)


def test_overwrite_writes_a_struct_over_a_string_column() -> None:
    frame = pl.DataFrame({"payload": [{"x": 1.0}]}).lazy()
    schema = pl.Schema({"payload": pl.String})

    assert apply_schema_polars(frame, schema, SchemaMode.OVERWRITE) is frame
