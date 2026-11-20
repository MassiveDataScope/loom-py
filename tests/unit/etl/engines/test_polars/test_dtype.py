"""Unit tests for LoomType <-> Polars DataType conversions."""

from __future__ import annotations

import polars as pl
import pytest

from loom.etl.engines.polars._dtype import (
    loom_to_polars,
    loom_type_to_polars,
    polars_to_loom,
    polars_to_loom_type,
)
from loom.etl.schema import (
    ArrayType,
    CategoricalType,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)


class TestLoomToPolarsCoarse:
    @pytest.mark.parametrize(
        ("loom_dtype", "polars_dtype"),
        [
            (LoomDtype.INT64, pl.Int64),
            (LoomDtype.UTF8, pl.String),
            (LoomDtype.BOOLEAN, pl.Boolean),
        ],
    )
    def test_primitive_loom_dtype_maps_to_polars_class(
        self,
        loom_dtype: LoomDtype,
        polars_dtype: type[pl.DataType],
    ) -> None:
        assert loom_to_polars(loom_dtype) is polars_dtype

    @pytest.mark.parametrize("loom_dtype", [LoomDtype.LIST, LoomDtype.ARRAY, LoomDtype.STRUCT])
    def test_coarse_structural_dtype_requires_rich_type(self, loom_dtype: LoomDtype) -> None:
        with pytest.raises(TypeError, match="structural type"):
            loom_to_polars(loom_dtype)


class TestPolarsToLoomType:
    @pytest.mark.parametrize(
        ("polars_dtype", "loom_dtype"),
        [
            (pl.Int64(), LoomDtype.INT64),
            (pl.List(pl.String), LoomDtype.LIST),
            (pl.Struct({"id": pl.Int64}), LoomDtype.STRUCT),
            (pl.Object(), LoomDtype.NULL),
        ],
    )
    def test_polars_to_loom_returns_coarse_dtype(
        self,
        polars_dtype: pl.DataType,
        loom_dtype: LoomDtype,
    ) -> None:
        assert polars_to_loom(polars_dtype) is loom_dtype

    def test_polars_to_loom_type_recurses_nested_structures(self) -> None:
        result = polars_to_loom_type(
            pl.Struct(
                {
                    "id": pl.Int64,
                    "events": pl.List(pl.Struct({"ts": pl.Datetime("ns", "UTC")})),
                }
            )
        )

        assert result == StructType(
            fields=(
                StructField("id", LoomDtype.INT64),
                StructField(
                    "events",
                    ListType(StructType(fields=(StructField("ts", DatetimeType("ns", "UTC")),))),
                ),
            )
        )

    @pytest.mark.parametrize(
        ("polars_dtype", "loom_type"),
        [
            (pl.Array(pl.Int16, 3), ArrayType(LoomDtype.INT16, 3)),
            (pl.Decimal(18, 4), DecimalType(18, 4)),
            (pl.Duration("ms"), DurationType("ms")),
            (pl.Categorical(), CategoricalType()),
            (pl.Enum(["a", "b"]), EnumType(("a", "b"))),
            (pl.Int8, LoomDtype.INT8),
        ],
    )
    def test_polars_to_loom_type_handles_rich_leaf_types(
        self,
        polars_dtype: pl.DataType,
        loom_type: object,
    ) -> None:
        assert polars_to_loom_type(polars_dtype) == loom_type


class TestLoomTypeToPolars:
    def test_loom_type_to_polars_recurses_nested_structures(self) -> None:
        result = loom_type_to_polars(
            StructType(
                fields=(
                    StructField("id", LoomDtype.INT64),
                    StructField("tags", ListType(LoomDtype.UTF8)),
                )
            )
        )

        assert result == pl.Struct(
            [pl.Field("id", pl.Int64()), pl.Field("tags", pl.List(pl.String()))]
        )

    @pytest.mark.parametrize(
        ("loom_type", "polars_dtype"),
        [
            (ArrayType(LoomDtype.FLOAT64, 2), pl.Array(pl.Float64(), 2)),
            (DecimalType(10, None), pl.Decimal(10, 0)),
            (DatetimeType("bad", "UTC"), pl.Datetime("us", "UTC")),
            (DurationType("bad"), pl.Duration("us")),
            (CategoricalType(), pl.Categorical()),
            (EnumType(("x", "y")), pl.Enum(["x", "y"])),
        ],
    )
    def test_loom_type_to_polars_handles_rich_leaf_types(
        self,
        loom_type: object,
        polars_dtype: pl.DataType,
    ) -> None:
        assert loom_type_to_polars(loom_type) == polars_dtype  # type: ignore[arg-type]

    def test_loom_type_to_polars_rejects_unknown_type(self) -> None:
        with pytest.raises(TypeError, match="Unknown LoomType"):
            loom_type_to_polars(object())  # type: ignore[arg-type]
