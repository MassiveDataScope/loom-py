"""A Python annotation maps to the Loom type and Polars dtype a loom contract declares."""

from __future__ import annotations

import typing
from datetime import date, datetime

import msgspec
import polars as pl
import pytest

from loom.etl.backends.polars import loom_type_to_polars, polars_to_loom_type
from loom.etl.backends.polars._dtype import loom_type_to_polars as private_loom_type_to_polars
from loom.etl.schema import (
    ListType,
    LoomDtype,
    StructField,
    StructType,
    annotation_to_loom_type,
    resolve_schema,
)
from loom.etl.schema._contract import _annotation_to_loom_type


class _Line(msgspec.Struct):
    sku: str
    quantity: int


class _Order(msgspec.Struct):
    order_id: str
    placed_at: datetime
    lines: list[_Line]
    shipped_on: date | None = None


@pytest.mark.parametrize(
    ("annotation", "dtype"),
    [
        (int, pl.Int64()),
        (str | None, pl.String()),
        (datetime, pl.Datetime("us")),
        (list[float], pl.List(pl.Float64())),
        (_Line, pl.Struct({"sku": pl.String(), "quantity": pl.Int64()})),
    ],
)
def test_an_annotation_maps_to_the_polars_dtype_it_declares(
    annotation: object, dtype: pl.DataType
) -> None:
    assert loom_type_to_polars(annotation_to_loom_type(annotation)) == dtype


def test_each_field_maps_as_resolve_schema_maps_it() -> None:
    by_name = {column.name: column.dtype for column in resolve_schema(_Order)}

    for name, annotation in typing.get_type_hints(_Order).items():
        assert annotation_to_loom_type(annotation) == by_name[name]


def test_a_nested_annotation_round_trips_through_polars() -> None:
    loom_type = annotation_to_loom_type(list[_Line])

    assert loom_type == ListType(
        inner=StructType(
            fields=(
                StructField(name="sku", dtype=LoomDtype.UTF8),
                StructField(name="quantity", dtype=LoomDtype.INT64),
            )
        )
    )
    assert polars_to_loom_type(loom_type_to_polars(loom_type)) == loom_type


def test_an_annotation_without_a_loom_type_is_refused() -> None:
    with pytest.raises(TypeError, match="Cannot convert annotation"):
        annotation_to_loom_type(complex)


def test_the_private_names_give_the_same_result() -> None:
    loom_type = _annotation_to_loom_type(list[int])

    assert loom_type == annotation_to_loom_type(list[int])
    assert private_loom_type_to_polars(loom_type) == loom_type_to_polars(loom_type)
