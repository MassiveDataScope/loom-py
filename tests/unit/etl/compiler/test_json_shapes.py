"""A JSON shape declared with a leaf type JSON text cannot hold is refused at compile time."""

from __future__ import annotations

import msgspec
import pytest

from loom.etl import FromTable, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLErrorCode
from loom.etl.schema import (
    ArrayType,
    CategoricalType,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.compiler.probes import compile_source, compile_target
from tests.unit.etl.models import probe_model


class Sealed(msgspec.Struct):
    data: bytes


def _in_struct(leaf: FrameType) -> StructType:
    return StructType((StructField("v", leaf),))


_UNSUPPORTED = {
    "binary": (LoomDtype.BINARY, "$"),
    "time": (LoomDtype.TIME, "$"),
    "duration": (LoomDtype.DURATION, "$"),
    "duration-with-unit": (DurationType("ms"), "$"),
    "null": (LoomDtype.NULL, "$"),
    "categorical": (LoomDtype.CATEGORICAL, "$"),
    "categorical-type": (CategoricalType(), "$"),
    "coarse-list": (LoomDtype.LIST, "$"),
    "coarse-array": (LoomDtype.ARRAY, "$"),
    "coarse-struct": (LoomDtype.STRUCT, "$"),
    "coarse-enum": (LoomDtype.ENUM, "$"),
    "datetime-with-unknown-unit": (DatetimeType("s"), "$"),
    "binary-in-a-struct": (_in_struct(LoomDtype.BINARY), "$.v"),
    "time-in-a-list": (ListType(LoomDtype.TIME), "$[]"),
    "duration-in-an-array": (ArrayType(DurationType(), 2), "$[]"),
    "binary-deep": (ListType(_in_struct(LoomDtype.BINARY)), "$[].v"),
}


_SUPPORTED = {
    "integer": LoomDtype.INT64,
    "float": LoomDtype.FLOAT32,
    "boolean": LoomDtype.BOOLEAN,
    "string": LoomDtype.UTF8,
    "date": LoomDtype.DATE,
    "coarse-datetime": LoomDtype.DATETIME,
    "timestamp": DatetimeType("ns", "UTC"),
    "decimal": DecimalType(10, 2),
    "enum": EnumType(("low", "high")),
    "list-of-dates": ListType(LoomDtype.DATE),
    "array-of-floats": ArrayType(LoomDtype.FLOAT64, 2),
    "struct": _in_struct(LoomDtype.INT8),
}


class TestJsonLeafTypes:
    @pytest.mark.parametrize(("frame_type", "path"), _UNSUPPORTED.values(), ids=list(_UNSUPPORTED))
    def test_a_parse_json_type_with_an_unsupported_leaf_is_refused_naming_its_path(
        self, frame_type: FrameType, path: str
    ) -> None:
        source = FromTable("raw.orders").parse_json("payload", frame_type)

        with pytest.raises(ETLCompilationError) as refused:
            compile_source(source)

        assert (refused.value.code, refused.value.field) == (
            ETLErrorCode.JSON_LEAF_UNSUPPORTED,
            path,
        )

    def test_the_refusal_says_which_column_declares_which_leaf_where(self) -> None:
        source = FromTable("raw.orders").parse_json("payload", _in_struct(LoomDtype.BINARY))

        with pytest.raises(ETLCompilationError) as refused:
            compile_source(source)

        assert "orders.payload declares Binary at $.v, which" in str(refused.value)

    def test_a_model_column_stored_as_a_struct_with_an_unsupported_leaf_is_refused(self) -> None:
        target = IntoTable(probe_model(Sealed)).append()

        with pytest.raises(ETLCompilationError) as refused:
            compile_target(target)

        assert (refused.value.code, refused.value.field) == (
            ETLErrorCode.JSON_LEAF_UNSUPPORTED,
            "$.data",
        )

    @pytest.mark.parametrize("frame_type", _SUPPORTED.values(), ids=list(_SUPPORTED))
    def test_a_parse_json_type_with_a_supported_leaf_compiles(self, frame_type: FrameType) -> None:
        plan = compile_source(FromTable("raw.orders").parse_json("payload", frame_type))

        assert plan.fallible_json_columns
