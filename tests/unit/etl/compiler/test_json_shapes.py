"""A JSON shape declared with a leaf type JSON text cannot hold is refused at compile time."""

from __future__ import annotations

import msgspec
import pytest

from loom.etl import FromTable, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLErrorCode
from loom.etl.schema import (
    ArrayType,
    CategoricalType,
    DurationType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.compiler.probes import CompileDeclaration
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
    "binary-in-a-struct": (_in_struct(LoomDtype.BINARY), "$.v"),
    "time-in-a-list": (ListType(LoomDtype.TIME), "$[]"),
    "duration-in-an-array": (ArrayType(DurationType(), 2), "$[]"),
    "binary-deep": (ListType(_in_struct(LoomDtype.BINARY)), "$[].v"),
}


class TestJsonLeafTypes:
    @pytest.mark.parametrize(("frame_type", "path"), _UNSUPPORTED.values(), ids=list(_UNSUPPORTED))
    def test_a_parse_json_type_with_an_unsupported_leaf_is_refused_naming_its_path(
        self, compile_source: CompileDeclaration, frame_type: FrameType, path: str
    ) -> None:
        source = FromTable("raw.orders").parse_json("payload", frame_type)

        with pytest.raises(ETLCompilationError) as refused:
            compile_source(source)

        assert (refused.value.code, refused.value.field) == (
            ETLErrorCode.JSON_LEAF_UNSUPPORTED,
            path,
        )
        assert "orders.payload" in str(refused.value)

    def test_a_model_column_stored_as_a_struct_with_an_unsupported_leaf_is_refused(
        self, compile_target: CompileDeclaration
    ) -> None:
        target = IntoTable(probe_model(Sealed)).append()

        with pytest.raises(ETLCompilationError) as refused:
            compile_target(target)

        assert (refused.value.code, refused.value.field) == (
            ETLErrorCode.JSON_LEAF_UNSUPPORTED,
            "$.data",
        )

    def test_every_supported_leaf_compiles(self, compile_source: CompileDeclaration) -> None:
        supported = StructType(
            (
                StructField("n", LoomDtype.INT64),
                StructField("t", LoomDtype.UTF8),
                StructField("d", ListType(LoomDtype.DATE)),
                StructField("p", ArrayType(LoomDtype.FLOAT64, 2)),
            )
        )

        plan = compile_source(FromTable("raw.orders").parse_json("payload", supported))

        assert plan.fallible_json_columns
