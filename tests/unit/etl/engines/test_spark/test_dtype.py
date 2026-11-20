"""Unit tests for LoomType ↔ PySpark DataType conversions."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from loom.etl.engines.spark._dtype import (  # noqa: E402
    loom_to_spark,
    loom_type_to_spark,
    spark_to_loom,
)
from loom.etl.schema._schema import (  # noqa: E402
    ArrayType,
    DatetimeType,
    DecimalType,
    DurationType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)


class TestLoomToSpark:
    def test_int64_maps_to_long(self) -> None:
        assert loom_to_spark(LoomDtype.INT64) == T.LongType()

    def test_utf8_maps_to_string(self) -> None:
        assert loom_to_spark(LoomDtype.UTF8) == T.StringType()

    def test_scalar_primitives_have_mapping(self) -> None:
        # Only scalar dtypes have a direct Spark equivalent; composite types
        # (LIST, ARRAY, STRUCT, CATEGORICAL, ENUM) are handled by loom_type_to_spark.
        scalar = {
            LoomDtype.INT8,
            LoomDtype.INT16,
            LoomDtype.INT32,
            LoomDtype.INT64,
            LoomDtype.UINT8,
            LoomDtype.UINT16,
            LoomDtype.UINT32,
            LoomDtype.UINT64,
            LoomDtype.FLOAT32,
            LoomDtype.FLOAT64,
            LoomDtype.DECIMAL,
            LoomDtype.UTF8,
            LoomDtype.BINARY,
            LoomDtype.BOOLEAN,
            LoomDtype.DATE,
            LoomDtype.DATETIME,
            LoomDtype.DURATION,
            LoomDtype.TIME,
            LoomDtype.NULL,
        }
        for dtype in scalar:
            loom_to_spark(dtype)  # must not raise


class TestSparkToLoom:
    def test_long_maps_to_int64(self) -> None:
        assert spark_to_loom(T.LongType()) is LoomDtype.INT64

    def test_unknown_type_maps_to_null(self) -> None:
        assert spark_to_loom(T.NullType()) is LoomDtype.NULL


class TestLoomTypeToSpark:
    def test_primitive(self) -> None:
        assert loom_type_to_spark(LoomDtype.BOOLEAN) == T.BooleanType()

    def test_list_type(self) -> None:
        result = loom_type_to_spark(ListType(LoomDtype.INT32))
        assert isinstance(result, T.ArrayType)
        assert result.elementType == T.IntegerType()

    def test_array_type(self) -> None:
        result = loom_type_to_spark(ArrayType(LoomDtype.FLOAT64, 4))
        assert isinstance(result, T.ArrayType)

    def test_struct_type(self) -> None:
        lt = StructType(fields=(StructField("id", LoomDtype.INT64),))
        result = loom_type_to_spark(lt)
        assert isinstance(result, T.StructType)
        assert result.fields[0].name == "id"

    def test_decimal_type(self) -> None:
        result = loom_type_to_spark(DecimalType(precision=10, scale=2))
        assert isinstance(result, T.DecimalType)
        assert result.precision == 10
        assert result.scale == 2

    def test_datetime_type(self) -> None:
        assert loom_type_to_spark(DatetimeType()) == T.TimestampType()

    def test_duration_type(self) -> None:
        assert loom_type_to_spark(DurationType()) == T.LongType()

    def test_unknown_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="No Spark mapping"):
            loom_type_to_spark(object())  # type: ignore[arg-type]
