"""Unit tests for FrameType ↔ PySpark DataType conversions."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

from loom.etl.engines.spark._dtype import (  # noqa: E402
    frame_type_to_spark,
    loom_to_spark,
    spark_to_frame_type,
)
from loom.etl.schema._schema import (  # noqa: E402
    ArrayType,
    DatetimeType,
    DecimalType,
    DurationType,
    FrameType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)
from tests.unit.etl.engines.frame_types import LAKE_FRAME_TYPES  # noqa: E402


class TestLoomToSpark:
    def test_int64_maps_to_long(self) -> None:
        assert loom_to_spark(LoomDtype.INT64) == T.LongType()

    def test_utf8_maps_to_string(self) -> None:
        assert loom_to_spark(LoomDtype.UTF8) == T.StringType()

    def test_scalar_primitives_have_mapping(self) -> None:
        # Only scalar dtypes have a direct Spark equivalent; composite types
        # (LIST, ARRAY, STRUCT, CATEGORICAL, ENUM) are handled by frame_type_to_spark.
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


class TestSparkToFrameType:
    def test_long_maps_to_int64(self) -> None:
        assert spark_to_frame_type(T.LongType()) is LoomDtype.INT64

    def test_unknown_type_maps_to_null(self) -> None:
        assert spark_to_frame_type(T.NullType()) is LoomDtype.NULL


class TestFrameTypeToSpark:
    def test_primitive(self) -> None:
        assert frame_type_to_spark(LoomDtype.BOOLEAN) == T.BooleanType()

    def test_list_type(self) -> None:
        result = frame_type_to_spark(ListType(LoomDtype.INT32))
        assert isinstance(result, T.ArrayType)
        assert result.elementType == T.IntegerType()

    def test_array_type(self) -> None:
        result = frame_type_to_spark(ArrayType(LoomDtype.FLOAT64, 4))
        assert isinstance(result, T.ArrayType)

    def test_struct_type(self) -> None:
        lt = StructType(fields=(StructField("id", LoomDtype.INT64),))
        result = frame_type_to_spark(lt)
        assert isinstance(result, T.StructType)
        assert result.fields[0].name == "id"

    def test_decimal_type(self) -> None:
        result = frame_type_to_spark(DecimalType(precision=10, scale=2))
        assert isinstance(result, T.DecimalType)
        assert result.precision == 10
        assert result.scale == 2

    @pytest.mark.parametrize(
        ("frame_type", "spark_type"),
        [
            (DatetimeType("us", "UTC"), T.TimestampType()),
            (DatetimeType("us", None), T.TimestampNTZType()),
        ],
        ids=["utc", "naive"],
    )
    def test_datetime_type_keeps_its_zone(
        self, frame_type: DatetimeType, spark_type: T.DataType
    ) -> None:
        assert frame_type_to_spark(frame_type) == spark_type

    def test_duration_type(self) -> None:
        assert frame_type_to_spark(DurationType()) == T.LongType()

    def test_unknown_type_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="No Spark mapping"):
            frame_type_to_spark(object())  # type: ignore[arg-type]


class TestSparkRoundTrip:
    @pytest.mark.parametrize("frame_type", LAKE_FRAME_TYPES.values(), ids=list(LAKE_FRAME_TYPES))
    def test_a_frame_type_survives_the_trip_through_spark(self, frame_type: FrameType) -> None:
        assert spark_to_frame_type(frame_type_to_spark(frame_type)) == frame_type

    @pytest.mark.parametrize("frame_type", LAKE_FRAME_TYPES.values(), ids=list(LAKE_FRAME_TYPES))
    def test_a_spark_type_survives_the_trip_through_a_frame_type(
        self, frame_type: FrameType
    ) -> None:
        spark_type = frame_type_to_spark(frame_type)

        assert frame_type_to_spark(spark_to_frame_type(spark_type)) == spark_type
