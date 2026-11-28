"""FrameType ↔ PySpark DataType conversion utilities.

Internal module — not part of the public API.
"""

from __future__ import annotations

from pyspark.sql import types as T

from loom.core.errors import InvalidType
from loom.etl.schema._schema import (
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

_LOOM_TO_SPARK: dict[LoomDtype, T.DataType] = {
    LoomDtype.INT8: T.ByteType(),
    LoomDtype.INT16: T.ShortType(),
    LoomDtype.INT32: T.IntegerType(),
    LoomDtype.INT64: T.LongType(),
    LoomDtype.UINT8: T.ShortType(),
    LoomDtype.UINT16: T.IntegerType(),
    LoomDtype.UINT32: T.LongType(),
    LoomDtype.UINT64: T.LongType(),
    LoomDtype.FLOAT32: T.FloatType(),
    LoomDtype.FLOAT64: T.DoubleType(),
    LoomDtype.UTF8: T.StringType(),
    LoomDtype.BINARY: T.BinaryType(),
    LoomDtype.BOOLEAN: T.BooleanType(),
    LoomDtype.DATE: T.DateType(),
    LoomDtype.DATETIME: T.TimestampType(),
    LoomDtype.DURATION: T.LongType(),
    LoomDtype.TIME: T.LongType(),
    LoomDtype.NULL: T.NullType(),
}

_SPARK_TO_LOOM: dict[type[T.DataType], LoomDtype] = {
    T.ByteType: LoomDtype.INT8,
    T.ShortType: LoomDtype.INT16,
    T.IntegerType: LoomDtype.INT32,
    T.LongType: LoomDtype.INT64,
    T.FloatType: LoomDtype.FLOAT32,
    T.DoubleType: LoomDtype.FLOAT64,
    T.StringType: LoomDtype.UTF8,
    T.BinaryType: LoomDtype.BINARY,
    T.BooleanType: LoomDtype.BOOLEAN,
    T.DateType: LoomDtype.DATE,
    T.NullType: LoomDtype.NULL,
}


def loom_to_spark(dtype: LoomDtype) -> T.DataType:
    """Return the PySpark DataType for a :class:`~loom.etl.schema.LoomDtype`.

    Args:
        dtype: Canonical Loom dtype.

    Returns:
        PySpark DataType instance.

    Raises:
        KeyError: If *dtype* has no Spark equivalent.
    """
    return _LOOM_TO_SPARK[dtype]


def spark_to_frame_type(dtype: T.DataType) -> FrameType:
    """Return the :data:`~loom.etl.schema.FrameType` for a PySpark DataType.

    Decimal precision and scale, timestamp zone, array elements and struct
    fields are kept; a timestamp reads as UTC and a tz-less one as naive.

    Args:
        dtype: PySpark DataType instance.

    Returns:
        The matching frame type, or :attr:`~loom.etl.schema.LoomDtype.NULL` if
        the Spark type has no direct mapping.
    """
    if isinstance(dtype, T.ArrayType):
        return ListType(spark_to_frame_type(dtype.elementType))
    if isinstance(dtype, T.StructType):
        return StructType(
            tuple(
                StructField(f.name, spark_to_frame_type(f.dataType), f.nullable)
                for f in dtype.fields
            )
        )
    if isinstance(dtype, T.DecimalType):
        return DecimalType(dtype.precision, dtype.scale)
    if isinstance(dtype, T.TimestampNTZType):
        return DatetimeType("us", None)
    if isinstance(dtype, T.TimestampType):
        return DatetimeType("us", "UTC")
    return _SPARK_TO_LOOM.get(type(dtype), LoomDtype.NULL)


def frame_type_to_spark(lt: FrameType) -> T.DataType:
    """Recursively convert a :data:`~loom.etl.schema.FrameType` to a PySpark DataType.

    Handles primitive :class:`~loom.etl.schema.LoomDtype` values as well as
    complex structural types such as :class:`~loom.etl.schema.ListType`,
    :class:`~loom.etl.schema.ArrayType`, :class:`~loom.etl.schema.StructType`,
    :class:`~loom.etl.schema.DecimalType`, :class:`~loom.etl.schema.DatetimeType`,
    and :class:`~loom.etl.schema.DurationType`.

    Args:
        lt: Any :data:`~loom.etl.schema.FrameType` value.

    Returns:
        Concrete PySpark DataType instance.

    Raises:
        InvalidType: When *lt* is not a recognized :data:`~loom.etl.schema.FrameType`.
    """
    if isinstance(lt, LoomDtype):
        return loom_to_spark(lt)
    if isinstance(lt, ListType):
        return T.ArrayType(frame_type_to_spark(lt.inner))
    if isinstance(lt, ArrayType):
        return T.ArrayType(frame_type_to_spark(lt.inner))
    if isinstance(lt, StructType):
        return T.StructType(
            [T.StructField(f.name, frame_type_to_spark(f.dtype)) for f in lt.fields]
        )
    if isinstance(lt, DecimalType):
        return T.DecimalType(lt.precision, lt.scale)
    if isinstance(lt, DatetimeType):
        return T.TimestampNTZType() if lt.time_zone is None else T.TimestampType()
    if isinstance(lt, DurationType):
        return T.LongType()
    raise InvalidType(f"No Spark mapping for FrameType {type(lt).__name__!r}")
