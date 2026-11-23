"""LoomDtype ↔ PySpark DataType conversion utilities.

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
    ListType,
    LoomDtype,
    LoomType,
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
    LoomDtype.DECIMAL: T.DecimalType(38, 18),
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
    T.DecimalType: LoomDtype.DECIMAL,
    T.StringType: LoomDtype.UTF8,
    T.BinaryType: LoomDtype.BINARY,
    T.BooleanType: LoomDtype.BOOLEAN,
    T.DateType: LoomDtype.DATE,
    T.TimestampType: LoomDtype.DATETIME,
    T.TimestampNTZType: LoomDtype.DATETIME,
    T.NullType: LoomDtype.NULL,
}


def loom_to_spark(dtype: LoomDtype) -> T.DataType:
    """Return the PySpark DataType for a :class:`~loom.etl._schema.LoomDtype`.

    Args:
        dtype: Canonical Loom dtype.

    Returns:
        PySpark DataType instance.

    Raises:
        KeyError: If *dtype* has no Spark equivalent.
    """
    return _LOOM_TO_SPARK[dtype]


def spark_to_loom(dtype: T.DataType) -> LoomDtype:
    """Return the :class:`~loom.etl._schema.LoomDtype` for a PySpark DataType.

    Args:
        dtype: PySpark DataType instance.

    Returns:
        Canonical Loom dtype, or :attr:`~loom.etl._schema.LoomDtype.NULL` if
        the Spark type has no direct mapping.
    """
    return _SPARK_TO_LOOM.get(type(dtype), LoomDtype.NULL)


def loom_type_to_spark(lt: LoomType) -> T.DataType:
    """Recursively convert a :data:`~loom.etl._schema.LoomType` to a PySpark DataType.

    Handles primitive :class:`~loom.etl._schema.LoomDtype` values as well as
    complex structural types such as :class:`~loom.etl._schema.ListType`,
    :class:`~loom.etl._schema.ArrayType`, :class:`~loom.etl._schema.StructType`,
    :class:`~loom.etl._schema.DecimalType`, :class:`~loom.etl._schema.DatetimeType`,
    and :class:`~loom.etl._schema.DurationType`.

    Args:
        lt: Any :data:`~loom.etl._schema.LoomType` value.

    Returns:
        Concrete PySpark DataType instance.

    Raises:
        InvalidType: When *lt* is not a recognized :data:`~loom.etl._schema.LoomType`.
    """
    if isinstance(lt, LoomDtype):
        return loom_to_spark(lt)
    if isinstance(lt, ListType):
        return T.ArrayType(loom_type_to_spark(lt.inner))
    if isinstance(lt, ArrayType):
        return T.ArrayType(loom_type_to_spark(lt.inner))
    if isinstance(lt, StructType):
        return T.StructType([T.StructField(f.name, loom_type_to_spark(f.dtype)) for f in lt.fields])
    if isinstance(lt, DecimalType):
        return T.DecimalType(lt.precision or 38, lt.scale or 18)
    if isinstance(lt, DatetimeType):
        return T.TimestampType()
    if isinstance(lt, DurationType):
        return T.LongType()
    raise InvalidType(f"No Spark mapping for LoomType {type(lt).__name__!r}")
