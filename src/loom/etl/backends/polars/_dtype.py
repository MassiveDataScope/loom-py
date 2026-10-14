"""LoomType ↔ Polars DataType conversion utilities.

Internal module — import ``loom_type_to_polars`` and ``polars_to_loom_type``
from :mod:`loom.etl.backends.polars`.

Two conversion layers
---------------------

``polars_to_loom`` / ``loom_to_polars``
    Coarse, class-level mapping.  Works only for primitive
    :class:`~loom.etl._schema.LoomDtype` values.  Structural types
    (``LIST``, ``STRUCT``, etc.) are recognised but ``loom_to_polars``
    raises :exc:`TypeError` for them because the inner type is unknown.

``polars_to_loom_type`` / ``loom_type_to_polars``
    Full recursive conversion.  Handles arbitrarily nested complex types
    such as ``List[Struct[...]]``, ``Struct[List[...]]``,
    ``Decimal(18,4)``, ``Datetime("us","UTC")``, etc.  These are the
    functions that :mod:`loom.etl.backends.polars._schema` and the reader
    use when working with rich :data:`~loom.etl._schema.LoomType` column
    definitions.
"""

from __future__ import annotations

from typing import Literal, cast

import polars as pl
from polars.datatypes import DataTypeClass

from loom.etl.schema._schema import (
    ArrayType,
    CategoricalType,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    ListType,
    LoomDtype,
    LoomType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Coarse primitive mapping tables
# ---------------------------------------------------------------------------

_LOOM_TO_POLARS: dict[LoomDtype, type[pl.DataType]] = {
    LoomDtype.INT8: pl.Int8,
    LoomDtype.INT16: pl.Int16,
    LoomDtype.INT32: pl.Int32,
    LoomDtype.INT64: pl.Int64,
    LoomDtype.UINT8: pl.UInt8,
    LoomDtype.UINT16: pl.UInt16,
    LoomDtype.UINT32: pl.UInt32,
    LoomDtype.UINT64: pl.UInt64,
    LoomDtype.FLOAT32: pl.Float32,
    LoomDtype.FLOAT64: pl.Float64,
    LoomDtype.DECIMAL: pl.Float64,  # coarse fallback — use DecimalType for precision/scale
    LoomDtype.UTF8: pl.String,
    LoomDtype.BINARY: pl.Binary,
    LoomDtype.BOOLEAN: pl.Boolean,
    LoomDtype.DATE: pl.Date,
    LoomDtype.DATETIME: pl.Datetime,
    LoomDtype.DURATION: pl.Duration,
    LoomDtype.TIME: pl.Time,
    LoomDtype.NULL: pl.Null,
}

_POLARS_TO_LOOM: dict[type[pl.DataType], LoomDtype] = {
    pl.Int8: LoomDtype.INT8,
    pl.Int16: LoomDtype.INT16,
    pl.Int32: LoomDtype.INT32,
    pl.Int64: LoomDtype.INT64,
    pl.UInt8: LoomDtype.UINT8,
    pl.UInt16: LoomDtype.UINT16,
    pl.UInt32: LoomDtype.UINT32,
    pl.UInt64: LoomDtype.UINT64,
    pl.Float32: LoomDtype.FLOAT32,
    pl.Float64: LoomDtype.FLOAT64,
    pl.Decimal: LoomDtype.DECIMAL,
    pl.String: LoomDtype.UTF8,
    pl.Binary: LoomDtype.BINARY,
    pl.Boolean: LoomDtype.BOOLEAN,
    pl.Date: LoomDtype.DATE,
    pl.Datetime: LoomDtype.DATETIME,
    pl.Duration: LoomDtype.DURATION,
    pl.Time: LoomDtype.TIME,
    # structural / complex types — coarse mapping
    pl.List: LoomDtype.LIST,
    pl.Array: LoomDtype.ARRAY,
    pl.Struct: LoomDtype.STRUCT,
    pl.Categorical: LoomDtype.CATEGORICAL,
    pl.Enum: LoomDtype.ENUM,
    pl.Null: LoomDtype.NULL,
}

# Structural LoomDtype members that carry inner-type information which
# cannot be expressed at the enum level.
_STRUCTURAL_DTYPES: frozenset[LoomDtype] = frozenset(
    {
        LoomDtype.LIST,
        LoomDtype.ARRAY,
        LoomDtype.STRUCT,
        LoomDtype.CATEGORICAL,
        LoomDtype.ENUM,
    }
)


# ---------------------------------------------------------------------------
# Coarse conversion (primitive LoomDtype only)
# ---------------------------------------------------------------------------


def loom_to_polars(dtype: LoomDtype) -> type[pl.DataType]:
    """Return the Polars DataType *class* for a primitive :class:`~loom.etl._schema.LoomDtype`.

    Args:
        dtype: Canonical Loom dtype.

    Returns:
        Polars DataType class (e.g. ``pl.Int64``).

    Raises:
        TypeError: If *dtype* is a structural type (``LIST``, ``ARRAY``,
                   ``STRUCT``, ``CATEGORICAL``, ``ENUM``).  Use
                   :func:`loom_type_to_polars` with :class:`~loom.etl._schema.ListType`,
                   :class:`~loom.etl._schema.StructType`, etc. instead.
        KeyError:  If *dtype* has no Polars equivalent.
    """
    if dtype in _STRUCTURAL_DTYPES:
        raise TypeError(
            f"LoomDtype.{dtype} is a structural type with unknown inner structure. "
            "Use the rich LoomType classes (ListType, StructType, ArrayType, …) "
            "together with loom_type_to_polars() to obtain a concrete Polars type."
        )
    return _LOOM_TO_POLARS[dtype]


def polars_to_loom(dtype: pl.DataType) -> LoomDtype:
    """Return the coarse :class:`~loom.etl._schema.LoomDtype` for a Polars DataType.

    Returns a class-level mapping only.  Complex parametrisation (inner types,
    precision, timezone) is discarded.  For full structural conversion use
    :func:`polars_to_loom_type`.

    Args:
        dtype: Polars DataType instance.

    Returns:
        Canonical Loom dtype, or :attr:`~loom.etl._schema.LoomDtype.NULL` when
        no mapping exists.
    """
    return _POLARS_TO_LOOM.get(type(dtype), LoomDtype.NULL)


# ---------------------------------------------------------------------------
# Rich recursive conversion
# ---------------------------------------------------------------------------


def polars_to_loom_type(dtype: pl.DataType | DataTypeClass) -> LoomType:
    """Recursively convert a Polars DataType to its :data:`~loom.etl._schema.LoomType`.

    Handles arbitrarily nested types — ``List[Struct[...]]``,
    ``Struct[List[...]]``, ``Decimal(18,4)``, ``Datetime("ns","UTC")``, etc.

    For primitive types the result is the corresponding :class:`~loom.etl._schema.LoomDtype`
    member (which is part of the :data:`~loom.etl._schema.LoomType` union).

    Args:
        dtype: Polars DataType instance.

    Returns:
        Matching :data:`~loom.etl._schema.LoomType` value.
    """
    if isinstance(dtype, pl.List):
        return ListType(inner=polars_to_loom_type(dtype.inner))
    if isinstance(dtype, pl.Array):
        return ArrayType(inner=polars_to_loom_type(dtype.inner), width=dtype.size)
    if isinstance(dtype, pl.Struct):
        fields = tuple(
            StructField(name=f.name, dtype=polars_to_loom_type(f.dtype)) for f in dtype.fields
        )
        return StructType(fields=fields)
    if isinstance(dtype, pl.Decimal):
        scale = dtype.scale if dtype.scale is not None else 0
        return DecimalType(precision=dtype.precision, scale=scale)
    if isinstance(dtype, pl.Datetime):
        return DatetimeType(time_unit=dtype.time_unit, time_zone=dtype.time_zone)
    if isinstance(dtype, pl.Duration):
        return DurationType(time_unit=dtype.time_unit)
    if isinstance(dtype, pl.Categorical):
        return CategoricalType()
    if isinstance(dtype, pl.Enum):
        return EnumType(categories=tuple(str(c) for c in dtype.categories))
    if isinstance(dtype, DataTypeClass):
        return _POLARS_TO_LOOM.get(cast(type[pl.DataType], dtype), LoomDtype.NULL)
    return _POLARS_TO_LOOM.get(type(dtype), LoomDtype.NULL)


def loom_type_to_polars(lt: LoomType) -> pl.DataType:
    """Recursively convert a :data:`~loom.etl._schema.LoomType` to a concrete Polars DataType.

    Returns an *instance* (not a class) so it can be used directly in
    ``pl.col(...).cast(...)`` and ``pl.lit(None).cast(...)``.

    Args:
        lt: Any :data:`~loom.etl._schema.LoomType` value.

    Returns:
        Concrete Polars DataType instance.

    Raises:
        TypeError: If *lt* is a coarse structural :class:`~loom.etl._schema.LoomDtype`
                   (e.g. ``LoomDtype.LIST``) whose inner type is unknown.
                   Use :class:`~loom.etl._schema.ListType`,
                   :class:`~loom.etl._schema.StructType`, etc. instead.
    """
    if isinstance(lt, LoomDtype):
        return _loom_primitive_to_polars(lt)
    if isinstance(lt, (DatetimeType, DurationType)):
        return _loom_temporal_to_polars(lt)
    if isinstance(lt, ListType):
        return pl.List(loom_type_to_polars(lt.inner))
    if isinstance(lt, ArrayType):
        return pl.Array(loom_type_to_polars(lt.inner), lt.width)
    if isinstance(lt, StructType):
        return pl.Struct([pl.Field(f.name, loom_type_to_polars(f.dtype)) for f in lt.fields])
    if isinstance(lt, DecimalType):
        return pl.Decimal(lt.precision, lt.scale if lt.scale is not None else 0)
    if isinstance(lt, CategoricalType):
        return pl.Categorical()
    if isinstance(lt, EnumType):
        return pl.Enum(list(lt.categories))
    raise TypeError(f"Unknown LoomType: {lt!r}")


def _loom_primitive_to_polars(lt: LoomDtype) -> pl.DataType:
    if lt in _STRUCTURAL_DTYPES:
        raise TypeError(
            f"LoomDtype.{lt} is a coarse structural type without inner-type information. "
            "Use the rich LoomType classes (ListType, StructType, ArrayType, …) "
            "to define complex column types."
        )
    return _LOOM_TO_POLARS[lt]()


def _loom_temporal_to_polars(lt: DatetimeType | DurationType) -> pl.DataType:
    time_unit = cast(
        Literal["ns", "us", "ms"],
        lt.time_unit if lt.time_unit in ("ns", "us", "ms") else "us",
    )
    if isinstance(lt, DurationType):
        return pl.Duration(time_unit)
    return pl.Datetime(time_unit, lt.time_zone)
