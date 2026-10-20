"""Loom canonical schema types — backend-agnostic column definitions.

These types form the schema contract between the ETL framework and any
catalog or writer implementation.  They carry no dependency on Polars,
Spark, or any specific storage backend.

Primitive types
---------------
Use :class:`LoomDtype` for scalars and simple temporals::

    ColumnSchema("id",     LoomDtype.INT64)
    ColumnSchema("amount", LoomDtype.FLOAT64)
    ColumnSchema("ts",     LoomDtype.DATETIME)   # coarse — any time unit / tz

Complex / parametrised types
-----------------------------
Use the rich structural classes for nested or parametrised columns::

    # Exact temporal parameters
    ColumnSchema("ts",    DatetimeType("us", "UTC"))
    ColumnSchema("delta", DurationType("ms"))
    ColumnSchema("price", DecimalType(precision=18, scale=4))

    # Collections
    ColumnSchema("tags",   ListType(inner=LoomDtype.UTF8))
    ColumnSchema("coords", ArrayType(inner=LoomDtype.FLOAT64, width=3))

    # Struct (record) — arbitrarily nested
    ColumnSchema("address", StructType(fields=(
        StructField("street", LoomDtype.UTF8),
        StructField("zip",    LoomDtype.UTF8),
        StructField("point",  StructType(fields=(
            StructField("lat", LoomDtype.FLOAT64),
            StructField("lon", LoomDtype.FLOAT64),
        ))),
    )))

    # List of structs
    ColumnSchema("events", ListType(inner=StructType(fields=(
        StructField("ts",   DatetimeType("us", "UTC")),
        StructField("kind", LoomDtype.UTF8),
    ))))

Coarse vs exact validation
---------------------------
When a :class:`ColumnSchema` uses a coarse :class:`LoomDtype` (e.g.
``LoomDtype.DATETIME``), validation accepts any parametrisation of that
type (any time unit, any timezone).  When a structural class is used
(e.g. :class:`DatetimeType`), validation enforces exact structural
equality including inner types.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class LoomDtype(StrEnum):
    """Canonical data types understood by loom's schema system.

    Values match Polars naming conventions to minimise translation friction
    in the Polars backend, while remaining backend-agnostic at the protocol
    level.

    For complex / parametrised types use the dedicated structural classes
    (:class:`ListType`, :class:`ArrayType`, :class:`StructType`,
    :class:`DecimalType`, :class:`DatetimeType`, :class:`DurationType`,
    :class:`CategoricalType`, :class:`EnumType`).  The enum members
    ``LIST``, ``STRUCT``, etc. are kept for coarse (unparameterised)
    schema declarations; they accept any inner type during validation.
    """

    # --- integer ---
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    UINT8 = "UInt8"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"

    # --- floating point ---
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
    DECIMAL = "Decimal"

    # --- string / binary ---
    UTF8 = "Utf8"
    BINARY = "Binary"

    # --- boolean ---
    BOOLEAN = "Boolean"

    # --- temporal ---
    DATE = "Date"
    DATETIME = "Datetime"
    DURATION = "Duration"
    TIME = "Time"

    # --- complex / structural (coarse, unparameterised) ---
    LIST = "List"
    ARRAY = "Array"
    STRUCT = "Struct"
    CATEGORICAL = "Categorical"
    ENUM = "Enum"

    # --- null / unknown ---
    NULL = "Null"


# ---------------------------------------------------------------------------
# Rich structural / parametrised type nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ListType:
    """Homogeneous list column — ``List[inner]``.

    Args:
        inner: Element type.  May itself be a :class:`StructType`,
               :class:`ListType`, or any other :data:`LoomType`.

    Example::

        ListType(inner=LoomDtype.UTF8)
        ListType(inner=StructType(fields=(StructField("x", LoomDtype.INT64),)))
    """

    inner: LoomType


@dataclass(frozen=True)
class ArrayType:
    """Fixed-width array column — ``Array[inner, width]``.

    Args:
        inner: Element type.
        width: Fixed number of elements per row.

    Example::

        ArrayType(inner=LoomDtype.FLOAT64, width=3)
    """

    inner: LoomType
    width: int


@dataclass(frozen=True)
class StructField:
    """A single named field within a :class:`StructType`.

    Args:
        name:     Field name.
        dtype:    Field type — any :data:`LoomType`.
        nullable: Whether the field accepts nulls.  Defaults to ``True``.
    """

    name: str
    dtype: LoomType
    nullable: bool = True


@dataclass(frozen=True)
class StructType:
    """Record / struct column with named fields.

    Args:
        fields: Ordered tuple of :class:`StructField` definitions.

    Example::

        StructType(fields=(
            StructField("lat", LoomDtype.FLOAT64),
            StructField("lon", LoomDtype.FLOAT64),
        ))
    """

    fields: tuple[StructField, ...]


@dataclass(frozen=True)
class DecimalType:
    """Fixed-precision decimal column.

    Args:
        precision: Total number of significant digits (``None`` = backend default).
        scale:     Number of digits after the decimal point (``None`` = backend default).

    Example::

        DecimalType(precision=18, scale=4)
    """

    precision: int | None = None
    scale: int | None = None


@dataclass(frozen=True)
class DatetimeType:
    """Datetime column with explicit time unit and optional timezone.

    Args:
        time_unit: ``"us"`` (microseconds, default), ``"ns"``, or ``"ms"``.
        time_zone: IANA timezone string (e.g. ``"UTC"``) or ``None`` for naive.

    Example::

        DatetimeType("us", "UTC")
        DatetimeType("ns")          # naive, nanosecond precision
    """

    time_unit: str = "us"
    time_zone: str | None = None


@dataclass(frozen=True)
class DurationType:
    """Duration (timedelta) column with explicit time unit.

    Args:
        time_unit: ``"us"`` (microseconds, default), ``"ns"``, or ``"ms"``.

    Example::

        DurationType("ms")
    """

    time_unit: str = "us"


@dataclass(frozen=True)
class CategoricalType:
    """Dictionary-encoded categorical string column.

    Example::

        ColumnSchema("region", CategoricalType())
    """


@dataclass(frozen=True)
class EnumType:
    """Fixed-vocabulary enum column.

    Args:
        categories: Ordered tuple of allowed string values.

    Example::

        EnumType(categories=("low", "medium", "high"))
    """

    categories: tuple[str, ...]


# ---------------------------------------------------------------------------
# LoomType — the full column-type language
# ---------------------------------------------------------------------------

#: Union of all valid column type representations.
#:
#: Use :class:`LoomDtype` for primitive types.  Use the structural
#: classes for complex, nested, or parametrised types.
LoomType = (
    LoomDtype
    | ListType
    | ArrayType
    | StructType
    | DecimalType
    | DatetimeType
    | DurationType
    | CategoricalType
    | EnumType
)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SchemaNotFoundError(Exception):
    """Raised when a write is attempted but no schema is registered for the table.

    Register the schema via :meth:`~loom.etl._io.TableDiscovery.update_schema`
    before the first write, or use a backend that creates the table explicitly
    (for example with :attr:`~loom.core.schema.SchemaMode.OVERWRITE` on first write).
    """


class SchemaError(Exception):
    """Raised when a frame is incompatible with the registered table schema."""


# ---------------------------------------------------------------------------
# ColumnSchema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnSchema:
    """Backend-agnostic definition of a single table column.

    Used by :class:`~loom.etl._io.TableDiscovery` to describe table schemas
    and by backend writers to align frames before writing.

    Args:
        name:     Column name.
        dtype:    Column type — any :data:`LoomType`.  Use :class:`LoomDtype`
                  for simple scalars; use the structural classes
                  (:class:`ListType`, :class:`StructType`, :class:`DatetimeType`,
                  etc.) for complex or parametrised columns.
        nullable: Whether the column accepts null values.  Defaults to ``True``.

    Example::

        schema = (
            ColumnSchema("order_id", LoomDtype.INT64, nullable=False),
            ColumnSchema("amount",   DecimalType(precision=18, scale=4)),
            ColumnSchema("tags",     ListType(inner=LoomDtype.UTF8)),
            ColumnSchema("address",  StructType(fields=(
                StructField("street", LoomDtype.UTF8),
                StructField("zip",    LoomDtype.UTF8),
            ))),
        )
    """

    name: str
    dtype: LoomType
    nullable: bool = True
