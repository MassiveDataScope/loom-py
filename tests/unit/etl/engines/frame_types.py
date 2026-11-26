"""Every frame type the lake table produces, shared by the engine round-trip tests."""

from __future__ import annotations

from loom.etl.schema import (
    DatetimeType,
    DecimalType,
    ListType,
    LoomDtype,
    StructField,
    StructType,
)

_SCALARS = (
    LoomDtype.INT8,
    LoomDtype.INT16,
    LoomDtype.INT32,
    LoomDtype.INT64,
    LoomDtype.FLOAT64,
    LoomDtype.BOOLEAN,
    LoomDtype.UTF8,
    LoomDtype.BINARY,
    LoomDtype.DATE,
)
LAKE_FRAME_TYPES = {
    **{str(scalar): scalar for scalar in _SCALARS},
    "decimal-scale-0": DecimalType(10),
    "decimal": DecimalType(38, 4),
    "datetime-utc": DatetimeType("us", "UTC"),
    "datetime-naive": DatetimeType("us", None),
    "list": ListType(LoomDtype.UTF8),
    "struct": StructType(
        (
            StructField("id", LoomDtype.INT64),
            StructField("seen", DatetimeType("us", None)),
            StructField("tags", ListType(DecimalType(18, 2))),
        )
    ),
}
