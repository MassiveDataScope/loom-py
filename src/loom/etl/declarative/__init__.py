"""I/O declaration API for ETL sources, targets, and file formats."""

from __future__ import annotations

from ._format import Format
from ._read_options import (
    CsvReadOptions,
    ExcelReadOptions,
    JsonReadOptions,
    ParquetReadOptions,
    ReadOptions,
)
from ._with_agent import WithAgent
from ._write_options import CsvWriteOptions, JsonWriteOptions, ParquetWriteOptions, WriteOptions
from .source import (
    ClickHouseSourceSpec,
    FileSourceSpec,
    FromClickHouse,
    FromConfig,
    FromFile,
    FromMongo,
    FromTable,
    FromTemp,
    MongoSourceSpec,
    SourceKind,
    SourceRef,
    Sources,
    SourceSet,
    SourceSpec,
    TableSourceSpec,
    TempSourceSpec,
)
from .target import (
    DeletePolicy,
    HistorifyDateCollisionError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifyRepairReport,
    HistorifySpec,
    HistorifyTemporalConflictError,
    HistoryDateType,
    IntoFile,
    IntoHistory,
    IntoTable,
    IntoTemp,
    SchemaMode,
)

__all__ = [
    "Format",
    "ReadOptions",
    "CsvReadOptions",
    "JsonReadOptions",
    "ExcelReadOptions",
    "ParquetReadOptions",
    "WriteOptions",
    "CsvWriteOptions",
    "JsonWriteOptions",
    "ParquetWriteOptions",
    "SourceSpec",
    "SourceKind",
    "TableSourceSpec",
    "FileSourceSpec",
    "TempSourceSpec",
    "MongoSourceSpec",
    "ClickHouseSourceSpec",
    "FromTable",
    "FromFile",
    "FromTemp",
    "FromConfig",
    "WithAgent",
    "FromMongo",
    "FromClickHouse",
    "SourceRef",
    "Sources",
    "SourceSet",
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    "IntoHistory",
    "SchemaMode",
    # SCD2 enums
    "HistorifyInputMode",
    "DeletePolicy",
    "HistoryDateType",
    # SCD2 spec
    "HistorifySpec",
    # SCD2 report
    "HistorifyRepairReport",
    # SCD2 errors
    "HistorifyKeyConflictError",
    "HistorifyDateCollisionError",
    "HistorifyTemporalConflictError",
]
