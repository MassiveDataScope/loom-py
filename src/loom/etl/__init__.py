"""Loom ETL — declarative, compile-time-validated ETL framework.

Public API
----------

Authoring::

    from loom.etl import (
        ETLParams,
        ETLStep,
        ETLProcess,
        ETLPipeline,
        Sources,
        SourceSet,
        FromTable,
        FromFile,
        FromConfig,
        WithAgent,
        IntoTable,
        IntoFile,
        Format,
        TableRef,
        col,
        params,
    )

I/O protocols::

    from loom.etl import TableDiscovery, SourceReader, TargetWriter

Compilation::

    from loom.etl.compiler import ETLCompiler, ETLCompilationError

Testing stubs::

    from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter

Namespaced API (discoverable by bounded context)::

    from loom.etl import declarative, pipeline, runner, schema, storage, runtime

Internal modules (``_*.py``) and ``loom.etl.compiler._*`` are not part of
the public API and may change without notice.
"""

from loom.core.observability.config import OtelConfig
from loom.etl.backends._format_registry import UnsupportedFormatError
from loom.etl.checkpoint import (
    CheckpointCleaner,
    CheckpointScope,
    CheckpointStore,
)
from loom.etl.declarative import (
    CsvReadOptions,
    CsvWriteOptions,
    DeletePolicy,
    ExcelReadOptions,
    Format,
    FromConfig,
    FromFile,
    FromTable,
    FromTemp,
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
    JsonReadOptions,
    JsonWriteOptions,
    ParquetReadOptions,
    ParquetWriteOptions,
    ReadOptions,
    SchemaMode,
    Sources,
    SourceSet,
    WithAgent,
    WriteOptions,
)
from loom.etl.declarative.target._client import IntoClient
from loom.etl.executor import AgentMapper
from loom.etl.io import (
    ClickHouseClientExecutor,
    FromClickHouse,
    FromDynamoDb,
    FromMongo,
    IntoClickHouse,
    SourceRef,
)
from loom.etl.lineage._config import ETLObservabilityConfig, LineageConfig
from loom.etl.maintenance import (
    DeltaTableMaintainer,
    MaintainSchema,
    MaintainTable,
    MaintenanceError,
    MaintenanceReport,
    MaintenanceRunner,
    MaintenanceStep,
    OptimizeResult,
    TableMaintenanceResult,
    VacuumResult,
)
from loom.etl.pipeline import (
    ClientStep,
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    ParamExpr,
    StepSQL,
    params,
)
from loom.etl.runner import ETLRunner, InvalidStageError
from loom.etl.runtime.contracts import (
    AgentBatchRunner,
    ClientCommandExecutor,
    SourceReader,
    TableDiscovery,
    TargetWriter,
)
from loom.etl.schema import (
    ArrayType,
    CategoricalType,
    ColumnSchema,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    ListType,
    LoomDtype,
    LoomType,
    SchemaError,
    SchemaNotFoundError,
    StructField,
    StructType,
    TableRef,
    col,
)
from loom.etl.storage import (
    CatalogConnection,
    FilePathConfig,
    FileRoute,
    MaintenanceConfig,
    MaintenanceVacuumConfig,
    MappingLocator,
    PrefixLocator,
    StorageConfig,
    StorageDefaults,
    StorageEngine,
    TableLocation,
    TableLocator,
    TablePathConfig,
    TableRoute,
)

__all__ = [
    # params
    "ETLParams",
    # step / process / pipeline
    "ETLStep",
    "StepSQL",
    "ClientStep",
    "ETLProcess",
    "ETLPipeline",
    # runner
    "ETLRunner",
    "InvalidStageError",
    # sources
    "Sources",
    "SourceSet",
    "FromTable",
    "FromFile",
    "FromTemp",
    "FromConfig",
    "WithAgent",
    "AgentMapper",
    "FromClickHouse",
    "FromMongo",
    "FromDynamoDb",
    "SourceRef",
    # targets
    "IntoTable",
    "IntoFile",
    "IntoTemp",
    "IntoClickHouse",
    "IntoClient",
    "IntoHistory",
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
    # intermediates
    "CheckpointCleaner",
    "CheckpointScope",
    "CheckpointStore",
    # format
    "Format",
    # read options
    "ReadOptions",
    "CsvReadOptions",
    "JsonReadOptions",
    "ExcelReadOptions",
    "ParquetReadOptions",
    # write options
    "WriteOptions",
    "CsvWriteOptions",
    "JsonWriteOptions",
    "ParquetWriteOptions",
    # table / column refs
    "TableRef",
    "col",
    # params proxy
    "params",
    "ParamExpr",
    # I/O protocols
    "TableDiscovery",
    "SourceReader",
    "TargetWriter",
    "ClientCommandExecutor",
    "AgentBatchRunner",
    # ClickHouse client executor
    "ClickHouseClientExecutor",
    # storage locator
    "TableLocation",
    "TableLocator",
    "PrefixLocator",
    "MappingLocator",
    # storage config (YAML-loadable)
    "StorageEngine",
    "StorageConfig",
    "StorageDefaults",
    "CatalogConnection",
    "TablePathConfig",
    "TableRoute",
    "FilePathConfig",
    "FileRoute",
    "MaintenanceConfig",
    "MaintenanceVacuumConfig",
    # maintenance
    "MaintenanceStep",
    "MaintainTable",
    "MaintainSchema",
    "MaintenanceRunner",
    "MaintenanceReport",
    "MaintenanceError",
    "VacuumResult",
    "OptimizeResult",
    "TableMaintenanceResult",
    "DeltaTableMaintainer",
    # observability config (YAML-loadable)
    "ETLObservabilityConfig",
    "LineageConfig",
    "OtelConfig",
    # schema — primitive
    "SchemaMode",
    "ColumnSchema",
    "LoomDtype",
    "LoomType",
    "SchemaNotFoundError",
    "SchemaError",
    "UnsupportedFormatError",
    # schema — complex / structural types
    "ListType",
    "ArrayType",
    "StructType",
    "StructField",
    "DecimalType",
    "DatetimeType",
    "DurationType",
    "CategoricalType",
    "EnumType",
]
