"""ETL source declaration API.

Builders (user-facing):

* :class:`FromTable`  — Delta table source
* :class:`FromFile`   — file source (CSV, JSON, XLSX, Parquet)
* :class:`FromTemp`   — intermediate store source
* :class:`FromConfig` — config value injected into ``execute()`` (not a frame)
* :class:`FromMongo`  — MongoDB collection source
* :class:`FromDynamoDb` — DynamoDB table source
* :class:`FromClickHouse` — ClickHouse table/view source
* :class:`SourceRef`  — cross-step column reference for ``isin()``
* :class:`Sources`    — named group of sources
* :class:`SourceSet`  — reusable, extensible source group

Spec types (internal — compiler and executor only):

* :class:`TableSourceSpec`
* :class:`FileSourceSpec`
* :class:`TempSourceSpec`
* :class:`MongoSourceSpec`
* :class:`DynamoDbSourceSpec`
* :class:`ClickHouseSourceSpec`
* :data:`SourceSpec`   — union type alias
* :class:`SourceKind`  — physical kind enum
* :class:`ExtraFieldsMode` — handling of fields the schema does not cover
* :class:`JsonColumnSpec`
"""

from loom.etl.declarative.source._from import FromFile, FromTable, FromTemp, Sources, SourceSet
from loom.etl.declarative.source._from_clickhouse import FromClickHouse
from loom.etl.declarative.source._from_config import FromConfig
from loom.etl.declarative.source._from_dynamodb import FromDynamoDb
from loom.etl.declarative.source._from_mongo import FromMongo, SourceRef
from loom.etl.declarative.source._specs import (
    ClickHouseSourceSpec,
    DynamoDbSourceSpec,
    ExtraFieldsMode,
    FileSourceSpec,
    JsonColumnSpec,
    MongoSourceSpec,
    SourceKind,
    SourceSpec,
    TableSourceSpec,
    TempSourceSpec,
)

__all__ = [
    "FromTable",
    "FromFile",
    "FromTemp",
    "FromConfig",
    "FromMongo",
    "FromDynamoDb",
    "FromClickHouse",
    "SourceRef",
    "Sources",
    "SourceSet",
    "SourceSpec",
    "SourceKind",
    "ExtraFieldsMode",
    "TableSourceSpec",
    "FileSourceSpec",
    "TempSourceSpec",
    "MongoSourceSpec",
    "DynamoDbSourceSpec",
    "ClickHouseSourceSpec",
    "JsonColumnSpec",
]
