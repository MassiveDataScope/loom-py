"""ETL external I/O connectors."""

from loom.etl.declarative.source import (
    ClickHouseSourceSpec,
    FromClickHouse,
    FromDynamoDB,
    FromMongo,
    SourceRef,
)
from loom.etl.declarative.source._specs import DynamoDBSourceSpec, MongoSourceSpec
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.io.sources._dynamodb import DynamoDBSourceReader
from loom.etl.io.sources._mongo import MongoSourceReader
from loom.etl.io.targets._clickhouse import (
    ClickHouseClientExecutor,
    ClickHouseTableSpec,
    ClickHouseTargetWriter,
    IntoClickHouse,
)

__all__ = [
    "ClickHouseClientExecutor",
    "ClickHouseSourceReader",
    "ClickHouseSourceSpec",
    "ClickHouseTableSpec",
    "ClickHouseTargetWriter",
    "DynamoDBSourceReader",
    "DynamoDBSourceSpec",
    "FromClickHouse",
    "FromDynamoDB",
    "FromMongo",
    "IntoClickHouse",
    "MongoSourceReader",
    "MongoSourceSpec",
    "SourceRef",
]
