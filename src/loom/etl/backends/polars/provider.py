"""Polars backend provider for ETL runner wiring."""

from __future__ import annotations

from typing import Any, cast

from loom.core.connections.aws import AwsClientConfig, aws_island
from loom.core.connections.mongo import MongoConnection, mongo_island
from loom.etl.backends.polars._reader import PolarsSourceReader
from loom.etl.backends.polars._writer import PolarsTargetWriter
from loom.etl.io._registry import WriterRegistry
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.io.sources._dynamodb import DynamoDbSourceReader
from loom.etl.io.sources._mongo import MongoSourceReader
from loom.etl.io.targets._clickhouse import ClickHouseClientExecutor, ClickHouseTargetWriter
from loom.etl.lineage._config import LineageConfig
from loom.etl.lineage.sinks import RecordFrameTargetWriter, TargetLineageWriter
from loom.etl.runner._providers import BackendProvider
from loom.etl.runtime.contracts import ClientCommandExecutor, SourceReader, TargetWriter
from loom.etl.storage._config import CatalogConnection, StorageConfig
from loom.etl.storage._locator import MappingLocator, PrefixLocator, TableLocation, TableLocator


class PolarsProvider(BackendProvider):
    """Create Polars reader/writer pair from storage config."""

    def create_backends(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> tuple[SourceReader, TargetWriter]:
        _ = spark
        locator = _build_polars_locator(config)
        file_locator = config.to_file_locator()
        mongo_reader = (
            MongoSourceReader(_build_mongo_client(config.mongo), config.mongo.database)
            if config.mongo is not None
            else MongoSourceReader()
        )
        clickhouse_reader = ClickHouseSourceReader(connection=config.clickhouse)
        dynamodb_reader = (
            DynamoDbSourceReader(_build_dynamodb_client(config.dynamodb))
            if config.dynamodb is not None
            else DynamoDbSourceReader()
        )
        polars_reader = PolarsSourceReader(
            locator,
            file_locator=file_locator,
            mongo_reader=mongo_reader,
            clickhouse_reader=clickhouse_reader,
            dynamodb_reader=dynamodb_reader,
        )
        polars_writer = PolarsTargetWriter(
            locator,
            missing_table_policy=config.missing_table_policy,
            file_locator=file_locator,
            audit_config=config.audit,
        )
        writer: TargetWriter
        if config.clickhouse is not None:
            writer = WriterRegistry(
                polars_writer,
                extra={"clickhouse": ClickHouseTargetWriter(connection=config.clickhouse)},
            )
        else:
            writer = polars_writer
        return (polars_reader, writer)

    def create_lineage_writer(
        self,
        config: StorageConfig,
        lineage: LineageConfig,
        spark: Any = None,
    ) -> TargetLineageWriter:
        _ = (config, spark)
        if lineage.database:
            raise ValueError(
                "observability.lineage.database is only supported "
                "with storage.engine='spark'. "
                "For storage.engine='polars', configure observability.lineage.root."
            )

        locator = PrefixLocator(
            root=lineage.root,
            storage_options=lineage.storage_options or None,
            writer=lineage.writer or None,
            delta_config=lineage.delta_config or None,
            commit=lineage.commit or None,
        )
        target_writer = PolarsTargetWriter(
            locator,
            missing_table_policy=config.missing_table_policy,
        )
        return TargetLineageWriter(cast(RecordFrameTargetWriter, target_writer))

    def create_client_executor(
        self,
        config: StorageConfig,
        spark: Any = None,
    ) -> ClientCommandExecutor | None:
        _ = spark
        if config.clickhouse is not None:
            return ClickHouseClientExecutor(connection=config.clickhouse)
        return None


def _build_mongo_client(connection: MongoConnection) -> Any:
    """Build a PyMongo client for the ``storage.mongo`` connection.

    Raises:
        MissingExtraError: When the ``mongo`` extra is not installed.
    """
    return mongo_island().build_mongo_client(connection)


def _build_dynamodb_client(config: AwsClientConfig) -> Any:
    """Build a boto3 DynamoDB client for the ``storage.dynamodb`` settings.

    Raises:
        MissingExtraError: When the ``dynamodb`` extra is not installed.
    """
    return aws_island(extra="dynamodb").build_aws_client("dynamodb", config)


def _build_polars_locator(config: StorageConfig) -> TableLocator:
    mapping: dict[str, TableLocation] = {}
    for route in config.tables:
        if route.path is not None:
            mapping[route.name] = route.path.to_location()
            continue
        if route.ref.strip():
            qualified_ref, catalog_key = _qualify_polars_catalog_ref(
                config, route.ref, route.catalog
            )
            mapping[route.name] = TableLocation(
                uri=f"uc://{qualified_ref}",
                storage_options=_unity_storage_options(config, catalog_key),
            )

    default_path = config.defaults.table_path
    if mapping:
        default_location = default_path.to_location() if default_path is not None else None
        return MappingLocator(mapping=mapping, default=default_location)
    if default_path is not None:
        return PrefixLocator(
            root=default_path.uri,
            storage_options=default_path.storage_options or None,
            writer=default_path.writer or None,
            delta_config=default_path.delta_config or None,
            commit=default_path.commit or None,
        )
    raise ValueError(
        "storage.to_path_locator: no path routes configured. "
        "Define storage.defaults.table_path or add explicit storage.tables entries."
    )


def _qualify_polars_catalog_ref(
    config: StorageConfig, ref: str, catalog_key: str
) -> tuple[str, str]:
    parts = tuple(part for part in ref.split(".") if part)
    if len(parts) == 3:
        if catalog_key:
            return ref, catalog_key
        catalog_name = parts[0]
        return ref, catalog_name if catalog_name in config.catalogs else ""
    if len(parts) == 2:
        key = catalog_key or ("default" if "default" in config.catalogs else "")
        if not key:
            raise ValueError(
                "Polars UC routes with 2-part refs require route.catalog or "
                "storage.catalogs.default to build uc://catalog.schema.table."
            )
        return f"{key}.{ref}", key
    raise ValueError(
        f"Invalid catalog ref {ref!r}: expected 'schema.table' or 'catalog.schema.table'."
    )


def _unity_storage_options(config: StorageConfig, catalog_key: str) -> dict[str, str]:
    if not catalog_key:
        return {}
    connection = config.catalogs.get(catalog_key)
    if connection is None:
        return {}
    return _unity_storage_options_from_connection(connection)


def _unity_storage_options_from_connection(connection: CatalogConnection) -> dict[str, str]:
    options: dict[str, str] = {}
    if connection.workspace:
        options["databricks_workspace_url"] = connection.workspace
    if connection.token:
        options["databricks_access_token"] = connection.token
    return options


__all__ = ["PolarsProvider"]
