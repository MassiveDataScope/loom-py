"""Spark source reader - direct implementation without layers."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import CsvReadOptions, JsonReadOptions
from loom.etl.declarative.source import (
    FileSourceSpec,
    JsonMismatch,
    SourceSpec,
    TableSourceSpec,
)
from loom.etl.engines._format_registry import resolve_format_handler
from loom.etl.engines._path_template import resolve_file_uri
from loom.etl.engines._predicate import predicate_to_sql
from loom.etl.engines.spark._dtype import frame_type_to_spark
from loom.etl.engines.spark._json_failure import is_json_decode_failure
from loom.etl.ports.contracts import SourceReader
from loom.etl.schema._schema import ColumnSchema
from loom.etl.storage._file_locator import FileLocator
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import (
    CatalogRouteResolver,
    CatalogTarget,
    PathRouteResolver,
    TableRouteResolver,
)

_log = logging.getLogger(__name__)

_FROM_JSON_MODE: Mapping[JsonMismatch, str] = {
    JsonMismatch.FAIL: "FAILFAST",
    JsonMismatch.NULL: "PERMISSIVE",
}


class SparkSourceReader(SourceReader):
    """Spark source reader - reads Delta tables and files directly."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
        file_locator: FileLocator | None = None,
    ) -> None:
        self._spark = spark

        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                resolver = PathRouteResolver(_as_locator(locator))
        else:
            resolver = route_resolver

        self._resolver = resolver
        self._file_locator = file_locator

    def is_json_decode_failure(self, error: Exception) -> bool:
        """Whether *error* is a FAILFAST ``from_json`` rejecting a malformed document."""
        return is_json_decode_failure(error)

    def read(self, spec: SourceSpec, params_instance: Any, /) -> DataFrame:
        """Read source spec and return DataFrame."""
        if isinstance(spec, TableSourceSpec):
            return self._read_table(spec, params_instance)

        if isinstance(spec, FileSourceSpec):
            return self._read_file(spec, params_instance)

        raise InvalidType(
            f"SparkSourceReader does not support source kind {spec.kind!r}. "
            "TEMP sources are handled by CheckpointStore."
        )

    def execute_sql(self, frames: dict[str, Any], query: str) -> DataFrame:
        """Execute SQL query against backend frames."""
        return execute_sql(frames, query)

    def _read_table(self, spec: TableSourceSpec, params: Any) -> DataFrame:
        """Read Delta table."""
        target = self._resolver.resolve(spec.table_ref)

        if isinstance(target, CatalogTarget):
            df = self._spark.table(target.catalog_ref.ref)
        else:
            df = self._spark.read.format("delta").load(target.location.uri)

        for pred in spec.predicates:
            df = df.filter(predicate_to_sql(pred, params))

        return self._finalize_source_frame(df, spec)

    def _read_file(self, spec: FileSourceSpec, params_instance: Any) -> DataFrame:
        """Read file (CSV, JSON, Parquet), resolving alias if needed."""
        path = self._resolve_file_path(spec, params_instance)
        df = self._read_file_by_format(path, spec.format, spec.read_options)
        return self._finalize_source_frame(df, spec)

    def _resolve_file_path(self, spec: FileSourceSpec, params_instance: Any) -> str:
        """Return the physical URI for *spec*, resolving alias and template."""
        return resolve_file_uri(
            spec.path,
            is_alias=spec.is_alias,
            locator=self._file_locator,
            params_instance=params_instance,
            role="FromFile",
        )

    def _finalize_source_frame(
        self,
        df: DataFrame,
        spec: TableSourceSpec | FileSourceSpec,
    ) -> DataFrame:
        """Apply post-read transformations: columns, schema, json decode."""
        if spec.columns:
            df = df.select(list(spec.columns))
        if spec.read_schema:
            df = self._apply_source_schema(df, spec.read_schema)
        if spec.json_columns:
            df = self._apply_json_decode(df, spec.json_columns)
        return df

    def _read_file_by_format(self, path: str, format: Any, options: Any) -> DataFrame:
        """Dispatch to format-specific reader."""
        readers: dict[Format, Callable[[str, Any], DataFrame]] = {
            Format.DELTA: self._read_delta_file,
            Format.CSV: self._read_csv_file,
            Format.JSON: self._read_json_file,
            Format.PARQUET: self._read_parquet_file,
        }
        reader = resolve_format_handler(format, readers)
        return reader(path, options)

    def _read_delta_file(self, path: str, _options: Any) -> DataFrame:
        return self._spark.read.format("delta").load(path)

    def _read_csv_file(self, path: str, options: Any) -> DataFrame:
        csv_opts = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
        if csv_opts.skip_rows:
            raise InvalidValue("Spark backend does not support skip_rows for CSV files.")
        reader = (
            self._spark.read.option("sep", csv_opts.separator)
            .option("header", str(csv_opts.has_header).lower())
            .option("encoding", csv_opts.encoding)
            .option("inferSchema", "true")
        )
        if csv_opts.null_values:
            reader = reader.option("nullValue", csv_opts.null_values[0])
        if csv_opts.infer_schema_length is None:
            reader = reader.option("samplingRatio", "1.0")
        return reader.csv(path)

    def _read_json_file(self, path: str, options: Any) -> DataFrame:
        json_opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
        reader = self._spark.read.option("inferSchema", "true")
        if json_opts.infer_schema_length is None:
            reader = reader.option("samplingRatio", "1.0")
        return reader.json(path)

    def _read_parquet_file(self, path: str, _options: Any) -> DataFrame:
        return self._spark.read.parquet(path)

    def _apply_source_schema(self, df: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
        """Cast declared columns to their LoomDtype equivalents."""
        if not schema:
            return df

        for col in schema:
            df = df.withColumn(col.name, F.col(col.name).cast(frame_type_to_spark(col.dtype)))
        return df

    def _apply_json_decode(self, df: DataFrame, json_columns: tuple[Any, ...]) -> DataFrame:
        """Decode JSON string columns."""
        if not json_columns:
            return df

        for jc in json_columns:
            decoded = F.from_json(
                F.col(jc.column),
                frame_type_to_spark(jc.frame_type),
                {"mode": _FROM_JSON_MODE[jc.on_mismatch]},
            )
            df = df.withColumn(jc.column, decoded)
        return df


def execute_sql(frames: dict[str, DataFrame], query: str) -> DataFrame:
    """Execute SQL query against temporary views created from Spark frames."""
    first = next(iter(frames.values()), None)
    if first is None:
        raise InvalidValue("StepSQL requires at least one source frame.")

    isolated = first.sparkSession.newSession()
    for name, frame in frames.items():
        isolated.createDataFrame(frame.rdd, frame.schema).createOrReplaceTempView(name)
    return isolated.sql(query)


__all__ = ["SparkSourceReader", "execute_sql"]
