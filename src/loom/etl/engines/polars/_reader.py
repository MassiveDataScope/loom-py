"""Polars source reader."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from loom.core.errors import InvalidType, InvalidUse

if TYPE_CHECKING:
    from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
    from loom.etl.io.sources._dynamodb import DynamoDBSourceReader
    from loom.etl.io.sources._mongo import MongoSourceReader

import polars as pl

from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import CsvReadOptions, ExcelReadOptions, JsonReadOptions
from loom.etl.declarative.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.declarative.source._specs import (
    ClickHouseSourceSpec,
    DynamoDBSourceSpec,
    JsonColumnSpec,
    JsonMismatch,
    MongoSourceSpec,
)
from loom.etl.engines._format_registry import resolve_format_handler
from loom.etl.engines._path_template import resolve_file_uri
from loom.etl.ports.contracts import SourceReader
from loom.etl.schema._schema import ColumnSchema, FrameType
from loom.etl.storage._file_locator import FileLocator
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import PathRouteResolver, PathTarget, TableRouteResolver

from ._dtype import frame_type_to_polars
from ._frame import FrameT
from ._json_failure import is_json_decode_failure
from ._predicate import predicate_to_polars
from ._tolerant_json import TolerantJson

_log = logging.getLogger(__name__)

_JSON_DECODING: Mapping[JsonMismatch, Callable[[pl.Expr, FrameType], pl.Expr]] = {
    JsonMismatch.FAIL: lambda column, frame_type: column.str.json_decode(
        frame_type_to_polars(frame_type)
    ),
    JsonMismatch.NULL: lambda column, frame_type: TolerantJson(frame_type).decode(column),
}


def decode_json_columns(frame: FrameT, json_columns: Iterable[JsonColumnSpec]) -> FrameT:
    """Decode JSON string columns the way :class:`PolarsSourceReader` does at read time.

    Each column named in *json_columns* holds JSON text and is replaced by the
    value decoded into its declared :class:`~loom.etl.schema.FrameType` by
    ``str.json_decode`` over the column cast to text, a native expression with
    no Python per row. A null or JSON ``null`` value decodes to null; with
    :attr:`~loom.etl.declarative.source.JsonMismatch.FAIL` a value that is not
    JSON of the declared type raises :exc:`polars.exceptions.ComputeError`.
    With ``NULL`` a value that is not JSON of the declared type is nulled
    field by field instead, in Python per row and only for a batch Polars
    refuses. Other columns pass through unchanged.

    Use it to reproduce in a test what a step receives from a source declared
    with ``parse_json``.

    Args:
        frame: Eager or lazy frame holding the JSON text columns.
        json_columns: Column declarations, e.g. ``FromTable(...).json_columns``
            or ``FromFile(...).json_columns``.

    Returns:
        A frame of the same kind as *frame*, or *frame* itself when
        *json_columns* is empty.

    Example::

        from loom.etl.engines.polars import decode_json_columns

        events = FromTable("raw.events").parse_json("payload", Payload)
        decoded = decode_json_columns(raw_frame, events.json_columns)
    """
    exprs = [
        _JSON_DECODING[jc.on_mismatch](pl.col(jc.column).cast(pl.String), jc.frame_type)
        for jc in json_columns
    ]
    if not exprs:
        return frame
    return frame.with_columns(exprs)


class PolarsSourceReader(SourceReader):
    """Read table, file, and MongoDB sources using Polars."""

    def __init__(
        self,
        locator: str | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
        file_locator: FileLocator | None = None,
        mongo_reader: MongoSourceReader | None = None,
        clickhouse_reader: ClickHouseSourceReader | None = None,
        dynamodb_reader: DynamoDBSourceReader | None = None,
    ) -> None:
        self._locator = _as_locator(locator)
        self._resolver = route_resolver or PathRouteResolver(self._locator)
        self._file_locator = file_locator
        self._mongo_reader: MongoSourceReader | None = mongo_reader
        self._clickhouse_reader: ClickHouseSourceReader | None = clickhouse_reader
        self._dynamodb_reader: DynamoDBSourceReader | None = dynamodb_reader

    def is_json_decode_failure(self, error: Exception) -> bool:
        """Whether *error* is Polars refusing a JSON document while decoding a column."""
        return is_json_decode_failure(error)

    def read(self, spec: SourceSpec, params_instance: Any, /) -> pl.LazyFrame:
        """Read source spec and return lazy frame."""
        if isinstance(spec, TableSourceSpec):
            return self._read_table(spec, params_instance)
        if isinstance(spec, FileSourceSpec):
            return self._read_file(spec, params_instance)
        if isinstance(spec, MongoSourceSpec):
            return self._read_mongo(spec, params_instance)
        if isinstance(spec, ClickHouseSourceSpec):
            return self._read_clickhouse(spec, params_instance)
        if isinstance(spec, DynamoDBSourceSpec):
            return self._read_dynamodb(spec, params_instance)
        raise InvalidType(
            f"PolarsSourceReader does not support source kind {spec.kind!r}. "
            "TEMP sources are handled by CheckpointStore."
        )

    def read_streaming(self, spec: SourceSpec, params_instance: Any, /) -> pl.LazyFrame:
        """Route streaming reads to the matching capability-aware sub-reader.

        Only ``ClickHouseSourceSpec`` is supported today; other kinds raise
        :class:`TypeError`. We refuse to silently fall back to ``read()``
        because a step that opts into streaming for memory safety must fail
        loudly rather than risk OOM on a non-streaming path.

        Args:
            spec: Compiled source specification.
            params_instance: Concrete params for current run.

        Returns:
            Lazy frame whose ``collect(engine="streaming")`` is memory-bounded.

        Raises:
            InvalidType: When *spec* is not a ClickHouse spec, or the matching
                sub-reader was not configured at construction time.
        """
        if isinstance(spec, ClickHouseSourceSpec):
            return self._read_clickhouse_streaming(spec, params_instance)
        raise InvalidType(
            f"PolarsSourceReader does not support streaming for source kind "
            f"{spec.kind!r}. Streaming is only available for ClickHouse sources."
        )

    def _read_mongo(self, spec: MongoSourceSpec, params_instance: Any) -> pl.LazyFrame:
        if self._mongo_reader is None:
            raise InvalidUse(
                "MongoSourceSpec requires a MongoSourceReader injected at construction time. "
                "Set mongo_reader= when constructing PolarsSourceReader."
            )
        frame: pl.LazyFrame = self._mongo_reader.read(spec, params_instance)
        return frame

    def _read_dynamodb(self, spec: DynamoDBSourceSpec, params_instance: Any) -> pl.LazyFrame:
        if self._dynamodb_reader is None:
            raise InvalidUse(
                "DynamoDBSourceSpec requires a DynamoDBSourceReader injected at "
                "construction time. "
                "Set dynamodb_reader= when constructing PolarsSourceReader."
            )
        frame: pl.LazyFrame = self._dynamodb_reader.read(spec, params_instance)
        return frame

    def _read_clickhouse(
        self,
        spec: ClickHouseSourceSpec,
        params_instance: Any,
    ) -> pl.LazyFrame:
        if self._clickhouse_reader is None:
            raise InvalidUse(
                "ClickHouseSourceSpec requires a ClickHouseSourceReader "
                "injected at construction time. "
                "Set clickhouse_reader= when constructing PolarsSourceReader."
            )
        frame: pl.LazyFrame = self._clickhouse_reader.read(spec, params_instance)
        return frame

    def _read_clickhouse_streaming(
        self,
        spec: ClickHouseSourceSpec,
        params_instance: Any,
    ) -> pl.LazyFrame:
        if self._clickhouse_reader is None:
            raise InvalidUse(
                "ClickHouseSourceSpec requires a ClickHouseSourceReader "
                "injected at construction time. "
                "Set clickhouse_reader= when constructing PolarsSourceReader."
            )
        frame: pl.LazyFrame = self._clickhouse_reader.read_streaming(spec, params_instance)
        return frame

    def execute_sql(self, frames: dict[str, Any], query: str) -> pl.LazyFrame:
        """Execute SQL query against backend frames."""
        return execute_sql(frames, query)

    def _read_table(self, spec: TableSourceSpec, params: Any) -> pl.LazyFrame:
        """Read Delta table."""
        target = self._resolver.resolve(spec.table_ref)
        if not isinstance(target, PathTarget):
            raise InvalidType(
                "PolarsSourceReader requires path routing; "
                f"got {type(target).__name__} for {spec.table_ref.ref!r}."
            )

        frame = pl.scan_delta(
            target.location.uri,
            storage_options=target.location.storage_options or None,
        )
        return _finalize_source_frame(frame, spec, params)

    def _read_file(self, spec: FileSourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Read file (CSV, JSON, XLSX, Parquet), resolving alias if needed."""
        path = self._resolve_file_path(spec, params_instance)
        frame = self._read_file_by_format(path, spec.format, spec.read_options)
        return _finalize_source_frame(frame, spec, params_instance)

    def _resolve_file_path(self, spec: FileSourceSpec, params_instance: Any) -> str:
        """Return the physical URI for *spec*, resolving alias and template."""
        return resolve_file_uri(
            spec.path,
            is_alias=spec.is_alias,
            locator=self._file_locator,
            params_instance=params_instance,
            role="FromFile",
        )

    @staticmethod
    def _read_file_by_format(path: str, format: Any, options: Any) -> pl.LazyFrame:
        """Dispatch to format-specific reader."""
        readers: dict[Format, Callable[[str, Any], pl.LazyFrame]] = {
            Format.CSV: PolarsSourceReader._read_csv,
            Format.JSON: PolarsSourceReader._read_json,
            Format.XLSX: PolarsSourceReader._read_xlsx,
            Format.PARQUET: PolarsSourceReader._read_parquet,
        }
        reader = resolve_format_handler(format, readers)
        return reader(path, options)

    @staticmethod
    def _read_csv(path: str, options: Any) -> pl.LazyFrame:
        csv_opts = options if isinstance(options, CsvReadOptions) else CsvReadOptions()
        kwargs: dict[str, Any] = {
            "separator": csv_opts.separator,
            "has_header": csv_opts.has_header,
            "encoding": csv_opts.encoding,
            "infer_schema_length": csv_opts.infer_schema_length,
            "skip_rows": csv_opts.skip_rows,
        }
        if csv_opts.null_values:
            kwargs["null_values"] = list(csv_opts.null_values)
        return pl.scan_csv(path, **kwargs)

    @staticmethod
    def _read_json(path: str, options: Any) -> pl.LazyFrame:
        json_opts = options if isinstance(options, JsonReadOptions) else JsonReadOptions()
        return pl.scan_ndjson(path, infer_schema_length=json_opts.infer_schema_length)

    @staticmethod
    def _read_xlsx(path: str, options: Any) -> pl.LazyFrame:
        excel_opts = options if isinstance(options, ExcelReadOptions) else ExcelReadOptions()
        if excel_opts.sheet_name is not None:
            df = pl.read_excel(
                path,
                sheet_name=excel_opts.sheet_name,
                has_header=excel_opts.has_header,
            )
        else:
            df = pl.read_excel(path, has_header=excel_opts.has_header)
        return df.lazy()

    @staticmethod
    def _read_parquet(path: str, _options: Any) -> pl.LazyFrame:
        return pl.scan_parquet(path)


def _finalize_source_frame(
    frame: pl.LazyFrame,
    spec: TableSourceSpec | FileSourceSpec,
    params_instance: Any,
) -> pl.LazyFrame:
    """Apply what *spec* declares on top of a raw scan, as :class:`PolarsSourceReader` does.

    Filters by the table predicates first, then projects ``columns``, casts the
    declared ``schema`` and decodes ``json_columns``.  Returns *frame* itself
    when the spec declares none of them.

    Args:
        frame: Raw frame scanned or seeded for the source.
        spec: Compiled table or file source spec.
        params_instance: Concrete params the predicates resolve against.

    Returns:
        The lazy frame the step receives.
    """
    if isinstance(spec, TableSourceSpec):
        for predicate in spec.predicates:
            frame = frame.filter(predicate_to_polars(predicate, params_instance))
    if spec.columns:
        frame = frame.select(list(spec.columns))
    if spec.read_schema:
        frame = _apply_source_schema(frame, spec.read_schema)
    if spec.json_columns:
        frame = decode_json_columns(frame, spec.json_columns)
    return frame


def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Cast declared columns to their Loom type."""
    cast_exprs = [pl.col(col.name).cast(frame_type_to_polars(col.dtype)) for col in schema]
    return frame.with_columns(cast_exprs)


def execute_sql(frames: dict[str, pl.LazyFrame], query: str) -> pl.LazyFrame:
    """Execute SQL query against lazy frames registered in a SQLContext."""
    ctx = pl.SQLContext()
    for name, frame in frames.items():
        ctx.register(name, frame)
    return ctx.execute(query, eager=False)


__all__ = [
    "PolarsSourceReader",
    "decode_json_columns",
    "execute_sql",
]
