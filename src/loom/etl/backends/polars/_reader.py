"""Polars source reader."""

from __future__ import annotations

import functools
import json
import logging
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
    from loom.etl.io.sources._dynamodb import DynamoDbSourceReader
    from loom.etl.io.sources._mongo import MongoSourceReader

import polars as pl

from loom.etl.backends._format_registry import resolve_format_handler
from loom.etl.backends._path_template import resolve_file_uri
from loom.etl.declarative._format import Format
from loom.etl.declarative._read_options import CsvReadOptions, ExcelReadOptions, JsonReadOptions
from loom.etl.declarative.source import FileSourceSpec, SourceSpec, TableSourceSpec
from loom.etl.declarative.source._specs import (
    ClickHouseSourceSpec,
    DynamoDbSourceSpec,
    JsonColumnSpec,
    MongoSourceSpec,
)
from loom.etl.runtime.contracts import SourceReader
from loom.etl.schema._schema import (
    ArrayType,
    ColumnSchema,
    ListType,
    LoomDtype,
    LoomType,
    StructType,
)
from loom.etl.storage._file_locator import FileLocator
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import PathRouteResolver, PathTarget, TableRouteResolver

from ._dtype import loom_type_to_polars
from ._predicate import predicate_to_polars

_log = logging.getLogger(__name__)

_FrameT = TypeVar("_FrameT", pl.DataFrame, pl.LazyFrame)


# ---------------------------------------------------------------------------
# Polymorphic JSON normalization
# ---------------------------------------------------------------------------


def _normalize_utf8_value(value: object) -> object:
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _normalize_struct_value(value: object, loom_type: StructType) -> object:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return None
    if not isinstance(value, dict):
        return None
    return {f.name: _normalize_to_schema(value.get(f.name), f.dtype) for f in loom_type.fields}


def _normalize_list_value(value: object, loom_type: ListType | ArrayType) -> object:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return None
    if not isinstance(value, list):
        return None
    return [_normalize_to_schema(item, loom_type.inner) for item in value]


_COMPLEX_NORMALIZERS: dict[type, Callable[[object, Any], object]] = {
    StructType: _normalize_struct_value,
    ListType: _normalize_list_value,
    ArrayType: _normalize_list_value,
}

_PRIMITIVE_COERCERS: dict[LoomDtype, Callable[[object], object]] = {
    LoomDtype.UTF8: _normalize_utf8_value,
}


def _normalize_to_schema(value: object, loom_type: LoomType) -> object:
    """Coerce *value* to match *loom_type*. Returns ``None`` on unrecoverable mismatch."""
    if value is None:
        return None
    handler = _COMPLEX_NORMALIZERS.get(type(loom_type))
    if handler is not None:
        return handler(value, loom_type)
    if isinstance(loom_type, LoomDtype):
        prim = _PRIMITIVE_COERCERS.get(loom_type)
        return prim(value) if prim is not None else value
    return value


def _normalize_json_string(json_str: str | None, *, loom_type: LoomType) -> str | None:
    """Parse *json_str*, coerce to *loom_type*, and re-serialize.

    Returns ``None`` when *json_str* is ``None``, unparseable, or coercion
    produces ``None``. After this step, ``str.json_decode(schema)`` always
    succeeds for the target column.
    """
    if json_str is None:
        return None
    try:
        parsed = json.loads(json_str)
    except (ValueError, TypeError):
        return None
    result = _normalize_to_schema(parsed, loom_type)
    if result is None:
        return None
    if result is parsed:
        return json_str  # no structural change — skip re-serialization
    return json.dumps(result, ensure_ascii=False)


def decode_json_columns(frame: _FrameT, json_columns: Iterable[JsonColumnSpec]) -> _FrameT:
    """Decode JSON string columns the way :class:`PolarsSourceReader` does at read time.

    Each column named in *json_columns* holds JSON text and is replaced by the
    value decoded into its declared :class:`~loom.etl.schema.LoomType`. Rows
    are first normalised to that type, so a field that is a string in some rows
    and an object in others decodes instead of raising
    :exc:`polars.exceptions.ComputeError`. A null or unparseable value decodes
    to null. Other columns pass through unchanged.

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

        from loom.etl.backends.polars import decode_json_columns

        events = FromTable("raw.events").parse_json("payload", Payload)
        decoded = decode_json_columns(raw_frame, events.json_columns)
    """
    exprs = [
        pl.col(jc.column)
        .map_elements(
            functools.partial(_normalize_json_string, loom_type=jc.loom_type),
            return_dtype=pl.String,
        )
        .str.json_decode(loom_type_to_polars(jc.loom_type))
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
        dynamodb_reader: DynamoDbSourceReader | None = None,
    ) -> None:
        self._locator = _as_locator(locator)
        self._resolver = route_resolver or PathRouteResolver(self._locator)
        self._file_locator = file_locator
        self._mongo_reader: MongoSourceReader | None = mongo_reader
        self._clickhouse_reader: ClickHouseSourceReader | None = clickhouse_reader
        self._dynamodb_reader: DynamoDbSourceReader | None = dynamodb_reader

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
        if isinstance(spec, DynamoDbSourceSpec):
            return self._read_dynamodb(spec, params_instance)
        raise TypeError(
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
            TypeError: When *spec* is not a ClickHouse spec, or the matching
                sub-reader was not configured at construction time.
        """
        if isinstance(spec, ClickHouseSourceSpec):
            return self._read_clickhouse_streaming(spec, params_instance)
        raise TypeError(
            f"PolarsSourceReader does not support streaming for source kind "
            f"{spec.kind!r}. Streaming is only available for ClickHouse sources."
        )

    def _read_mongo(self, spec: MongoSourceSpec, params_instance: Any) -> pl.LazyFrame:
        if self._mongo_reader is None:
            raise TypeError(
                "MongoSourceSpec requires a MongoSourceReader injected at construction time. "
                "Set mongo_reader= when constructing PolarsSourceReader."
            )
        frame: pl.LazyFrame = self._mongo_reader.read(spec, params_instance)
        return frame

    def _read_dynamodb(self, spec: DynamoDbSourceSpec, params_instance: Any) -> pl.LazyFrame:
        if self._dynamodb_reader is None:
            raise TypeError(
                "DynamoDbSourceSpec requires a DynamoDbSourceReader injected at "
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
            raise TypeError(
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
            raise TypeError(
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
            raise TypeError(
                "PolarsSourceReader requires path routing; "
                f"got {type(target).__name__} for {spec.table_ref.ref!r}."
            )

        frame = pl.scan_delta(
            target.location.uri,
            storage_options=target.location.storage_options or None,
        )

        for predicate in spec.predicates:
            frame = frame.filter(predicate_to_polars(predicate, params))

        return self._finalize_source_frame(frame, spec)

    def _read_file(self, spec: FileSourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Read file (CSV, JSON, XLSX, Parquet), resolving alias if needed."""
        path = self._resolve_file_path(spec, params_instance)
        frame = self._read_file_by_format(path, spec.format, spec.read_options)
        return self._finalize_source_frame(frame, spec)

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
        frame: pl.LazyFrame,
        spec: TableSourceSpec | FileSourceSpec,
    ) -> pl.LazyFrame:
        """Apply post-read transformations: columns, schema, json decode."""
        if spec.columns:
            frame = frame.select(list(spec.columns))
        if spec.schema:
            frame = self._apply_source_schema(frame, spec.schema)
        if spec.json_columns:
            frame = self._apply_json_decode(frame, spec.json_columns)
        return frame

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

    @staticmethod
    def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
        """Cast declared columns to their Loom type."""
        if not schema:
            return frame

        cast_exprs = [pl.col(col.name).cast(loom_type_to_polars(col.dtype)) for col in schema]
        return frame.with_columns(cast_exprs)

    @staticmethod
    def _apply_json_decode(frame: pl.LazyFrame, json_columns: tuple[Any, ...]) -> pl.LazyFrame:
        """Decode JSON string columns.

        Applies a per-row normalization pass before ``json_decode`` so that
        polymorphic fields (e.g. ``String`` in some rows, ``Object`` in others)
        are coerced to the declared :class:`~loom.etl.schema.LoomType` without
        raising :exc:`polars.exceptions.ComputeError`. See
        :func:`decode_json_columns`.
        """
        return decode_json_columns(frame, json_columns)


def _apply_source_schema(frame: pl.LazyFrame, schema: tuple[ColumnSchema, ...]) -> pl.LazyFrame:
    """Compatibility helper used by tests."""
    return PolarsSourceReader._apply_source_schema(frame, schema)


def _apply_json_decode(frame: pl.LazyFrame, json_columns: tuple[Any, ...]) -> pl.LazyFrame:
    """Compatibility alias of :func:`decode_json_columns`."""
    return decode_json_columns(frame, json_columns)


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
    "_apply_source_schema",
    "_apply_json_decode",
]
