"""Polars file target writer (CSV / JSON / PARQUET)."""

from __future__ import annotations

import logging
from collections.abc import Callable

import polars as pl

from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import (
    CsvWriteOptions,
    JsonWriteOptions,
    ParquetWriteOptions,
    WriteOptions,
)
from loom.etl.declarative.target._file import FileSpec
from loom.etl.engines._format_registry import resolve_format_handler, write_options_or_default

_log = logging.getLogger(__name__)


class PolarsFileWriter:
    """Write FILE targets with Polars DataFrame writers."""

    def write(self, frame: pl.LazyFrame, spec: FileSpec, *, streaming: bool = False) -> None:
        """Write *frame* to the path declared in *spec*."""
        if streaming:
            _log.debug("streaming write: sink_%s path=%s", spec.format, spec.path)
            sink = resolve_format_handler(spec.format, _STREAMING_WRITERS)
            sink(frame, spec.path, spec.write_options)
        else:
            write = resolve_format_handler(spec.format, _FILE_WRITERS)
            write(frame.collect(), spec.path, spec.write_options)


_Options = WriteOptions | None


def _write_csv_file(df: pl.DataFrame, path: str, options: _Options) -> None:
    opts = write_options_or_default(options, CsvWriteOptions)
    df.write_csv(
        path,
        separator=opts.separator,
        include_header=opts.has_header,
        **dict(opts.kwargs),
    )


def _write_parquet_file(df: pl.DataFrame, path: str, options: _Options) -> None:
    opts = write_options_or_default(options, ParquetWriteOptions)
    df.write_parquet(path, compression=opts.compression, **dict(opts.kwargs))


def _write_json_file(df: pl.DataFrame, path: str, options: _Options) -> None:
    opts = write_options_or_default(options, JsonWriteOptions)
    df.write_ndjson(path, **dict(opts.kwargs))


_DataFrameWriter = Callable[[pl.DataFrame, str, _Options], None]

_FILE_WRITERS: dict[Format, _DataFrameWriter] = {
    Format.CSV: _write_csv_file,
    Format.PARQUET: _write_parquet_file,
    Format.JSON: _write_json_file,
}


def _sink_csv_file(frame: pl.LazyFrame, path: str, options: _Options) -> None:
    opts = write_options_or_default(options, CsvWriteOptions)
    frame.sink_csv(
        path,
        separator=opts.separator,
        include_header=opts.has_header,
        **dict(opts.kwargs),
    )


def _sink_parquet_file(frame: pl.LazyFrame, path: str, options: _Options) -> None:
    opts = write_options_or_default(options, ParquetWriteOptions)
    frame.sink_parquet(path, compression=opts.compression, **dict(opts.kwargs))


def _sink_ndjson_file(frame: pl.LazyFrame, path: str, options: _Options) -> None:
    opts = write_options_or_default(options, JsonWriteOptions)
    frame.sink_ndjson(path, **dict(opts.kwargs))


_LazyFrameWriter = Callable[[pl.LazyFrame, str, _Options], None]

_STREAMING_WRITERS: dict[Format, _LazyFrameWriter] = {
    Format.CSV: _sink_csv_file,
    Format.PARQUET: _sink_parquet_file,
    Format.JSON: _sink_ndjson_file,
}
