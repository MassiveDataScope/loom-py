"""Polars backend for the Loom ETL framework."""

from loom.etl.backends.polars._file_writer import PolarsFileWriter
from loom.etl.backends.polars._reader import PolarsSourceReader, decode_json_columns
from loom.etl.backends.polars._schema import PolarsPhysicalSchema, read_delta_physical_schema
from loom.etl.backends.polars._writer import PolarsTargetWriter

__all__ = [
    "PolarsFileWriter",
    "PolarsPhysicalSchema",
    "PolarsSourceReader",
    "PolarsTargetWriter",
    "decode_json_columns",
    "read_delta_physical_schema",
]
