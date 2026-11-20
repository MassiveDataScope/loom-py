"""Polars backend for the Loom ETL framework."""

from loom.etl.engines.polars._dtype import loom_type_to_polars, polars_to_loom_type
from loom.etl.engines.polars._file_writer import PolarsFileWriter
from loom.etl.engines.polars._reader import PolarsSourceReader, decode_json_columns
from loom.etl.engines.polars._schema import PolarsPhysicalSchema, read_delta_physical_schema
from loom.etl.engines.polars._writer import PolarsTargetWriter

__all__ = [
    "PolarsFileWriter",
    "PolarsPhysicalSchema",
    "PolarsSourceReader",
    "PolarsTargetWriter",
    "decode_json_columns",
    "loom_type_to_polars",
    "polars_to_loom_type",
    "read_delta_physical_schema",
]
