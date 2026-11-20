"""PySpark backend for the Loom ETL framework.

Public API:
- SparkSourceReader: SourceReader protocol implementation
- SparkTargetWriter: TargetWriter protocol implementation
"""

from loom.etl.engines.spark._reader import SparkSourceReader
from loom.etl.engines.spark._schema import apply_schema_spark
from loom.etl.engines.spark._writer import SparkTargetWriter

__all__ = [
    "SparkSourceReader",
    "SparkTargetWriter",
    "apply_schema_spark",
]
