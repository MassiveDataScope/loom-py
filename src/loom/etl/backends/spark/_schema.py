"""Spark physical schema types and schema alignment."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from loom.etl.declarative.target import SchemaMode

_log = logging.getLogger(__name__)

__all__ = ["apply_schema_spark", "SparkPhysicalSchema"]


@dataclass(frozen=True)
class SparkPhysicalSchema:
    """Physical schema snapshot for a resolved Spark target.

    Args:
        schema: Native Spark StructType for write-time alignment.
        partition_columns: Ordered partition columns.
    """

    schema: T.StructType
    partition_columns: tuple[str, ...] = ()


def apply_schema_spark(
    frame: DataFrame,
    schema: T.StructType | None,
    mode: SchemaMode,
) -> DataFrame:
    from loom.etl.backends._schema_aligner import SchemaAlignmentPolicy
    from loom.etl.backends.spark._schema_aligner import SparkSchemaAligner

    policy = SchemaAlignmentPolicy(SparkSchemaAligner())
    return policy.align(frame, schema, mode)
