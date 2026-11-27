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
        decodes_json_text: Whether a frame column of JSON text is decoded into the struct or
            array the schema declares, as it is for a model's schema.
    """

    schema: T.StructType
    partition_columns: tuple[str, ...] = ()
    decodes_json_text: bool = False


def apply_schema_spark(
    frame: DataFrame,
    schema: T.StructType | None,
    mode: SchemaMode,
    *,
    decodes_json_text: bool = False,
) -> DataFrame:
    from loom.etl.engines._schema_aligner import SchemaAlignmentPolicy
    from loom.etl.engines.spark._schema_aligner import SparkSchemaAligner

    policy = SchemaAlignmentPolicy(SparkSchemaAligner(decodes_json_text))
    return policy.align(frame, schema, mode)
