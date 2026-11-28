"""Spark physical schema types and schema alignment."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import cast

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from loom.etl.declarative.target import SchemaMode
from loom.etl.engines._schema_aligner import SchemaAlignmentPolicy
from loom.etl.engines.spark._dtype import frame_type_to_spark
from loom.etl.engines.spark._schema_aligner import SparkModelSchemaAligner, SparkSchemaAligner
from loom.etl.schema._schema import StructType

_log = logging.getLogger(__name__)

__all__ = ["apply_schema_spark", "SparkPhysicalSchema"]


@dataclass(frozen=True)
class SparkPhysicalSchema:
    """Physical schema snapshot for a resolved Spark target.

    Args:
        schema: Native Spark StructType for write-time alignment.
        partition_columns: Ordered partition columns.
        aligner: How a frame column is cast to the schema's type.
    """

    schema: T.StructType
    partition_columns: tuple[str, ...] = ()
    aligner: SparkSchemaAligner = field(default_factory=SparkSchemaAligner)

    @classmethod
    def of_model(cls, struct: StructType) -> SparkPhysicalSchema:
        """The physical schema of a model's *struct*: its fields in order, JSON text decoded."""
        return cls(
            schema=cast(T.StructType, frame_type_to_spark(struct)),
            aligner=SparkModelSchemaAligner(),
        )

    def align(self, frame: DataFrame, mode: SchemaMode) -> DataFrame:
        """Align *frame* with this schema under *mode*."""
        return SchemaAlignmentPolicy(self.aligner).align(frame, self.schema, mode)


def apply_schema_spark(
    frame: DataFrame,
    schema: T.StructType | None,
    mode: SchemaMode,
) -> DataFrame:
    return SchemaAlignmentPolicy(SparkSchemaAligner()).align(frame, schema, mode)
