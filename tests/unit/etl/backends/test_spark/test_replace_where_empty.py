"""SparkTargetWriter with replace_where and a frame with no rows."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from loom.etl import ETLParams, IntoTable, col
from loom.etl.backends.spark import SparkTargetWriter
from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef

_REF = TableRef("staging.yearly")
_SCHEMA = "year BIGINT, v BIGINT"


class _DateParams(ETLParams):
    run_date: date


def test_an_empty_frame_deletes_the_matching_rows(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(2023, 10), (2024, 20)], _SCHEMA))
    spec = IntoTable(_REF).replace_where(col("year") == p.run_date.year)._to_spec()

    spark_writer.write(
        spark.createDataFrame([], _SCHEMA), spec, _DateParams(run_date=date(2024, 1, 1))
    )

    rows = spark.read.format("delta").load(str(path)).collect()
    assert [(row.year, row.v) for row in rows] == [(2023, 10)]
