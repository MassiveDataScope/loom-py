"""SparkTargetWriter honours IntoTable(...).replace(allow_empty=False)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from loom.etl import EmptyReplaceError, IntoTable
from loom.etl.backends.spark import SparkTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import SchemaMode
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage.routing import ResolvedTarget

from .conftest import spark_table_path

_REF = TableRef("silver.dim_dealer")
_SCHEMA = "id BIGINT, name STRING"


def _rows(spark: SparkSession, path: Path) -> list[tuple[int, str]]:
    frame = spark.read.format("delta").load(str(path)).orderBy("id")
    return [(row.id, row.name) for row in frame.collect()]


def test_an_empty_frame_leaves_the_existing_table_untouched(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a"), (2, "b")], _SCHEMA))
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    with pytest.raises(EmptyReplaceError, match="silver.dim_dealer"):
        spark_writer.write(spark.createDataFrame([], _SCHEMA), spec, None)

    assert _rows(spark, path) == [(1, "a"), (2, "b")]


def test_rows_replace_the_table_as_usual(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    spark_writer.write(spark.createDataFrame([(3, "c")], _SCHEMA), spec, None)

    assert _rows(spark, path) == [(3, "c")]


def _rows_while(spark: SparkSession, flag: Path) -> DataFrame:
    """A frame holding ``(3, "c")`` when computed while *flag* exists, and no row after."""
    flag_exists = F.udf(flag.exists, BooleanType()).asNondeterministic()
    return spark.createDataFrame([(3, "c")], _SCHEMA).where(flag_exists())


def test_the_write_uses_the_rows_the_check_counted(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    flag = tmp_path / "source-has-rows"
    flag.touch()
    replace = spark_writer._replace

    def empty_the_source_then_replace(
        frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode
    ) -> None:
        flag.unlink()
        replace(frame, target, schema_mode=schema_mode)

    monkeypatch.setattr(spark_writer, "_replace", empty_the_source_then_replace)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    spark_writer.write(_rows_while(spark, flag), spec, None)

    assert _rows(spark, path) == [(3, "c")]


def test_an_empty_frame_does_not_create_a_missing_table(
    spark: SparkSession,
    spark_root: Path,
) -> None:
    writer = SparkTargetWriter(
        spark, str(spark_root), missing_table_policy=MissingTablePolicy.CREATE
    )
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    with pytest.raises(EmptyReplaceError, match="silver.dim_dealer"):
        writer.write(spark.createDataFrame([], _SCHEMA), spec, None)

    assert not spark_table_path(spark_root, _REF).exists()


def test_a_streaming_frame_is_refused_before_the_table_is_touched(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    stream = spark.readStream.format("delta").load(str(path))
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    with pytest.raises(ValueError, match="streaming DataFrame") as raised:
        spark_writer.write(stream, spec, None)

    assert not isinstance(raised.value, EmptyReplaceError)
    assert _rows(spark, path) == [(1, "a")]


def test_a_streaming_frame_is_refused_before_a_missing_table_is_created(
    spark: SparkSession,
    spark_root: Path,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
) -> None:
    source = seed_spark_table("raw.dealers", spark.createDataFrame([(1, "a")], _SCHEMA))
    stream = spark.readStream.format("delta").load(str(source))
    writer = SparkTargetWriter(
        spark, str(spark_root), missing_table_policy=MissingTablePolicy.CREATE
    )
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    with pytest.raises(ValueError, match="streaming DataFrame"):
        writer.write(stream, spec, None)

    assert not spark_table_path(spark_root, _REF).exists()
