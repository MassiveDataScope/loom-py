"""Unit tests for apply_schema_spark — native PySpark StructType contract."""

from __future__ import annotations

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from loom.etl import ETLParams, ETLStep, FromTable, IntoTable
from loom.etl.backends.spark._schema import apply_schema_spark
from loom.etl.declarative.target import SchemaMode
from loom.etl.schema import SchemaNotFoundError
from loom.etl.testing import ETLScenario

_ID_AMOUNT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), nullable=True),
        T.StructField("amount", T.DoubleType(), nullable=True),
    ]
)


def _snapshot(df: DataFrame) -> tuple[T.StructType, list[dict[str, object]]]:
    return df.schema, [row.asDict(recursive=True) for row in df.collect()]


@pytest.mark.parametrize("mode", [SchemaMode.STRICT, SchemaMode.EVOLVE])
def test_raises_schema_not_found_when_schema_is_none(mode: SchemaMode, spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    with pytest.raises(SchemaNotFoundError):
        apply_schema_spark(frame, None, mode)


def test_overwrite_passthrough_identity(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 1.0)], ["id", "amount"])
    assert apply_schema_spark(frame, None, SchemaMode.OVERWRITE) is frame


def test_strict_casts_fills_missing_and_drops_extra_columns(spark: SparkSession) -> None:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("amount", T.DoubleType()),
            T.StructField("label", T.StringType()),
        ]
    )
    frame = spark.createDataFrame([(1, "1.5", "drop-me")], ["id", "amount", "extra"])

    out = apply_schema_spark(frame, schema, SchemaMode.STRICT)
    out_schema, rows = _snapshot(out)
    row = rows[0]

    assert out_schema.fieldNames() == ["id", "amount", "label"]
    assert isinstance(out_schema["id"].dataType, T.LongType)
    assert isinstance(out_schema["amount"].dataType, T.DoubleType)
    assert isinstance(out_schema["label"].dataType, T.StringType)
    assert row["id"] == 1
    assert row["amount"] == pytest.approx(1.5)
    assert row["label"] is None


def test_evolve_casts_and_keeps_extra_columns(spark: SparkSession) -> None:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("amount", T.DoubleType()),
            T.StructField("label", T.StringType()),
        ]
    )
    frame = spark.createDataFrame([(1, "2.5", "keep-me")], ["id", "amount", "extra"])

    out = apply_schema_spark(frame, schema, SchemaMode.EVOLVE)
    out_schema, _ = _snapshot(out)

    assert "extra" in out_schema.fieldNames()
    assert isinstance(out_schema["id"].dataType, T.LongType)
    assert isinstance(out_schema["amount"].dataType, T.DoubleType)
    assert isinstance(out_schema["label"].dataType, T.StringType)


def test_struct_cast_is_recursive(spark: SparkSession) -> None:
    schema = T.StructType(
        [
            T.StructField(
                "point",
                T.StructType(
                    [
                        T.StructField("x", T.DoubleType()),
                        T.StructField(
                            "meta",
                            T.StructType([T.StructField("z", T.LongType())]),
                        ),
                    ]
                ),
            )
        ]
    )
    input_schema = T.StructType(
        [
            T.StructField(
                "point",
                T.StructType(
                    [
                        T.StructField("x", T.IntegerType(), nullable=True),
                        T.StructField(
                            "meta",
                            T.StructType([T.StructField("z", T.StringType(), nullable=True)]),
                            nullable=True,
                        ),
                    ]
                ),
                nullable=True,
            )
        ]
    )
    frame = spark.createDataFrame([((1, ("2",)),)], schema=input_schema)

    out = apply_schema_spark(frame, schema, SchemaMode.STRICT)
    out = out.select(
        F.col("point.x").alias("x"),
        F.col("point.meta.z").alias("z"),
    )
    out_schema, rows = _snapshot(out)
    row = rows[0]

    assert isinstance(out_schema["x"].dataType, T.DoubleType)
    assert isinstance(out_schema["z"].dataType, T.LongType)
    assert row["x"] == pytest.approx(1.0)
    assert row["z"] == 2


def test_missing_struct_column_is_null_filled_recursively(spark: SparkSession) -> None:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField(
                "point",
                T.StructType(
                    [
                        T.StructField("x", T.DoubleType()),
                        T.StructField("y", T.DoubleType()),
                    ]
                ),
            ),
        ]
    )
    frame = spark.createDataFrame([(1,)], ["id"])

    out = apply_schema_spark(frame, schema, SchemaMode.STRICT)
    out_schema, rows = _snapshot(out)
    row = rows[0]

    assert row["point"] is None
    assert isinstance(out_schema["point"].dataType, T.StructType)


def test_missing_array_column_is_null_filled_with_exact_type(spark: SparkSession) -> None:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("tags", T.ArrayType(T.StringType())),
        ]
    )
    frame = spark.createDataFrame([(1,)], ["id"])

    out = apply_schema_spark(frame, schema, SchemaMode.STRICT)
    out_schema, rows = _snapshot(out)
    row = rows[0]

    assert row["tags"] is None
    assert isinstance(out_schema["tags"].dataType, T.ArrayType)
    assert isinstance(out_schema["tags"].dataType.elementType, T.StringType)


ORDERS_SCENARIO = ETLScenario().with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])


def test_scenario_seeds_frame(step_runner, spark: SparkSession) -> None:  # type: ignore[no-untyped-def]
    """ETLScenario.apply() correctly seeds the runner and executes the step."""

    class NoParams(ETLParams):
        pass

    class DoubleStep(ETLStep[NoParams]):
        orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
        target = IntoTable("staging.orders").replace()

        def execute(self, params: NoParams, *, orders: DataFrame) -> DataFrame:  # type: ignore[override]
            return orders.withColumn("amount", F.col("amount") * 2)

    ORDERS_SCENARIO.apply(step_runner)
    result = step_runner.run(DoubleStep, NoParams())

    expected = spark.createDataFrame([(1, 20.0), (2, 40.0)], ["id", "amount"])
    expected_rows = [tuple(row) for row in expected.orderBy("id").collect()]
    actual_rows = list(result.to_polars().sort("id").iter_rows())
    assert actual_rows == expected_rows


def test_apply_schema_passes_with_matching_frame(spark: SparkSession) -> None:
    base_rows = [(1, 1.0)]
    columns = ["id", "amount"]

    frame_overwrite = spark.createDataFrame(base_rows, columns)
    result_overwrite = apply_schema_spark(frame_overwrite, None, SchemaMode.OVERWRITE)
    assert_df_equality(result_overwrite, frame_overwrite)

    frame_strict = spark.createDataFrame(base_rows, columns)
    result_strict = apply_schema_spark(frame_strict, _ID_AMOUNT_SCHEMA, SchemaMode.STRICT)
    assert_df_equality(result_strict, frame_strict, ignore_nullable=True)

    frame_evolve = spark.createDataFrame(base_rows, columns)
    result_evolve = apply_schema_spark(frame_evolve, _ID_AMOUNT_SCHEMA, SchemaMode.EVOLVE)
    assert_df_equality(result_evolve, frame_evolve, ignore_nullable=True)
