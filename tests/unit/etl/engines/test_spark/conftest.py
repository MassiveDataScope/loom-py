"""Fixtures for ETL Spark integration tests.

Requires ``pyspark>=3.5`` and ``delta-spark>=3.2`` to be installed.
The entire package is skipped automatically when either is absent.

SparkSession is scoped to the test session — Spark startup is expensive
(~5s) so we create it once and share across all tests in this package.
"""

from __future__ import annotations

from collections.abc import Callable, Generator
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from loom.etl.declarative.expr._refs import TableRef  # noqa: E402
from loom.etl.engines.spark import SparkSourceReader, SparkTargetWriter  # noqa: E402
from loom.etl.testing.spark import SparkStepRunner, SparkTestSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Local SparkSession with Delta Lake extensions.

    Scoped to the session to avoid the ~5s JVM startup cost per test.
    Log level set to ERROR to suppress verbose Spark output.
    """
    with SparkTestSession.start(app="loom-etl-spark-tests", parallelism=1) as session:
        yield session


@pytest.fixture
def step_runner(spark: SparkSession) -> SparkStepRunner:
    """Fresh SparkStepRunner per test — no Delta I/O, in-memory only."""
    return SparkStepRunner(spark)


@pytest.fixture
def spark_root(tmp_path: Path) -> Path:
    """Temporary Delta catalog root, cleaned up after each test by pytest's ``tmp_path``."""
    return tmp_path


@pytest.fixture
def spark_reader(spark: SparkSession, spark_root: Path) -> SparkSourceReader:
    """SparkSourceReader pointing at spark_root."""
    return SparkSourceReader(spark, spark_root)


@pytest.fixture
def spark_writer(
    spark: SparkSession,
    spark_root: Path,
) -> SparkTargetWriter:
    """SparkTargetWriter pointing at spark_root."""
    return SparkTargetWriter(spark, spark_root)


def spark_table_path(root: Path, ref: TableRef) -> Path:
    """Resolve a TableRef to a filesystem path under root."""
    return root.joinpath(*ref.ref.split("."))


@pytest.fixture
def seed_spark_table(
    spark: SparkSession,
    spark_root: Path,
) -> Callable[[str | TableRef, DataFrame], Path]:
    """Factory fixture that writes a Spark DataFrame as a real Delta table
    and registers its schema in the catalog.

    Returns:
        ``seed(ref, data) -> Path``
    """

    def _seed(ref: str | TableRef, data: DataFrame) -> Path:
        table_ref = TableRef(ref) if isinstance(ref, str) else ref
        path = spark_table_path(spark_root, table_ref)
        path.mkdir(parents=True, exist_ok=True)
        data.write.format("delta").mode("overwrite").save(str(path))
        return path

    return _seed
