"""Fixtures shared by the engine tests of every backend."""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Local SparkSession with Delta Lake extensions, skipped when pyspark or delta is absent.

    Scoped to the test session to avoid the ~5s JVM startup cost per test.
    """
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from loom.etl.testing.spark import SparkTestSession

    with SparkTestSession.start(app="loom-etl-spark-tests", parallelism=1) as session:
        yield session
