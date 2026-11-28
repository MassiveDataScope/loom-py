"""Fixtures shared by the engine tests of every backend."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.unit.etl.engines.doubles import ModelEngine, PolarsEngine, SparkEngine

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def polars_engine(tmp_path: Path) -> PolarsEngine:
    return PolarsEngine(tmp_path)


@pytest.fixture
def spark_engine(spark: SparkSession, tmp_path: Path) -> SparkEngine:
    return SparkEngine(spark, tmp_path)


@pytest.fixture(params=["polars_engine", "spark_engine"])
def engine(request: pytest.FixtureRequest) -> ModelEngine:
    return request.getfixturevalue(request.param)
