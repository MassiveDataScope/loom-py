"""Fixtures shared by the engine tests of every backend."""

from __future__ import annotations

import importlib.util
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
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


@pytest.fixture
def session_in_utc(engine: ModelEngine) -> Iterator[None]:
    """Pin the Spark session time zone to UTC, as loom runs it; Polars has no session zone."""
    if not isinstance(engine, SparkEngine):
        yield
        return
    previous = engine.spark.conf.get("spark.sql.session.timeZone")
    engine.spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield
    engine.spark.conf.set("spark.sql.session.timeZone", previous)


@pytest.fixture
def no_python_per_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any Python-per-row decode of a column fail, on Polars and on Spark."""

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a column was decoded by Python per row")

    monkeypatch.setattr(pl.Expr, "map_elements", refuse)
    if importlib.util.find_spec("pyspark") is not None:
        from pyspark.sql import functions

        monkeypatch.setattr(functions, "udf", refuse)
