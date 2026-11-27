"""Shared fixtures for ETL unit tests."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from loom.etl import ETLParams

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


class OrdersParams(ETLParams):
    run_date: date
    countries: tuple[str, ...]


class CustomersParams(ETLParams):
    run_date: date


@pytest.fixture
def orders_params() -> OrdersParams:
    return OrdersParams(run_date=date(2024, 1, 5), countries=("ES", "FR"))


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Run reload-contract tests last to avoid cross-module identity drift.

    ``tests/unit/etl/test_module_contracts.py`` intentionally uses
    ``importlib.reload`` on ETL modules. Running those tests before other ETL
    unit tests can invalidate class/enum identity assumptions in already-loaded
    test modules.

    This hook keeps the suite deterministic by moving that file to the end of
    ETL unit collection.
    """

    target = Path("tests/unit/etl/test_module_contracts.py").as_posix()
    items.sort(key=lambda item: target in item.nodeid)
