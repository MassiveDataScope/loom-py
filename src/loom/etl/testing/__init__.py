"""ETL testing utilities — stubs, runners, scenario, and pytest fixtures.

Public API
----------
* :class:`StubCatalog` — in-memory catalog for compile-time tests.
* :class:`StubSourceReader` — in-memory reader, keyed by table reference.
* :class:`StubTargetWriter` — in-memory writer that captures written frames.
* :class:`StubRunObserver` — in-memory observer that records lifecycle events.
* :class:`PolarsStepRunner` — Polars step harness, in memory or on a local
  Delta root (``delta_root=``).
* :class:`StepResult` — unified assertion surface for step output.
* :class:`MergeMetrics` — rows a Delta MERGE applied (``StepResult.merged``).
* :class:`ETLScenario` — backend-agnostic, immutable, composable seed dataset.
* :class:`StepRunnerProto` — protocol satisfied by both runner types.

Pytest fixtures
----------------------------------
* ``loom_polars_runner`` — fresh :class:`PolarsStepRunner` per test.
* ``loom_spark_runner``  — fresh :class:`SparkStepRunner` per test when
  :mod:`loom.etl.testing.spark` is explicitly imported or registered as a
  pytest plugin in the consuming suite.

Spark
-----
Import :mod:`loom.etl.testing.spark` explicitly to access
:class:`~loom.etl.testing.spark.SparkStepRunner` and
:class:`~loom.etl.testing.spark.SparkTestSession`.  That module is the only
place where PySpark is imported at module level — this file has zero PySpark
dependency.
"""

from __future__ import annotations

try:
    import pytest
except ModuleNotFoundError as exc:  # pragma: no cover - packaging guard
    raise ModuleNotFoundError(
        "loom.etl.testing defines pytest fixtures: install loom-kernel[testing]"
    ) from exc

from loom.etl.testing._result import MergeMetrics, StepResult
from loom.etl.testing._runners import PolarsStepRunner
from loom.etl.testing._scenario import ETLScenario, StepRunnerProto
from loom.etl.testing._stubs import (
    StubCatalog,
    StubRunObserver,
    StubSourceReader,
    StubTargetWriter,
)

__all__ = [
    "StubCatalog",
    "StubSourceReader",
    "StubTargetWriter",
    "StubRunObserver",
    "PolarsStepRunner",
    "StepResult",
    "MergeMetrics",
    "ETLScenario",
    "StepRunnerProto",
]


@pytest.fixture
def loom_polars_runner() -> PolarsStepRunner:
    """Fresh :class:`PolarsStepRunner` per test — no Delta I/O, in-memory only."""
    return PolarsStepRunner()
