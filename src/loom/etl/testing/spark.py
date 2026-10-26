"""Spark testing utilities and pytest fixtures for the Loom ETL framework.

Provides :class:`SparkTestSession` — a context manager that handles Spark
configuration, Delta extension setup, and teardown.

Import :mod:`loom.etl.testing.spark` explicitly from Spark-enabled test
projects, or register it via ``pytest_plugins`` / ``pytest -p`` in the
consuming suite. The module keeps Spark-only dependencies out of import time so
non-Spark installations can remain lightweight.

Usage in ``conftest.py``::

    import pytest
    from loom.etl.testing.spark import SparkTestSession, SparkStepRunner

    @pytest.fixture(scope="session")
    def spark():
        with SparkTestSession.start(app="my-tests", parallelism=1) as session:
            yield session

    @pytest.fixture
    def step_runner(spark):
        return SparkStepRunner(spark)

Two environment variables tune where Spark finds its packages:
``LOOM_SPARK_IVY_DIR`` sets the Ivy cache directory, and ``LOOM_SPARK_OFFLINE=1``
resolves the Delta jars already present under ``~/.ivy2/jars`` instead of
fetching them from Maven.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any

try:
    import pytest
except ModuleNotFoundError as exc:  # pragma: no cover - packaging guard
    raise ModuleNotFoundError(
        "loom.etl.testing.spark defines pytest fixtures: install loom-kernel[testing]"
    ) from exc

from loom.core.config import ConfigContext
from loom.core.environment import SPARK_IVY_DIR_VARIABLE, SPARK_OFFLINE_VARIABLE, loom_variable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.source import SourceSpec
from loom.etl.declarative.target import TargetSpec
from loom.etl.executor import ETLExecutor
from loom.etl.testing._result import StepResult

if TYPE_CHECKING:
    import polars as pl
    from pyspark.sql import SparkSession


class SparkTestSession:
    """Context manager for a local PySpark + Delta test session.

    Handles Delta extension configuration and clean teardown.

    Use as a context manager to guarantee ``spark.stop()`` is always called::

        with SparkTestSession.start(app="etl-tests") as spark:
            df = spark.createDataFrame([(1, "a")], ["id", "label"])

    Args:
        session: Active :class:`pyspark.sql.SparkSession`.
    """

    def __init__(self, session: SparkSession) -> None:
        self._session = session

    @classmethod
    def start(
        cls,
        *,
        app: str = "loom-etl-test",
        parallelism: int = 1,
        memory: str = "1g",
        snapshot_partitions: int = 1,
        ivy_dir: str | Path | None = None,
    ) -> SparkTestSession:
        """Create and return a configured local SparkSession.

        Args:
            app:         Spark application name shown in the UI.
            parallelism: ``spark.sql.shuffle.partitions`` — keep low (1–2) for
                         unit tests to reduce overhead.
            memory:      Driver heap size string (e.g. ``"1g"``).
            snapshot_partitions: ``spark.databricks.delta.snapshotPartitions`` — the
                         number of tasks Delta uses to reconstruct a table's log
                         snapshot. Keep at 1 for test-sized tables; the Delta
                         default of 50 costs a task per partition on every read.
            ivy_dir:     Optional Ivy cache directory for Maven package
                         resolution. When omitted in a constrained sandbox,
                         a writable temp directory is selected automatically.

        Returns:
            A :class:`SparkTestSession` wrapping the active session.
        """
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .appName(app)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.driver.memory", memory)
            .config("spark.sql.shuffle.partitions", str(parallelism))
            .config("spark.databricks.delta.snapshotPartitions", str(snapshot_partitions))
        )
        resolved_ivy_dir = _resolve_ivy_dir(ivy_dir)
        if resolved_ivy_dir is not None:
            resolved_ivy_dir.mkdir(parents=True, exist_ok=True)
            builder = builder.config("spark.jars.ivy", str(resolved_ivy_dir))
        local_delta_jars = _resolve_local_delta_jars()
        if _offline_jars_requested() and local_delta_jars is not None:
            builder = builder.config("spark.jars", ",".join(str(jar) for jar in local_delta_jars))
            session = builder.getOrCreate()
        else:
            from delta import configure_spark_with_delta_pip

            session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        return cls(session)

    def __enter__(self) -> SparkSession:
        return self._session

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._session.stop()

    @property
    def session(self) -> SparkSession:
        """The underlying :class:`pyspark.sql.SparkSession`."""
        return self._session


# ---------------------------------------------------------------------------
# SparkStepRunner — in-memory Spark step test harness
# ---------------------------------------------------------------------------


class _SparkCapturingWriter:
    def __init__(self) -> None:
        self.frame: Any = None
        self.spec: TargetSpec | None = None

    def write(
        self,
        frame: Any,
        spec: TargetSpec,
        _params_instance: Any,
        *,
        streaming: bool = False,
        write_ctx: Any = None,
    ) -> None:
        _ = (streaming, write_ctx)
        self.frame = frame
        self.spec = spec


class _SparkStubReader:
    def __init__(self, frames: dict[str, Any]) -> None:
        self._frames = frames

    def read(
        self,
        spec: SourceSpec,
        _params_instance: Any,
        /,
    ) -> Any:
        table_ref = getattr(spec, "table_ref", None)
        key = table_ref.ref if table_ref is not None else spec.alias
        return self._frames[key]

    def execute_sql(self, frames: dict[str, Any], query: str, /) -> Any:
        first = next(iter(frames.values()), None)
        if first is None:
            raise ValueError("StepSQL requires at least one source frame.")
        isolated = first.sparkSession.newSession()
        for name, frame in frames.items():
            isolated.createDataFrame(frame.rdd, frame.schema).createOrReplaceTempView(name)
        return isolated.sql(query)


class SparkStepRunner:
    """In-memory test harness for Spark :class:`~loom.etl.ETLStep` subclasses.

    Seeds are plain Python tuples — no Spark dependency at definition time.
    Internally, each seed is converted to a ``pyspark.sql.DataFrame`` so
    ``execute()`` receives the same type as in production.

    No Delta I/O — reads and writes are captured in memory.

    Example::

        def test_aggregate(loom_spark_runner):
            loom_spark_runner.seed("raw.orders", [(1, 10.0)], ["id", "amount"])
            result = loom_spark_runner.run(AggregateStep, NoParams())
            result.assert_count(1)
            result.show()
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._seeds: dict[str, tuple[list[tuple[Any, ...]], list[str]]] = {}
        self._writer = _SparkCapturingWriter()
        self._config_context: ConfigContext | None = None

    def seed(
        self,
        ref: str,
        data: list[tuple[Any, ...]],
        columns: list[str],
    ) -> SparkStepRunner:
        """Register raw data under the logical table reference *ref*.

        Args:
            ref:     Logical table reference, e.g. ``"raw.orders"``.
            data:    Row data as a list of tuples.
            columns: Column names aligned with the tuple positions.

        Returns:
            ``self`` for fluent chaining.
        """
        self._seeds[ref] = (list(data), list(columns))
        return self

    def with_config(self, values: Mapping[str, Any]) -> SparkStepRunner:
        """Set the config the step's ``FromConfig`` values resolve from.

        Keys are validated when :meth:`run` compiles the step, exactly as the
        runner does, so a fake token exercises the production path.

        Args:
            values: Config mapping, e.g. ``{"respondio": {"api_token": "fake"}}``.

        Returns:
            ``self`` for fluent chaining.
        """
        self._config_context = ConfigContext.from_dict(values)
        return self

    def run(self, step_cls: type[Any], params: Any) -> StepResult:
        """Compile and execute *step_cls* against the seeded tables.

        Args:
            step_cls: :class:`~loom.etl.ETLStep` subclass to execute.
            params:   Concrete params instance for this run.

        Returns:
            :class:`~loom.etl.testing._result.StepResult` for assertions.

        Raises:
            KeyError:     When a source table was not seeded.
            ETLCompilationError: When a ``FromConfig`` key is not in
                :meth:`with_config`.
            RuntimeError: When the step produced no output.
        """
        frames = {
            ref: self._spark.createDataFrame(data, columns)
            for ref, (data, columns) in self._seeds.items()
        }
        plan = ETLCompiler(config_context=self._config_context).compile_step(step_cls)
        self._writer = _SparkCapturingWriter()
        ETLExecutor(
            _SparkStubReader(frames), self._writer, config_context=self._config_context
        ).run_step(plan, params)
        raw = self._writer.frame
        if raw is None:
            raise RuntimeError("Step produced no output — check that target is declared.")
        return StepResult(_spark_frame_to_polars(raw))

    @property
    def target_spec(self) -> TargetSpec:
        """Target spec from the last :meth:`run` call.

        Raises:
            RuntimeError: When :meth:`run` has not been called yet.
        """
        if self._writer.spec is None:
            raise RuntimeError("No spec — call run() first.")
        return self._writer.spec


# ---------------------------------------------------------------------------
# pytest fixture — opt-in via explicit import or pytest plugin registration
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def loom_spark_session() -> Generator[SparkSession, None, None]:
    """Local SparkSession with Delta Lake extensions.

    Scoped to the test session to amortise the ~5 s JVM startup cost.
    """
    with SparkTestSession.start(app="loom-etl-tests", parallelism=1) as session:
        yield session


@pytest.fixture
def loom_spark_runner(loom_spark_session: SparkSession) -> SparkStepRunner:
    """Fresh :class:`SparkStepRunner` per test — no Delta I/O, in-memory only.

    Depends on ``loom_spark_session`` — the SparkSession is scoped to the
    test session to amortise the ~5 s JVM startup cost.
    """
    return SparkStepRunner(loom_spark_session)


def _resolve_ivy_dir(ivy_dir: str | Path | None) -> Path | None:
    if ivy_dir is not None:
        return Path(ivy_dir)
    from_env = loom_variable(SPARK_IVY_DIR_VARIABLE)
    if from_env:
        return Path(from_env)
    return None


def _offline_jars_requested() -> bool:
    return loom_variable(SPARK_OFFLINE_VARIABLE) == "1"


def _resolve_local_delta_jars() -> tuple[Path, ...] | None:
    jar_dir = Path.home() / ".ivy2" / "jars"
    if not jar_dir.exists():
        return None

    delta_version = _delta_spark_version()
    delta_spark = _pick_jar(jar_dir, "io.delta_delta-spark_2.12-", delta_version)
    delta_storage = _pick_jar(jar_dir, "io.delta_delta-storage-", delta_version)
    antlr = _pick_jar(jar_dir, "org.antlr_antlr4-runtime-", None)
    if delta_spark is None or delta_storage is None or antlr is None:
        return None
    return (delta_spark, delta_storage, antlr)


def _delta_spark_version() -> str | None:
    try:
        return version("delta-spark")
    except PackageNotFoundError:
        return None


def _pick_jar(jar_dir: Path, prefix: str, expected_version: str | None) -> Path | None:
    if expected_version is not None:
        expected = jar_dir / f"{prefix}{expected_version}.jar"
        if expected.exists():
            return expected

    candidates = sorted(jar_dir.glob(f"{prefix}*.jar"))
    if not candidates:
        return None
    return candidates[-1]


def _spark_frame_to_polars(frame: Any) -> pl.DataFrame:
    import polars as pl

    columns = list(frame.columns)
    rows = frame.collect()
    records = [{col: row[col] for col in columns} for row in rows]
    if not records:
        return pl.DataFrame({col: [] for col in columns})
    return pl.DataFrame(records)
