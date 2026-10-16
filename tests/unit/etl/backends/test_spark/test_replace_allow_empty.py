"""SparkTargetWriter honours IntoTable(...).replace(allow_empty=False)."""

from __future__ import annotations

import logging
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


def _persisted_rdds(spark: SparkSession) -> set[int]:
    """Ids of the RDDs whose blocks the Spark context currently keeps."""
    persistent = spark.sparkContext._jsc.getPersistentRDDs()
    return {int(rdd_id) for rdd_id in persistent.keySet().toArray()}


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


class _PinnedRdds:
    """Ids of the RDDs ``_pin_rows`` checkpointed, each checked to be kept when pinned."""

    def __init__(self, spark: SparkSession, writer: SparkTargetWriter) -> None:
        self._spark = spark
        self._pin_rows = writer._pin_rows
        self.ids: list[int] = []

    def __call__(self, frame: DataFrame) -> tuple[DataFrame, bool]:
        pinned, empty = self._pin_rows(frame)
        rdd_id = int(pinned._jdf.queryExecution().logical().rdd().id())
        assert rdd_id in _persisted_rdds(self._spark)
        self.ids.append(rdd_id)
        return pinned, empty

    def still_kept(self) -> set[int]:
        return set(self.ids) & _persisted_rdds(self._spark)


def _spy_pins(
    spark: SparkSession, writer: SparkTargetWriter, monkeypatch: pytest.MonkeyPatch
) -> _PinnedRdds:
    pins = _PinnedRdds(spark, writer)
    monkeypatch.setattr(writer, "_pin_rows", pins)
    return pins


def test_the_checked_rows_are_released_after_the_write(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    pins = _spy_pins(spark, spark_writer, monkeypatch)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    spark_writer.write(spark.createDataFrame([(3, "c")], _SCHEMA), spec, None)

    assert len(pins.ids) == 1
    assert pins.still_kept() == set()
    assert _rows(spark, path) == [(3, "c")]


def test_the_checked_rows_are_released_when_the_frame_is_empty(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    pins = _spy_pins(spark, spark_writer, monkeypatch)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()
    frame = spark.createDataFrame([], _SCHEMA)

    with pytest.raises(EmptyReplaceError):
        spark_writer.write(frame, spec, None)

    assert len(pins.ids) == 1
    assert pins.still_kept() == set()


def test_the_checked_rows_are_released_when_the_write_fails(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    pins = _spy_pins(spark, spark_writer, monkeypatch)

    def fail(frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        raise RuntimeError("write failed")

    monkeypatch.setattr(spark_writer, "_replace", fail)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()
    frame = spark.createDataFrame([(3, "c")], _SCHEMA)

    with pytest.raises(RuntimeError, match="write failed"):
        spark_writer.write(frame, spec, None)

    assert len(pins.ids) == 1
    assert pins.still_kept() == set()


def test_the_checked_rows_are_released_after_creating_a_missing_table(
    spark: SparkSession,
    spark_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = SparkTargetWriter(
        spark, str(spark_root), missing_table_policy=MissingTablePolicy.CREATE
    )
    pins = _spy_pins(spark, writer, monkeypatch)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    writer.write(spark.createDataFrame([(3, "c")], _SCHEMA), spec, None)

    assert len(pins.ids) == 1
    assert pins.still_kept() == set()
    assert _rows(spark, spark_table_path(spark_root, _REF)) == [(3, "c")]


_RELEASE_FAILED = "release failed"


class _Unreleasable:
    """A pinned frame whose blocks cannot be freed, as with a lost Spark context."""

    def unpersist(self) -> None:
        raise RuntimeError(_RELEASE_FAILED)


@pytest.fixture
def failing_release(spark_writer: SparkTargetWriter, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make freeing the pinned blocks raise inside ``_release_pinned``."""
    release = spark_writer._release_pinned
    monkeypatch.setattr(spark_writer, "_release_pinned", lambda frame: release(_Unreleasable()))


def _release_warned(caplog: pytest.LogCaptureFixture) -> bool:
    return any(
        record.levelno == logging.WARNING and _RELEASE_FAILED in str(record.exc_info)
        for record in caplog.records
    )


@pytest.mark.usefixtures("failing_release")
def test_a_failed_release_does_not_fail_a_successful_write(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()

    with caplog.at_level(logging.WARNING):
        spark_writer.write(spark.createDataFrame([(3, "c")], _SCHEMA), spec, None)

    assert _rows(spark, path) == [(3, "c")]
    assert _release_warned(caplog)


@pytest.mark.usefixtures("failing_release")
def test_a_failed_release_does_not_hide_the_write_error(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))

    def fail(frame: DataFrame, target: ResolvedTarget, *, schema_mode: SchemaMode) -> None:
        raise ValueError("write failed")

    monkeypatch.setattr(spark_writer, "_replace", fail)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()
    frame = spark.createDataFrame([(3, "c")], _SCHEMA)

    with caplog.at_level(logging.WARNING), pytest.raises(ValueError, match="write failed"):
        spark_writer.write(frame, spec, None)

    assert _release_warned(caplog)


@pytest.mark.usefixtures("failing_release")
def test_a_failed_release_does_not_hide_an_empty_frame(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()
    frame = spark.createDataFrame([], _SCHEMA)

    with caplog.at_level(logging.WARNING), pytest.raises(EmptyReplaceError):
        spark_writer.write(frame, spec, None)

    assert _rows(spark, path) == [(1, "a")]
    assert _release_warned(caplog)


@pytest.mark.usefixtures("failing_release")
def test_a_failed_release_does_not_hide_a_failed_row_check(
    spark: SparkSession,
    spark_writer: SparkTargetWriter,
    seed_spark_table: Callable[[str | TableRef, DataFrame], Path],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = seed_spark_table(_REF, spark.createDataFrame([(1, "a")], _SCHEMA))

    def fail(frame: DataFrame) -> bool:
        raise ValueError("row check failed")

    monkeypatch.setattr(DataFrame, "isEmpty", fail)
    spec = IntoTable(_REF).replace(allow_empty=False)._to_spec()
    frame = spark.createDataFrame([(3, "c")], _SCHEMA)

    with caplog.at_level(logging.WARNING), pytest.raises(ValueError, match="row check failed"):
        spark_writer.write(frame, spec, None)

    assert _rows(spark, path) == [(1, "a")]
    assert _release_warned(caplog)
