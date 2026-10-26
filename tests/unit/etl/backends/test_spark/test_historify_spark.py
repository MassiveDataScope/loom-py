"""Spark SCD Type 2 historify — backend-specific tests.

Behavioral scenarios live in
:class:`~tests.unit.etl.backends._historify_contract.HistorifyContractTests`.
This module provides the Spark fixtures and backend-specific helper tests.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Generator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from loom.etl.backends.spark._historify import SparkHistorifyBackend  # noqa: E402
from loom.etl.backends.spark._writer import SparkTargetWriter  # noqa: E402
from loom.etl.declarative.expr._refs import TableRef  # noqa: E402
from loom.etl.declarative.target._history import (  # noqa: E402
    HistorifyDateCollisionError,
    HistorifyInputMode,
    HistorifySpec,
    HistoryDateType,
)
from loom.etl.storage._config import MissingTablePolicy  # noqa: E402
from tests.unit.etl.backends._historify_contract import (  # noqa: E402
    HistorifyContractTests,
    _log_spec,
    _snapshot_spec,
)

# ---------------------------------------------------------------------------
# Concrete contract implementation — Spark backend
# ---------------------------------------------------------------------------


class TestHistorifySparkTarget(HistorifyContractTests):
    """Run all shared behavioral scenarios against SparkTargetWriter."""

    @pytest.fixture
    def root(self, spark_root: Path) -> Path:
        return spark_root

    @pytest.fixture
    def writer(self, spark: SparkSession, spark_root: Path) -> SparkTargetWriter:
        return SparkTargetWriter(
            spark,
            str(spark_root),
            missing_table_policy=MissingTablePolicy.CREATE,
        )

    @pytest.fixture
    def make_frame(self, spark: SparkSession) -> Callable[[list[dict]], DataFrame]:
        return spark.createDataFrame

    @pytest.fixture
    def read_table(self, spark: SparkSession) -> Callable[[str], list[dict[str, Any]]]:
        return lambda uri: [row.asDict() for row in spark.read.format("delta").load(uri).collect()]

    @pytest.fixture(autouse=True)
    def utc_clock(
        self, spark: SparkSession, monkeypatch: pytest.MonkeyPatch
    ) -> Generator[None, None, None]:
        """Pin the Python and Spark session clocks to UTC.

        PySpark converts naive datetimes with the process local zone and casts
        timestamps to dates with the session zone; pinning both makes naive
        values round-trip as UTC wall time, as the contract expects.
        """
        previous = spark.conf.get("spark.sql.session.timeZone")
        monkeypatch.setenv("TZ", "UTC")
        time.tzset()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        yield
        spark.conf.set("spark.sql.session.timeZone", previous)
        monkeypatch.undo()
        time.tzset()


# ---------------------------------------------------------------------------
# Spark-specific helper tests (backend-specific operations)
# ---------------------------------------------------------------------------


def _stamp_new_rows(frame: DataFrame, spec: HistorifySpec, eff_date: Any) -> DataFrame:
    ops = SparkHistorifyBackend()
    dtype = ops.history_dtype(spec)
    frame = ops.stamp_col(frame, spec.valid_from, eff_date, dtype)
    return ops.stamp_col(frame, spec.valid_to, None, dtype)


class TestDuplicateKeys:
    def test_unique_frame_has_none(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]
        )
        assert SparkHistorifyBackend().duplicate_keys(frame, ["player_id", "team_id"]) is None

    def test_duplicates_are_described(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM"}, {"player_id": 1, "team_id": "RM"}]
        )
        described = SparkHistorifyBackend().duplicate_keys(frame, ["player_id", "team_id"])
        assert described is not None
        assert "'player_id': 1" in described


class TestAssertNoDateCollisions:
    def test_unique_events_pass(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "a", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "b", "event_date": date(2024, 6, 1)},
            ]
        )
        SparkHistorifyBackend().assert_no_date_collisions(
            frame, ["subscription_id", "plan"], "event_date", _log_spec()
        )

    def test_same_date_same_key_raises(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
            ]
        )
        backend = SparkHistorifyBackend()
        spec = _log_spec()
        with pytest.raises(HistorifyDateCollisionError):
            backend.assert_no_date_collisions(
                frame, ["subscription_id", "plan"], "event_date", spec
            )

    def test_skipped_for_timestamp(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
            ]
        )
        spec = HistorifySpec(
            table_ref=TableRef("dim_subs"),
            keys=("subscription_id",),
            effective_date="event_date",
            mode=HistorifyInputMode.LOG,
            track=("plan",),
            date_type=HistoryDateType.TIMESTAMP,
        )
        SparkHistorifyBackend().assert_no_date_collisions(
            frame, ["subscription_id", "plan"], "event_date", spec
        )


class TestBackendHelpers:
    def test_filter_eq_with_dtype(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame([{"d": date(2024, 1, 1)}, {"d": date(2024, 1, 2)}])
        result = SparkHistorifyBackend().filter_eq(frame, "d", "2024-01-01", "date")
        assert result.count() == 1
        assert result.collect()[0]["d"] == date(2024, 1, 1)

    def test_filter_ne_with_dtype(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame([{"d": date(2024, 1, 1)}, {"d": date(2024, 1, 2)}])
        result = SparkHistorifyBackend().filter_ne(frame, "d", "2024-01-01", "date")
        assert result.count() == 1
        assert result.collect()[0]["d"] == date(2024, 1, 2)

    def test_null_col(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame([{"a": 1}])
        result = SparkHistorifyBackend().null_col(frame, "n", "date")
        row = result.collect()[0]
        assert row["n"] is None
        assert "n" in result.columns

    def test_build_log_boundaries_timestamp(self, spark: SparkSession) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("dim_subs"),
            keys=("subscription_id",),
            effective_date="event_date",
            mode=HistorifyInputMode.LOG,
            track=("plan",),
            date_type=HistoryDateType.TIMESTAMP,
        )
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "a", "event_date": datetime(2024, 1, 1, 10, 0, 0)},
                {"subscription_id": 1, "plan": "b", "event_date": datetime(2024, 1, 1, 12, 0, 0)},
            ]
        )
        result = SparkHistorifyBackend().build_log_boundaries(frame, spec)
        rows = result.collect()
        assert rows[0]["valid_from"] == datetime(2024, 1, 1, 10, 0, 0)
        assert rows[0]["valid_to"] == datetime(2024, 1, 1, 11, 59, 59, 999999)

    def test_build_log_boundaries_clamps_same_instant(self, spark: SparkSession) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("vital"),
            keys=("id",),
            effective_date="event_ts",
            mode=HistorifyInputMode.LOG,
            track=("state",),
            date_type=HistoryDateType.TIMESTAMP,
        )
        # false and true stamped in the same snapshot instant -> zero-width, not negative.
        frame = spark.createDataFrame(
            [
                {"id": 1, "state": "false", "event_ts": datetime(2024, 1, 1, 8)},
                {"id": 1, "state": "true", "event_ts": datetime(2024, 1, 1, 8)},
                {"id": 1, "state": "done", "event_ts": datetime(2024, 6, 1, 8)},
            ]
        )
        rows = SparkHistorifyBackend().build_log_boundaries(frame, spec).collect()
        assert all(r["valid_to"] is None or r["valid_to"] >= r["valid_from"] for r in rows)
        collided = [r for r in rows if r["state"] == "false"]
        assert collided[0]["valid_to"] == collided[0]["valid_from"] == datetime(2024, 1, 1, 8)
        open_rows = [r for r in rows if r["valid_to"] is None]
        assert [r["state"] for r in open_rows] == ["done"]

    def test_build_log_boundaries_clamps_same_date(self, spark: SparkSession) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("vital"),
            keys=("id",),
            effective_date="event_date",
            mode=HistorifyInputMode.LOG,
            track=("state",),
            date_type=HistoryDateType.DATE,
        )
        # Two events on the same day -> zero-width [D, D], not D-1.
        frame = spark.createDataFrame(
            [
                {"id": 1, "state": "false", "event_date": date(2024, 1, 1)},
                {"id": 1, "state": "true", "event_date": date(2024, 1, 1)},
                {"id": 1, "state": "done", "event_date": date(2024, 6, 1)},
            ]
        )
        rows = SparkHistorifyBackend().build_log_boundaries(frame, spec).collect()
        assert all(r["valid_to"] is None or r["valid_to"] >= r["valid_from"] for r in rows)
        collided = [r for r in rows if r["state"] == "false"]
        assert collided[0]["valid_to"] == collided[0]["valid_from"] == date(2024, 1, 1)
        closed = [r for r in rows if r["state"] == "true"]
        assert closed[0]["valid_to"] == date(2024, 5, 31)
        open_rows = [r for r in rows if r["valid_to"] is None]
        assert [r["state"] for r in open_rows] == ["done"]


class TestLogTrackNoneNoCollapse:
    def test_track_none_emits_one_version_per_event(self, spark: SparkSession) -> None:
        spec = _log_spec(track=None)
        frame = spark.createDataFrame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 2, 1)},
            ]
        )
        result = SparkHistorifyBackend().build_log_boundaries(frame, spec)
        assert result.count() == 2


class TestStampNewRows:
    def test_adds_history_columns(self, spark: SparkSession) -> None:
        frame = spark.createDataFrame([{"player_id": 1, "team_id": "RM"}])
        spec = _snapshot_spec()
        result = _stamp_new_rows(frame, spec, date(2024, 1, 1))
        rows = result.collect()
        assert rows[0]["valid_from"] == date(2024, 1, 1)
        assert rows[0]["valid_to"] is None


_HISTORY_SCHEMA = T.StructType(
    [
        T.StructField("player_id", T.LongType()),
        T.StructField("team_id", T.StringType()),
        T.StructField("valid_from", T.DateType()),
        T.StructField("valid_to", T.DateType()),
    ]
)

_HISTORY_SCHEMA_SOFT = T.StructType(
    [*_HISTORY_SCHEMA.fields, T.StructField("deleted_at", T.DateType())]
)


class TestRewindTo:
    def test_discards_rows_from_same_eff_date(self, spark: SparkSession) -> None:
        existing = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM", "valid_from": date(2024, 6, 1), "valid_to": None}],
            schema=_HISTORY_SCHEMA,
        )
        result = SparkHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        assert result.count() == 0

    def test_discards_future_rows_on_backfill(self, spark: SparkSession) -> None:
        existing = spark.createDataFrame(
            [
                {"player_id": 1, "team_id": "RM", "valid_from": date(2024, 1, 1), "valid_to": None},
                {
                    "player_id": 1,
                    "team_id": "BCA",
                    "valid_from": date(2024, 6, 1),
                    "valid_to": None,
                },
            ],
            schema=_HISTORY_SCHEMA,
        )
        result = SparkHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 3, 1))
        rows = result.collect()
        assert [r["team_id"] for r in rows] == ["RM"]

    def test_reopens_row_closed_by_previous_run(self, spark: SparkSession) -> None:
        existing = spark.createDataFrame(
            [
                {
                    "player_id": 1,
                    "team_id": "RM",
                    "valid_from": date(2024, 1, 1),
                    "valid_to": date(2024, 5, 31),
                }
            ],
            schema=_HISTORY_SCHEMA,
        )
        result = SparkHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        rows = result.collect()
        assert rows[0]["valid_to"] is None

    def test_clears_deleted_at_only_on_reopened_rows(self, spark: SparkSession) -> None:
        existing = spark.createDataFrame(
            [
                {
                    "player_id": 1,
                    "team_id": "RM",
                    "valid_from": date(2024, 1, 1),
                    "valid_to": date(2024, 5, 31),
                    "deleted_at": date(2024, 6, 1),
                },
                {
                    "player_id": 2,
                    "team_id": "BCA",
                    "valid_from": date(2024, 1, 1),
                    "valid_to": date(2024, 3, 31),
                    "deleted_at": date(2024, 4, 1),
                },
            ],
            schema=_HISTORY_SCHEMA_SOFT,
        )
        result = SparkHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        rows = {r["player_id"]: r for r in result.collect()}
        assert rows[1]["valid_to"] is None
        assert rows[1]["deleted_at"] is None
        assert rows[2]["valid_to"] == date(2024, 3, 31)
        assert rows[2]["deleted_at"] == date(2024, 4, 1)


class TestCollectFutureRows:
    def test_empty_when_no_future_rows(self, spark: SparkSession) -> None:
        existing = spark.createDataFrame(
            [{"player_id": 1, "team_id": "RM", "valid_from": date(2024, 6, 1), "valid_to": None}],
            schema=_HISTORY_SCHEMA,
        )
        rows = SparkHistorifyBackend().collect_future_rows(
            existing, _snapshot_spec(), date(2024, 6, 1)
        )
        assert rows == []

    def test_returns_keys_and_valid_from_of_future_rows(self, spark: SparkSession) -> None:
        existing = spark.createDataFrame(
            [
                {"player_id": 1, "team_id": "RM", "valid_from": date(2024, 1, 1), "valid_to": None},
                {
                    "player_id": 1,
                    "team_id": "BCA",
                    "valid_from": date(2024, 6, 1),
                    "valid_to": None,
                },
                {
                    "player_id": 2,
                    "team_id": "LIV",
                    "valid_from": date(2024, 9, 1),
                    "valid_to": None,
                },
            ],
            schema=_HISTORY_SCHEMA,
        )
        rows = SparkHistorifyBackend().collect_future_rows(
            existing, _snapshot_spec(), date(2024, 3, 1)
        )
        assert sorted(rows) == [(1, date(2024, 6, 1)), (2, date(2024, 9, 1))]


_CLOSURE_HISTORY = [
    # Contiguous, open tail: no deletion.
    {"id": 1, "valid_from": date(2024, 1, 1), "valid_to": date(2024, 1, 31)},
    {"id": 1, "valid_from": date(2024, 2, 1), "valid_to": None},
    # Zero-width vector sharing valid_from with the open one: no deletion.
    {"id": 2, "valid_from": date(2024, 1, 1), "valid_to": date(2024, 1, 1)},
    {"id": 2, "valid_from": date(2024, 1, 1), "valid_to": None},
    # Gap before a reopen, then a closed tail: two deletions.
    {"id": 3, "valid_from": date(2024, 1, 1), "valid_to": date(2024, 1, 31)},
    {"id": 3, "valid_from": date(2024, 3, 1), "valid_to": date(2024, 3, 31)},
]
_CLOSURE_EVENTS = [(3, date(2024, 2, 1)), (3, date(2024, 4, 1))]
_CLOSURE_SPEC = HistorifySpec(
    table_ref=TableRef("t"),
    keys=("id",),
    effective_date="event_date",
    mode=HistorifyInputMode.LOG,
    track=("state",),
    delete_marker="is_deleted",
)


class TestClosureEvents:
    def test_returns_tombstone_after_each_closure_without_successor(
        self, spark: SparkSession
    ) -> None:
        schema = "id long, valid_from date, valid_to date"
        frame = spark.createDataFrame(
            [(r["id"], r["valid_from"], r["valid_to"]) for r in _CLOSURE_HISTORY], schema
        )
        result = SparkHistorifyBackend().closure_events(frame, _CLOSURE_SPEC)
        assert sorted(tuple(r) for r in result.collect()) == _CLOSURE_EVENTS
