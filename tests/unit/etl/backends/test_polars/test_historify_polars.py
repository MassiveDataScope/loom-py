"""Polars SCD Type 2 historify — backend-specific tests.

Behavioral scenarios live in
:class:`~tests.unit.etl.backends._historify_contract.HistorifyContractTests`.
This module provides the Polars fixtures and backend-specific helper tests.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl  # noqa: E402

from loom.etl.backends._historify._log import apply_log  # noqa: E402
from loom.etl.backends._historify._transform import scd2_transform  # noqa: E402
from loom.etl.backends.polars._historify import PolarsHistorifyBackend  # noqa: E402
from loom.etl.backends.polars._writer import PolarsTargetWriter  # noqa: E402
from loom.etl.declarative.expr._refs import TableRef  # noqa: E402
from loom.etl.declarative.target._history import (  # noqa: E402
    HistorifyDateCollisionError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifySpec,
    HistoryDateType,
)
from loom.etl.storage._config import MissingTablePolicy  # noqa: E402
from tests.unit.etl.backends._historify_contract import (  # noqa: E402
    HistorifyContractTests,
    _log_spec,
    _snapshot_spec,
)


@dataclass
class _Params:
    run_date: date


# ---------------------------------------------------------------------------
# Concrete contract implementation — Polars backend
# ---------------------------------------------------------------------------


class TestHistorifyPolars(HistorifyContractTests):
    """Run all shared behavioral scenarios against PolarsTargetWriter."""

    @pytest.fixture
    def root(self, tmp_path: Path) -> Path:
        return tmp_path

    @pytest.fixture
    def writer(self, tmp_path: Path) -> PolarsTargetWriter:
        return PolarsTargetWriter(str(tmp_path), missing_table_policy=MissingTablePolicy.CREATE)

    @pytest.fixture
    def make_frame(self) -> Callable[[list[dict]], pl.LazyFrame]:
        return lambda rows: pl.from_dicts(rows).lazy()

    @pytest.fixture
    def read_table(self) -> Callable[[str], list[dict[str, Any]]]:
        return lambda uri: pl.scan_delta(uri).collect().to_dicts()


# ---------------------------------------------------------------------------
# Polars-specific helper tests (backend-specific operations)
# ---------------------------------------------------------------------------


def _stamp_new_rows(frame: pl.DataFrame, spec: HistorifySpec, eff_date: Any) -> pl.DataFrame:
    ops = PolarsHistorifyBackend()
    dtype = ops.history_dtype(spec)
    frame = ops.stamp_col(frame, spec.valid_from, eff_date, dtype)
    return ops.stamp_col(frame, spec.valid_to, None, dtype)


class TestAssertUniqueEntityState:
    def test_unique_frame_passes(self) -> None:
        frame = pl.DataFrame({"player_id": [1, 2], "team_id": ["RM", "BCA"]})
        PolarsHistorifyBackend().assert_unique_keys(frame, ["player_id", "team_id"])

    def test_duplicate_raises(self) -> None:
        frame = pl.DataFrame({"player_id": [1, 1], "team_id": ["RM", "RM"]})
        backend = PolarsHistorifyBackend()
        with pytest.raises(HistorifyKeyConflictError):
            backend.assert_unique_keys(frame, ["player_id", "team_id"])


class TestAssertNoDateCollisions:
    def test_unique_events_pass(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["a", "b"],
                "event_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        PolarsHistorifyBackend().assert_no_date_collisions(
            frame, ["subscription_id", "plan"], "event_date", _log_spec()
        )

    def test_same_date_same_key_raises(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["pro", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 1, 1)],
            }
        )
        backend = PolarsHistorifyBackend()
        spec = _log_spec()
        with pytest.raises(HistorifyDateCollisionError):
            backend.assert_no_date_collisions(
                frame, ["subscription_id", "plan"], "event_date", spec
            )

    def test_skipped_for_timestamp(self) -> None:
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["pro", "pro"],
                "event_date": [date(2024, 1, 1), date(2024, 1, 1)],
            }
        )
        spec = HistorifySpec(
            table_ref=TableRef("dim_subs"),
            keys=("subscription_id",),
            effective_date="event_date",
            mode=HistorifyInputMode.LOG,
            track=("plan",),
            date_type=HistoryDateType.TIMESTAMP,
        )
        PolarsHistorifyBackend().assert_no_date_collisions(
            frame, ["subscription_id", "plan"], "event_date", spec
        )


class TestBackendHelpers:
    def test_filter_eq_with_dtype(self) -> None:
        frame = pl.DataFrame({"d": [date(2024, 1, 1), date(2024, 1, 2)]})
        result = PolarsHistorifyBackend().filter_eq(frame, "d", "2024-01-01", pl.Date)
        assert result["d"].to_list() == [date(2024, 1, 1)]

    def test_filter_ne_with_dtype(self) -> None:
        frame = pl.DataFrame({"d": [date(2024, 1, 1), date(2024, 1, 2)]})
        result = PolarsHistorifyBackend().filter_ne(frame, "d", "2024-01-01", pl.Date)
        assert result["d"].to_list() == [date(2024, 1, 2)]

    def test_null_col(self) -> None:
        frame = pl.DataFrame({"a": [1]})
        result = PolarsHistorifyBackend().null_col(frame, "n", pl.Date)
        assert result["n"].is_null().all()
        assert result["n"].dtype == pl.Date

    def test_union_aligns_tz_aware_with_naive(self) -> None:
        aware = pl.DataFrame({"t": [datetime(2024, 1, 1, 8)]}).with_columns(
            pl.col("t").dt.replace_time_zone("UTC")
        )
        naive = pl.DataFrame({"t": [datetime(2024, 1, 2, 8)]})
        result = PolarsHistorifyBackend().union([aware, naive])
        assert result["t"].dtype == pl.Datetime("us", "UTC")
        assert result["t"].to_list() == [
            datetime(2024, 1, 1, 8, tzinfo=UTC),
            datetime(2024, 1, 2, 8, tzinfo=UTC),
        ]


class TestTimestampBoundariesAreUtcAware:
    def test_log_timestamp_boundaries_are_utc_aware(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("dim"),
            keys=("id",),
            effective_date="eff",
            mode=HistorifyInputMode.LOG,
            track=("status",),
            date_type=HistoryDateType.TIMESTAMP,
        )
        frame = pl.DataFrame(
            {
                "id": [1, 1],
                "status": ["a", "b"],
                "eff": [datetime(2024, 1, 1, 8), datetime(2024, 6, 1, 8)],
            }
        ).with_columns(pl.col("eff").dt.replace_time_zone("UTC"))
        result, report = scd2_transform(PolarsHistorifyBackend(), frame, None, spec, None)
        assert report is None
        assert result["valid_from"].dtype == pl.Datetime("us", "UTC")
        assert result["valid_to"].dtype == pl.Datetime("us", "UTC")
        closed = result.filter(pl.col("status") == "a")
        assert closed["valid_to"].to_list() == [datetime(2024, 6, 1, 7, 59, 59, 999999, tzinfo=UTC)]


class TestBuildLogBoundariesClampsSameInstant:
    """Two events sharing an effective instant (e.g. a task created and flipped
    to true in the same snapshot millisecond) must collapse to a zero-width
    [T, T] vector, never an inverted valid_to < valid_from, and without raising."""

    _SPEC = HistorifySpec(
        table_ref=TableRef("vital"),
        keys=("id",),
        effective_date="event_ts",
        mode=HistorifyInputMode.LOG,
        track=("state",),
        date_type=HistoryDateType.TIMESTAMP,
    )

    @staticmethod
    def _events(rows: list[dict[str, Any]]) -> pl.DataFrame:
        return pl.from_dicts(rows).with_columns(pl.col("event_ts").dt.replace_time_zone("UTC"))

    def test_same_instant_yields_zero_width_not_negative(self) -> None:
        events = self._events(
            [
                {"id": 1, "state": "false", "event_ts": datetime(2024, 1, 1, 8)},
                {"id": 1, "state": "true", "event_ts": datetime(2024, 1, 1, 8)},
                {"id": 1, "state": "done", "event_ts": datetime(2024, 6, 1, 8)},
            ]
        )
        result = (
            PolarsHistorifyBackend().build_log_boundaries(events, self._SPEC).sort("valid_from")
        )
        assert len(result.filter(pl.col("valid_to") < pl.col("valid_from"))) == 0
        collided = result.filter(
            (pl.col("valid_from") == datetime(2024, 1, 1, 8, tzinfo=UTC))
            & (pl.col("state") == "false")
        )
        assert collided["valid_to"].to_list() == [datetime(2024, 1, 1, 8, tzinfo=UTC)]

    def test_open_row_stays_null(self) -> None:
        events = self._events(
            [
                {"id": 1, "state": "false", "event_ts": datetime(2024, 1, 1, 8)},
                {"id": 1, "state": "true", "event_ts": datetime(2024, 1, 1, 8)},
            ]
        )
        result = PolarsHistorifyBackend().build_log_boundaries(events, self._SPEC)
        open_rows = result.filter(pl.col("valid_to").is_null())
        assert open_rows["state"].to_list() == ["true"]

    def test_distinct_instants_unchanged(self) -> None:
        events = self._events(
            [
                {"id": 1, "state": "a", "event_ts": datetime(2024, 1, 1, 8)},
                {"id": 1, "state": "b", "event_ts": datetime(2024, 6, 1, 8)},
            ]
        )
        result = (
            PolarsHistorifyBackend().build_log_boundaries(events, self._SPEC).sort("valid_from")
        )
        closed = result.filter(pl.col("state") == "a")
        assert closed["valid_to"].to_list() == [datetime(2024, 6, 1, 7, 59, 59, 999999, tzinfo=UTC)]

    def test_same_date_yields_zero_width_date_vector(self) -> None:
        spec = HistorifySpec(
            table_ref=TableRef("vital"),
            keys=("id",),
            effective_date="event_date",
            mode=HistorifyInputMode.LOG,
            track=("state",),
            date_type=HistoryDateType.DATE,
        )
        # Two events on the same day -> zero-width [D, D], not D-1.
        events = pl.DataFrame(
            {
                "id": [1, 1, 1],
                "state": ["false", "true", "done"],
                "event_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        result = PolarsHistorifyBackend().build_log_boundaries(events, spec)
        assert len(result.filter(pl.col("valid_to") < pl.col("valid_from"))) == 0
        collided = result.filter(pl.col("state") == "false")
        assert collided["valid_from"].to_list() == [date(2024, 1, 1)]
        assert collided["valid_to"].to_list() == [date(2024, 1, 1)]
        closed = result.filter(pl.col("state") == "true")
        assert closed["valid_to"].to_list() == [date(2024, 5, 31)]
        assert result.filter(pl.col("valid_to").is_null())["state"].to_list() == ["done"]

    def test_same_instant_via_apply_log_reweave(self) -> None:
        # Existing history already holds two vectors at the same valid_from (e.g. a
        # false/true flip stamped in the same snapshot ms). Re-weaving through
        # apply_log feeds both same-instant rows into build_log_boundaries; the clamp
        # must keep the earlier one at zero width rather than inverting it.
        existing = pl.DataFrame(
            {
                "id": [1, 1],
                "state": ["false", "true"],
                "valid_from": [datetime(2024, 1, 1, 8), datetime(2024, 1, 1, 8)],
                "valid_to": [None, None],
            }
        ).with_columns(
            pl.col("valid_from").dt.replace_time_zone("UTC"),
            pl.col("valid_to").cast(pl.Datetime("us", "UTC")),
        )
        incoming = self._events([{"id": 1, "state": "done", "event_ts": datetime(2024, 6, 1, 8)}])
        result = apply_log(PolarsHistorifyBackend(), incoming, existing, self._SPEC)
        assert len(result.filter(pl.col("valid_to") < pl.col("valid_from"))) == 0


class TestLogTrackNoneNoCollapse:
    def test_track_none_emits_one_version_per_event(self) -> None:
        spec = _log_spec(track=None)
        frame = pl.DataFrame(
            {
                "subscription_id": [1, 1],
                "plan": ["basic", "basic"],
                "event_date": [date(2024, 1, 1), date(2024, 2, 1)],
            }
        )
        result = PolarsHistorifyBackend().build_log_boundaries(frame, spec)
        assert len(result) == 2


class TestCollapseRunsLazy:
    def test_lazy_frame_collapses_without_performance_warning(self) -> None:
        spec = _log_spec(overwrite=("price",))
        events = pl.DataFrame(
            {
                "subscription_id": [1, 1, 1, 2],
                "plan": ["basic", "basic", "pro", "basic"],
                "price": [10, 12, 30, 10],
                "channel": ["web", "app", "app", "web"],
                "event_date": [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 3, 1),
                    date(2024, 1, 1),
                ],
            }
        )
        entity_key = list(spec.keys)
        with warnings.catch_warnings():
            warnings.simplefilter("error", pl.exceptions.PerformanceWarning)
            lazy = PolarsHistorifyBackend._collapse_same_track_runs(
                events.lazy(), spec, entity_key, "event_date"
            )
            result = lazy.collect()
        eager = PolarsHistorifyBackend._collapse_same_track_runs(
            events, spec, entity_key, "event_date"
        )
        assert result.to_dicts() == eager.to_dicts()
        assert result.sort(["subscription_id", "event_date"]).to_dicts() == [
            {
                "subscription_id": 1,
                "event_date": date(2024, 1, 1),
                "price": 12,
                "plan": "basic",
                "channel": "web",
            },
            {
                "subscription_id": 1,
                "event_date": date(2024, 3, 1),
                "price": 30,
                "plan": "pro",
                "channel": "app",
            },
            {
                "subscription_id": 2,
                "event_date": date(2024, 1, 1),
                "price": 10,
                "plan": "basic",
                "channel": "web",
            },
        ]


class TestStampNewRows:
    def test_adds_history_columns(self) -> None:
        frame = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        result = _stamp_new_rows(frame, _snapshot_spec(), date(2024, 1, 1))
        assert result["valid_from"].to_list() == [date(2024, 1, 1)]
        assert result["valid_to"].is_null().all()


def _history_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    date_cols = [c for c in ("valid_from", "valid_to", "deleted_at") if c in rows[0]]
    return pl.from_dicts(rows).with_columns([pl.col(c).cast(pl.Date) for c in date_cols])


class TestRewindTo:
    def test_discards_rows_from_same_eff_date(self) -> None:
        existing = _history_frame(
            [{"player_id": 1, "team_id": "RM", "valid_from": date(2024, 6, 1), "valid_to": None}]
        )
        result = PolarsHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        assert len(result) == 0

    def test_discards_future_rows_on_backfill(self) -> None:
        existing = _history_frame(
            [
                {"player_id": 1, "team_id": "RM", "valid_from": date(2024, 1, 1), "valid_to": None},
                {
                    "player_id": 1,
                    "team_id": "BCA",
                    "valid_from": date(2024, 6, 1),
                    "valid_to": None,
                },
            ]
        )
        result = PolarsHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 3, 1))
        assert result["team_id"].to_list() == ["RM"]

    def test_reopens_row_closed_by_previous_run(self) -> None:
        existing = _history_frame(
            [
                {
                    "player_id": 1,
                    "team_id": "RM",
                    "valid_from": date(2024, 1, 1),
                    "valid_to": date(2024, 5, 31),
                }
            ]
        )
        result = PolarsHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        assert result["valid_to"].is_null().all()

    def test_preserves_rows_closed_before_run(self) -> None:
        existing = _history_frame(
            [
                {
                    "player_id": 1,
                    "team_id": "OLD",
                    "valid_from": date(2024, 1, 1),
                    "valid_to": date(2024, 3, 31),
                },
                {"player_id": 1, "team_id": "RM", "valid_from": date(2024, 4, 1), "valid_to": None},
            ]
        )
        result = PolarsHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        old = result.filter(pl.col("team_id") == "OLD")
        assert old["valid_to"].to_list() == [date(2024, 3, 31)]

    def test_clears_deleted_at_only_on_reopened_rows(self) -> None:
        existing = _history_frame(
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
            ]
        )
        result = PolarsHistorifyBackend().rewind_to(existing, _snapshot_spec(), date(2024, 6, 1))
        reopened = result.filter(pl.col("player_id") == 1)
        assert reopened["valid_to"].is_null().all()
        assert reopened["deleted_at"].is_null().all()
        untouched = result.filter(pl.col("player_id") == 2)
        assert untouched["valid_to"].to_list() == [date(2024, 3, 31)]
        assert untouched["deleted_at"].to_list() == [date(2024, 4, 1)]


class TestCollectFutureRows:
    def test_empty_when_no_future_rows(self) -> None:
        existing = _history_frame(
            [{"player_id": 1, "team_id": "RM", "valid_from": date(2024, 6, 1), "valid_to": None}]
        )
        rows = PolarsHistorifyBackend().collect_future_rows(
            existing, _snapshot_spec(), date(2024, 6, 1)
        )
        assert rows == []

    def test_returns_keys_and_valid_from_of_future_rows(self) -> None:
        existing = _history_frame(
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
            ]
        )
        rows = PolarsHistorifyBackend().collect_future_rows(
            existing, _snapshot_spec(), date(2024, 3, 1)
        )
        assert sorted(rows) == [(1, date(2024, 6, 1)), (2, date(2024, 9, 1))]


class TestTransformRepairReport:
    def test_backfill_returns_report(self) -> None:
        spec = _snapshot_spec(allow_temporal_rerun=True)
        existing = _history_frame(
            [
                {"player_id": 1, "team_id": "RM", "valid_from": date(2024, 1, 1), "valid_to": None},
                {
                    "player_id": 1,
                    "team_id": "BCA",
                    "valid_from": date(2024, 6, 1),
                    "valid_to": None,
                },
            ]
        )
        incoming = pl.DataFrame({"player_id": [1], "team_id": ["LIV"]})
        _, report = scd2_transform(
            PolarsHistorifyBackend(), incoming, existing, spec, _Params(run_date=date(2024, 3, 1))
        )
        assert report is not None
        assert report.affected_keys == frozenset({(1,)})
        assert report.dates_requiring_rerun == (date(2024, 6, 1),)
        assert len(report.warnings) == 1
        assert "1 future row(s)" in report.warnings[0]

    def test_same_day_rerun_returns_no_report(self) -> None:
        spec = _snapshot_spec(allow_temporal_rerun=True)
        existing = _history_frame(
            [{"player_id": 1, "team_id": "RM", "valid_from": date(2024, 6, 1), "valid_to": None}]
        )
        incoming = pl.DataFrame({"player_id": [1], "team_id": ["RM"]})
        _, report = scd2_transform(
            PolarsHistorifyBackend(), incoming, existing, spec, _Params(run_date=date(2024, 6, 1))
        )
        assert report is None
