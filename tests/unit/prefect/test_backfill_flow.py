"""Tests for the chunked backfill Prefect flow factory."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from prefect import Flow

from loom.etl import ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.prefect import BackfillChunk, backfill_flow
from loom.prefect.flow import _backfill_body
from tests.helpers.boundary import left_loom
from tests.unit.prefect.doubles import AcmeSampleParams, AcmeWindowStagingProcess

_CHUNK_PROCESS = "AcmeWindowStagingProcess"
_FINALIZE_PROCESS = "AcmeModelRefreshProcess"


class AcmeRefreshStep(ETLStep[AcmeSampleParams]):
    src = FromTable("staging.events")
    target = IntoTable("model.events").replace()

    def execute(self, params: AcmeSampleParams, *, src: Any = None, **frames: Any) -> Any:
        return src


class AcmeModelRefreshProcess(ETLProcess[AcmeSampleParams]):
    steps = [AcmeRefreshStep]


class AcmeBackfillPipeline(ETLPipeline[AcmeSampleParams]):
    processes = [AcmeWindowStagingProcess, AcmeModelRefreshProcess]


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "backfill.yaml"
    config_path.write_text("params: {}\n", encoding="utf-8")
    return config_path


def _build_flow(
    tmp_path: Path,
    chunk: BackfillChunk = "day",
    per_chunk_processes: list[str] | None = None,
    finalize_processes: list[str] | None = None,
) -> Flow[..., None]:
    flow: Flow[..., None] = backfill_flow(
        name="backfill",
        pipeline=AcmeBackfillPipeline,
        params_type=AcmeSampleParams,
        config_path=str(_write_config(tmp_path)),
        source_file=__file__,
        per_chunk_processes=per_chunk_processes or [_CHUNK_PROCESS],
        finalize_processes=finalize_processes or [_FINALIZE_PROCESS],
        window_start_field="updated_at_from",
        window_end_field="updated_at_to",
        chunk=chunk,
    )
    return flow


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    runner = MagicMock()
    monkeypatch.setattr(_backfill_body.ETLRunner, "from_yaml", lambda *a, **k: runner)
    monkeypatch.setattr(_backfill_body, "prefect_flow_run_id", lambda: None)
    monkeypatch.setattr(_backfill_body, "install_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill_body, "uninstall_log_bridge", lambda *a, **k: None)
    monkeypatch.setattr(_backfill_body, "maybe_delete_manifest", lambda *a, **k: None)
    return runner


def _windows(runner: MagicMock) -> list[tuple[datetime, datetime]]:
    """Per-chunk windows from every runner.run call that ran per_chunk_processes."""
    out = []
    for call in runner.run.call_args_list:
        if call.kwargs["include"] == [_CHUNK_PROCESS]:
            params = call.args[1]
            out.append((params.updated_at_from, params.updated_at_to))
    return out


class TestBackfillFlow:
    def test_n_day_window_yields_n_daily_runs(self, tmp_path: Path, runner: MagicMock) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 4, tzinfo=UTC),
        )
        assert _windows(runner) == [
            (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)),
            (datetime(2024, 1, 2, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC)),
            (datetime(2024, 1, 3, tzinfo=UTC), datetime(2024, 1, 4, tzinfo=UTC)),
        ]

    def test_days_run_oldest_first(self, tmp_path: Path, runner: MagicMock) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 3, 10, tzinfo=UTC),
            updated_at_to=datetime(2024, 3, 13, tzinfo=UTC),
        )
        starts = [w[0] for w in _windows(runner)]
        assert starts == sorted(starts)

    def test_partial_day_window_covers_whole_days(self, tmp_path: Path, runner: MagicMock) -> None:
        # from mid-day D0 to mid-day D2 -> whole days D0, D1, D2.
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, 13, 30, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, 5, 0, tzinfo=UTC),
        )
        assert _windows(runner) == [
            (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)),
            (datetime(2024, 1, 2, tzinfo=UTC), datetime(2024, 1, 3, tzinfo=UTC)),
            (datetime(2024, 1, 3, tzinfo=UTC), datetime(2024, 1, 4, tzinfo=UTC)),
        ]

    def test_finalize_runs_once_with_start_of_current_chunk(
        self, tmp_path: Path, runner: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            _backfill_body, "_now_utc", lambda: datetime(2024, 7, 24, 15, 30, tzinfo=UTC)
        )
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
        )
        finalize_calls = [
            c for c in runner.run.call_args_list if c.kwargs["include"] == [_FINALIZE_PROCESS]
        ]
        assert len(finalize_calls) == 1
        finalize_params = finalize_calls[0].args[1]
        # start of the current day chunk, not now.
        assert finalize_params.updated_at_to == datetime(2024, 7, 24, tzinfo=UTC)

    def test_start_from_skips_earlier_days(self, tmp_path: Path, runner: MagicMock) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
            start_from=datetime(2024, 1, 3, 9, 0, tzinfo=UTC),
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2024, 1, 3, tzinfo=UTC),
            datetime(2024, 1, 4, tzinfo=UTC),
        ]

    def test_start_from_before_window_start_skips_nothing(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 3, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
            start_from=datetime(2023, 12, 25, tzinfo=UTC),
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2024, 1, 3, tzinfo=UTC),
            datetime(2024, 1, 4, tzinfo=UTC),
        ]

    def test_each_chunk_runs_full_per_chunk_process_set(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        # No cross-chunk step skipping: every chunk passes the complete include set.
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
        )
        day_calls = [
            c for c in runner.run.call_args_list if c.kwargs["include"] == [_CHUNK_PROCESS]
        ]
        assert len(day_calls) == 2
        assert all(c.kwargs["include"] == [_CHUNK_PROCESS] for c in day_calls)

    def test_daily_runs_use_day_scoped_correlation_ids(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
        )
        corr_ids = [
            c.kwargs["correlation_id"]
            for c in runner.run.call_args_list
            if c.kwargs["include"] == [_CHUNK_PROCESS]
        ]
        assert corr_ids == ["backfill-20240101", "backfill-20240102"]
        assert len(set(corr_ids)) == len(corr_ids)

    def test_empty_window_still_finalizes_once(self, tmp_path: Path, runner: MagicMock) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert _windows(runner) == []
        finalize_calls = [
            c for c in runner.run.call_args_list if c.kwargs["include"] == [_FINALIZE_PROCESS]
        ]
        assert len(finalize_calls) == 1

    def test_hour_chunk_slices_by_hour(self, tmp_path: Path, runner: MagicMock) -> None:
        flow = _build_flow(tmp_path, chunk="hour")
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, 10, 20, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 1, 13, 0, tzinfo=UTC),
        )
        assert _windows(runner) == [
            (datetime(2024, 1, 1, 10, tzinfo=UTC), datetime(2024, 1, 1, 11, tzinfo=UTC)),
            (datetime(2024, 1, 1, 11, tzinfo=UTC), datetime(2024, 1, 1, 12, tzinfo=UTC)),
            (datetime(2024, 1, 1, 12, tzinfo=UTC), datetime(2024, 1, 1, 13, tzinfo=UTC)),
        ]

    def test_month_chunk_slices_by_month_across_year_boundary(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path, chunk="month")
        flow.fn(
            updated_at_from=datetime(2023, 11, 15, tzinfo=UTC),
            updated_at_to=datetime(2024, 2, 1, tzinfo=UTC),
        )
        assert _windows(runner) == [
            (datetime(2023, 11, 1, tzinfo=UTC), datetime(2023, 12, 1, tzinfo=UTC)),
            (datetime(2023, 12, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
            (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)),
        ]

    def test_year_chunk_slices_by_calendar_year(self, tmp_path: Path, runner: MagicMock) -> None:
        # partial edges: mid-2022 to mid-2025 -> whole years 2022..2025.
        flow = _build_flow(tmp_path, chunk="year")
        flow.fn(
            updated_at_from=datetime(2022, 6, 15, 10, 30, tzinfo=UTC),
            updated_at_to=datetime(2025, 3, 1, tzinfo=UTC),
        )
        assert _windows(runner) == [
            (datetime(2022, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, tzinfo=UTC)),
            (datetime(2023, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
            (datetime(2024, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
            (datetime(2025, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
        ]

    def test_year_chunk_exact_boundary_excludes_end_year(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        # exclusive end exactly on Jan 1 -> that year is NOT backfilled.
        flow = _build_flow(tmp_path, chunk="year")
        flow.fn(
            updated_at_from=datetime(2023, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2025, 1, 1, tzinfo=UTC),
        )
        assert _windows(runner) == [
            (datetime(2023, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
            (datetime(2024, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
        ]

    def test_year_runs_use_year_scoped_correlation_ids(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path, chunk="year")
        flow.fn(
            updated_at_from=datetime(2023, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2025, 1, 1, tzinfo=UTC),
        )
        corr_ids = [
            c.kwargs["correlation_id"]
            for c in runner.run.call_args_list
            if c.kwargs["include"] == [_CHUNK_PROCESS]
        ]
        assert corr_ids == ["backfill-2023", "backfill-2024"]
        assert len(set(corr_ids)) == len(corr_ids)

    def test_year_start_from_skips_earlier_years(self, tmp_path: Path, runner: MagicMock) -> None:
        flow = _build_flow(tmp_path, chunk="year")
        flow.fn(
            updated_at_from=datetime(2015, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2019, 1, 1, tzinfo=UTC),
            start_from=datetime(2017, 8, 3, 9, 0, tzinfo=UTC),
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2017, 1, 1, tzinfo=UTC),
            datetime(2018, 1, 1, tzinfo=UTC),
        ]

    def test_factory_rejects_unknown_per_chunk_process(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="per_chunk_processes"):
            _build_flow(tmp_path, per_chunk_processes=["NoSuchProcess"])

    def test_factory_rejects_unknown_finalize_process(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="finalize_processes"):
            _build_flow(tmp_path, finalize_processes=["NoSuchStep"])

    def test_factory_accepts_step_names(self, tmp_path: Path) -> None:
        # include= accepts step names too, so the factory validation must as well.
        flow = _build_flow(tmp_path, per_chunk_processes=["AcmeWindowStep"])
        assert flow.name == "backfill"

    def test_start_from_accepts_a_naive_iso_string(self, tmp_path: Path, runner: MagicMock) -> None:
        # Prefect submits run parameters unvalidated, so start_from arrives as the
        # operator typed it.
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
            start_from="2024-01-03T09:00:00",
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2024, 1, 3, tzinfo=UTC),
            datetime(2024, 1, 4, tzinfo=UTC),
        ]

    def test_start_from_resumes_at_the_chunk_of_the_instant_not_of_the_local_date(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        # 00:30 at +02:00 is 22:30 UTC of the previous day: the resume chunk is the
        # 2nd, not the 3rd.
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 5, tzinfo=UTC),
            start_from="2024-01-03T00:30:00+02:00",
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2024, 1, 2, tzinfo=UTC),
            datetime(2024, 1, 3, tzinfo=UTC),
            datetime(2024, 1, 4, tzinfo=UTC),
        ]

    def test_start_from_resolves_a_placeholder_in_the_past(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        # Clock-independent: a placeholder far enough in the past skips no chunk,
        # and an unresolved one would not be a datetime at all.
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
            start_from="${now-100000d}",
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        ]

    @pytest.mark.parametrize(
        "start_from",
        [
            pytest.param("${now+100000d}", id="start_from_resolves_a_placeholder_in_the_future"),
            pytest.param("${today+100000d}", id="start_from_resolves_a_today_placeholder"),
        ],
    )
    def test_start_from_resolves_a_placeholder(
        self, start_from: str, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path)
        flow.fn(
            updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
            start_from=start_from,
        )
        assert _windows(runner) == []

    @pytest.mark.parametrize(
        ("match", "start_from"),
        [
            pytest.param(
                "start_from", "yesterday", id="start_from_rejects_a_value_that_is_not_a_datetime"
            ),
            pytest.param(
                "start_from: invalid placeholder",
                "${nope}",
                id="start_from_rejects_an_invalid_placeholder_naming_the_parameter",
            ),
        ],
    )
    def test_start_from_rejects_an_invalid_value_naming_the_parameter(
        self, match: str, start_from: str, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path)
        with left_loom(ValueError, match=match):
            flow.fn(
                updated_at_from=datetime(2024, 1, 1, tzinfo=UTC),
                updated_at_to=datetime(2024, 1, 3, tzinfo=UTC),
                start_from=start_from,
            )

    def test_start_from_as_a_date_resumes_at_the_first_hour_of_that_day(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        flow = _build_flow(tmp_path, chunk="hour")
        flow.fn(
            updated_at_from=datetime(2024, 1, 3, tzinfo=UTC),
            updated_at_to=datetime(2024, 1, 3, 3, tzinfo=UTC),
            start_from=date(2024, 1, 3),
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2024, 1, 3, 0, tzinfo=UTC),
            datetime(2024, 1, 3, 1, tzinfo=UTC),
            datetime(2024, 1, 3, 2, tzinfo=UTC),
        ]

    def test_start_from_floors_a_year_chunk_after_converting_to_utc(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        # 2024-01-01T00:30+02:00 is 2023-12-31T22:30 UTC: the resume year is 2023.
        flow = _build_flow(tmp_path, chunk="year")
        flow.fn(
            updated_at_from=datetime(2023, 1, 1, tzinfo=UTC),
            updated_at_to=datetime(2025, 1, 1, tzinfo=UTC),
            start_from="2024-01-01T00:30:00+02:00",
        )
        assert [w[0] for w in _windows(runner)] == [
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC),
        ]

    def test_a_today_placeholder_reaches_the_window_fields(
        self, tmp_path: Path, runner: MagicMock
    ) -> None:
        # Clock-independent: yesterday to today is exactly one day chunk.
        flow = _build_flow(tmp_path)
        flow.fn(updated_at_from="${today-1d}", updated_at_to="${today}")
        windows = _windows(runner)
        assert len(windows) == 1
        start, end = windows[0]
        assert start.tzinfo is UTC
        assert end - start == timedelta(days=1)
