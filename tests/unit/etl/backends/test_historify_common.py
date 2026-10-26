"""Unit tests for common historify helpers."""

from __future__ import annotations

from datetime import date

from loom.etl.backends._historify._common import resolve_effective_date, resolve_track_cols
from loom.etl.declarative.expr._params import ParamExpr, params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import HistorifyInputMode, HistorifySpec


def _spec(
    *,
    mode: HistorifyInputMode = HistorifyInputMode.SNAPSHOT,
    effective_date: str | ParamExpr = "run_date",
    track: tuple[str, ...] | None = None,
    delete_marker: str | None = None,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("t"),
        keys=("id",),
        effective_date=effective_date,
        mode=mode,
        track=track,
        delete_marker=delete_marker,
    )


class TestResolveEffectiveDate:
    def test_snapshot_literal_string(self) -> None:
        spec = _spec(mode=HistorifyInputMode.SNAPSHOT, effective_date="snapshot_date")
        assert resolve_effective_date(spec, object()) == "snapshot_date"

    def test_snapshot_param_expr(self) -> None:
        class Params:
            run_date = date(2024, 1, 1)

        spec = _spec(mode=HistorifyInputMode.SNAPSHOT, effective_date=params.run_date)
        assert resolve_effective_date(spec, Params()) == date(2024, 1, 1)

    def test_log_returns_column_name(self) -> None:
        spec = _spec(mode=HistorifyInputMode.LOG, effective_date="event_date")
        assert resolve_effective_date(spec, object()) == "event_date"


class TestResolveTrackCols:
    def test_explicit_track(self) -> None:
        spec = _spec(track=("a", "b"))
        assert resolve_track_cols(spec, ["id", "a", "b", "c"]) == ("a", "b")

    def test_auto_excludes_keys_and_boundaries(self) -> None:
        spec = _spec(track=None)
        assert resolve_track_cols(spec, ["id", "valid_from", "valid_to", "a", "b"]) == ("a", "b")

    def test_auto_excludes_delete_marker(self) -> None:
        spec = _spec(mode=HistorifyInputMode.LOG, effective_date="event_date", delete_marker="gone")
        assert resolve_track_cols(spec, ["id", "a", "gone"]) == ("a",)
