"""Common helpers shared by all Historify backends."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from _typeshed import SupportsRichComparison

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.target._history import (
    HistorifyInputMode,
    HistorifyRepairReport,
    HistorifySpec,
    HistoryDateType,
)

_DATE_DELTA = timedelta(days=1)
_TS_DELTA = timedelta(microseconds=1)


def resolve_effective_date(spec: HistorifySpec, params_instance: object) -> Any:
    """Resolve effective date to a scalar (SNAPSHOT) or column name (LOG)."""
    if spec.mode is HistorifyInputMode.LOG:
        return spec.effective_date
    if isinstance(spec.effective_date, ParamExpr):
        return eval_param_expr(spec.effective_date, params_instance)
    return spec.effective_date


def eval_param_expr(expr: ParamExpr, params_instance: object) -> Any:
    """Walk the ParamExpr attribute path and return the resolved value."""
    value: Any = params_instance
    for attr in expr.path:
        value = getattr(value, attr)
    return value


def resolve_track_cols(spec: HistorifySpec, frame_cols: list[str]) -> tuple[str, ...]:
    """Return tracked columns — explicit or all non-key, non-history columns."""
    if spec.track is not None:
        return spec.track
    excluded = set(spec.keys) | {spec.valid_from, spec.valid_to}
    if spec.delete_marker is not None:
        excluded.add(spec.delete_marker)
    return tuple(c for c in frame_cols if c not in excluded)


def log_run_cols(spec: HistorifySpec) -> list[str]:
    """Return the columns whose change starts a new LOG run: ``track`` plus the delete marker.

    Including the marker keeps a tombstone from collapsing into the run it closes.
    """
    marker = [spec.delete_marker] if spec.delete_marker is not None else []
    return [*(spec.track or ()), *marker]


def prev_period_value(eff_date: Any, spec: HistorifySpec) -> Any:
    """Return one unit before eff_date (one day or one microsecond)."""
    delta = _DATE_DELTA if spec.date_type is HistoryDateType.DATE else _TS_DELTA
    return eff_date - delta


def build_rewind_report(
    future_rows: list[tuple[object, ...]],
    eff_date: object,
) -> HistorifyRepairReport | None:
    """Build a repair report from collected ``keys + valid_from`` tuples.

    Args:
        future_rows: One tuple per existing row with ``valid_from`` strictly
            after ``eff_date``, shaped as ``(*entity key values, valid_from)``.
        eff_date: Effective date of the temporal rerun.

    Returns:
        The repair report, or ``None`` when no future rows exist.
    """
    if not future_rows:
        return None
    date_values = cast("set[SupportsRichComparison]", {row[-1] for row in future_rows})
    dates = tuple(sorted(date_values))
    warning = (
        f"Temporal rerun at {eff_date} rewound {len(future_rows)} future row(s) "
        f"with valid_from between {dates[0]} and {dates[-1]}; "
        "downstream loads for those dates require a rerun."
    )
    return HistorifyRepairReport(
        affected_keys=frozenset(row[:-1] for row in future_rows),
        dates_requiring_rerun=dates,
        warnings=(warning,),
    )
