"""Backend-agnostic SCD2 transform delegating frame operations to a HistorifyBackend."""

from __future__ import annotations

from typing import Any, TypeVar

from loom.etl.backends._historify._common import (
    build_rewind_report,
    resolve_effective_date,
    resolve_track_cols,
)
from loom.etl.backends._historify._log import apply_log, build_log_history
from loom.etl.backends._historify._ops import HistorifyBackend
from loom.etl.backends._historify._snapshot import apply_snapshot
from loom.etl.declarative.target._history import (
    HistorifyDeleteMarkerError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifyRepairReport,
    HistorifySpec,
    HistorifyTemporalConflictError,
)

F = TypeVar("F")


def scd2_transform(
    ops: HistorifyBackend[F],
    frame: F,
    existing: F | None,
    spec: HistorifySpec,
    params_instance: object,
) -> tuple[F, HistorifyRepairReport | None]:
    """Apply SCD Type 2 logic and return the transformed frame plus repair report.

    Args:
        ops:             Backend-specific frame operations.
        frame:           Incoming data frame.
        existing:        Current target frame, or ``None`` for first run.
        spec:            Compiled HistorifySpec.
        params_instance: Runtime params for ParamExpr resolution.

    Returns:
        Tuple of the transformed frame ready to be written to Delta and a
        repair report when a temporal rerun rewound strictly-future history
        (SNAPSHOT mode with ``allow_temporal_rerun``), otherwise ``None``.

    Raises:
        HistorifyDeleteMarkerError:     ``delete_marker`` column missing or not boolean.
        HistorifyKeyConflictError:      Duplicate entity state vectors.
        HistorifyDateCollisionError:    Same-date collisions in LOG mode.
        HistorifyTemporalConflictError: Future-open records, re-weave off.
    """
    track_cols = resolve_track_cols(spec, ops.columns(frame))
    join_key = list(spec.keys) + list(track_cols)
    eff_date = resolve_effective_date(spec, params_instance)

    if spec.delete_marker is not None:
        _assert_delete_marker(ops, frame, spec.delete_marker)
    is_log = spec.mode is HistorifyInputMode.LOG
    unique_key = [*spec.keys, str(spec.effective_date)] if is_log else join_key
    duplicates = ops.duplicate_keys(frame, unique_key)
    if duplicates is not None:
        raise HistorifyKeyConflictError(duplicates, mode=spec.mode)
    if is_log:
        ops.assert_no_date_collisions(frame, join_key, str(spec.effective_date), spec)

    if existing is None:
        return _first_run(ops, frame, spec, eff_date), None

    _temporal_guard(ops, existing, spec, eff_date)

    if spec.mode is HistorifyInputMode.SNAPSHOT:
        report = _rewind_report_if_enabled(ops, existing, spec, eff_date)
        return apply_snapshot(ops, frame, existing, spec, join_key, eff_date), report
    return apply_log(ops, frame, existing, spec), None


def _assert_delete_marker(ops: HistorifyBackend[F], frame: F, marker: str) -> None:
    """Raise unless the incoming frame carries ``marker`` as a boolean column."""
    if marker not in ops.columns(frame):
        raise HistorifyDeleteMarkerError(marker, "is missing from the incoming frame")
    dtype = ops.non_boolean_dtype(frame, marker)
    if dtype is not None:
        raise HistorifyDeleteMarkerError(
            marker, f"has dtype {dtype}, not boolean; cast it to Boolean (null = regular event)"
        )


def _first_run(
    ops: HistorifyBackend[F],
    frame: F,
    spec: HistorifySpec,
    eff_date: Any,
) -> F:
    """Build the initial history frame when the target table is empty."""
    if spec.mode is HistorifyInputMode.SNAPSHOT:
        dtype = ops.history_dtype(spec)
        result = ops.stamp_col(
            ops.stamp_col(frame, spec.valid_from, eff_date, dtype),
            spec.valid_to,
            None,
            dtype,
        )
        return ops.ensure_soft_delete_col(result, spec)
    return build_log_history(ops, frame, spec)


def _rewind_report_if_enabled(
    ops: HistorifyBackend[F],
    existing: F,
    spec: HistorifySpec,
    eff_date: Any,
) -> HistorifyRepairReport | None:
    """Report strictly-future rows rewound by a temporal rerun, if enabled."""
    if not spec.allow_temporal_rerun:
        return None
    return build_rewind_report(ops.collect_future_rows(existing, spec, eff_date), eff_date)


def _temporal_guard(
    ops: HistorifyBackend[F],
    existing: F,
    spec: HistorifySpec,
    eff_date: Any,
) -> None:
    """Raise if future-open records exist and re-weave is disabled."""
    if spec.allow_temporal_rerun or spec.mode is HistorifyInputMode.LOG:
        return
    min_conflict = ops.temporal_conflict_min_date(existing, spec, eff_date)
    if min_conflict is None:
        return
    raise HistorifyTemporalConflictError(min_conflict, eff_date)
