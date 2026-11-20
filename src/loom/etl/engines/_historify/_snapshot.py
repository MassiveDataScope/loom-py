"""SNAPSHOT mode SCD Type 2 algorithm."""

from __future__ import annotations

from typing import Any, TypeVar

from loom.etl.declarative.target._history import HistorifySpec
from loom.etl.engines._historify._common import prev_period_value
from loom.etl.engines._historify._ops import HistorifyOps

F = TypeVar("F")


def apply_snapshot(
    ops: HistorifyOps[F],
    incoming: F,
    existing: F,
    spec: HistorifySpec,
    join_key: list[str],
    eff_date: Any,
) -> F:
    """Apply SNAPSHOT SCD2 classification.

    Partitions open existing rows into four disjoint sets:

    * Unchanged — exact match in incoming by full ``join_key`` → keep open.
    * Changed   — entity key in incoming, tracked values differ → always close.
    * Absent    — entity key not in incoming at all → apply delete policy.
    * New       — in incoming, not in open existing by ``join_key`` → insert open.

    The delete policy governs only the **absent** set.  Changed rows are always
    closed regardless of policy: the new snapshot provides explicit replacement
    state for the entity, so the old version is definitively superseded — it
    should not remain open alongside the new one regardless of IGNORE semantics.

    Closed historical rows are always preserved unchanged.
    """
    existing = ops.rewind_to(existing, spec, eff_date)

    entity_key = list(spec.keys)
    open_existing = ops.filter_null(existing, spec.valid_to)
    closed_existing = ops.filter_not_null(existing, spec.valid_to)

    dtype = ops.history_dtype(spec)
    prev = prev_period_value(eff_date, spec)

    new_raw = ops.anti_join(incoming, open_existing, join_key)
    new_rows = ops.stamp_col(
        ops.stamp_col(new_raw, spec.valid_from, eff_date, dtype),
        spec.valid_to,
        None,
        dtype,
    )

    unchanged = ops.semi_join(open_existing, incoming, join_key)
    if spec.overwrite:
        unchanged = ops.apply_overwrite_cols(unchanged, incoming, join_key, spec.overwrite)

    present_open = ops.semi_join(open_existing, incoming, entity_key)
    changed_old = ops.anti_join(present_open, incoming, join_key)
    changed_old_closed = ops.stamp_col(changed_old, spec.valid_to, prev, dtype)

    absent = ops.anti_join(open_existing, incoming, entity_key)
    closed_absent = ops.apply_delete_policy(absent, spec, eff_date)

    result = ops.union([closed_existing, unchanged, changed_old_closed, closed_absent, new_rows])
    return ops.ensure_soft_delete_col(result, spec)
