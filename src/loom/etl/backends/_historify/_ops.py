"""HistorifyBackend Protocol — backend-specific primitives for SCD Type 2 algorithms."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

from loom.etl.declarative.target._history import HistorifySpec

F = TypeVar("F")


class HistorifyBackend(Protocol[F]):
    """Backend-specific frame operations used by SCD2Transform."""

    def columns(self, frame: F) -> list[str]: ...

    def non_boolean_dtype(self, frame: F, col: str) -> str | None:
        """Return the dtype name of ``col`` when it is not boolean, else ``None``."""
        ...

    def history_dtype(self, spec: HistorifySpec) -> Any:
        """Return the native dtype for valid_from / valid_to columns."""
        ...

    def filter_null(self, frame: F, col: str) -> F: ...

    def filter_not_null(self, frame: F, col: str) -> F: ...

    def filter_eq(self, frame: F, col: str, value: Any, dtype: Any) -> F: ...

    def filter_ne(self, frame: F, col: str, value: Any, dtype: Any) -> F: ...

    def anti_join(self, left: F, right: F, on: list[str]) -> F: ...

    def semi_join(self, left: F, right: F, on: list[str]) -> F: ...

    def union(self, frames: list[F]) -> F: ...

    def stamp_col(self, frame: F, name: str, value: Any, dtype: Any) -> F:
        """Add or overwrite column with literal value."""
        ...

    def null_col(self, frame: F, name: str, dtype: Any) -> F:
        """Add null column with given dtype."""
        ...

    def fill_null(self, frame: F, col: str, value: Any) -> F:
        """Replace nulls in ``col`` with a literal value."""
        ...

    def rename(self, frame: F, rename_map: dict[str, str]) -> F:
        """Rename columns according to mapping."""
        ...

    def drop(self, frame: F, cols: list[str]) -> F:
        """Drop columns."""
        ...

    def dedup_priority(self, frame: F, subset: list[str], priority_col: str) -> F:
        """Deduplicate keeping, per ``subset`` group, rows with the max ``priority_col``."""
        ...

    def apply_overwrite_cols(
        self, unchanged: F, incoming: F, join_key: list[str], overwrite: tuple[str, ...]
    ) -> F:
        """Replace overwrite columns in unchanged rows with values from incoming.

        Drops the overwrite columns from ``unchanged`` then joins the fresh values
        from ``incoming`` on ``join_key``.  The open row is refreshed in-place;
        no new history row is created.
        """
        ...

    def rewind_to(self, frame: F, spec: HistorifySpec, eff_date: Any) -> F:
        """Rewind history to its state before any run at or after eff_date."""
        ...

    def collect_future_rows(
        self, existing: F, spec: HistorifySpec, eff_date: Any
    ) -> list[tuple[object, ...]]:
        """Materialize ``keys + valid_from`` tuples of rows with valid_from > eff_date."""
        ...

    def build_log_boundaries(self, frame: F, spec: HistorifySpec) -> F:
        """Compute valid_from / valid_to from sorted event frame (LOG mode)."""
        ...

    def closure_events(self, history: F, spec: HistorifySpec) -> F:
        """Return one tombstone instant per stored vector closed without a successor.

        LOG history has no gaps except those left by tombstones, so every gap is
        read as a deletion: a vector has no successor when no later vector of its
        entity starts within one period after its ``valid_to``. Each row carries
        ``keys`` and the effective-date column at that next period (the
        tombstone's instant); the caller flags it with ``delete_marker``.
        """
        ...

    def apply_delete_policy(self, deleted: F, spec: HistorifySpec, eff_date: Any) -> F: ...

    def ensure_soft_delete_col(self, result: F, spec: HistorifySpec) -> F: ...

    def duplicate_keys(self, frame: F, keys: list[str]) -> str | None:
        """Describe up to five ``keys`` combinations repeated in ``frame``, or ``None``."""
        ...

    def assert_no_date_collisions(
        self, frame: F, keys: list[str], eff_col: str, spec: HistorifySpec
    ) -> None: ...

    def temporal_conflict_min_date(
        self, existing: F, spec: HistorifySpec, eff_date: Any
    ) -> Any | None:
        """Return min conflicting valid_from or None if no conflict."""
        ...
