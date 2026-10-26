"""Historify runtime/domain error types."""

from __future__ import annotations

from loom.etl.declarative.target._history._enums import HistorifyInputMode


class HistorifyKeyConflictError(ValueError):
    """Raised when the incoming frame repeats an entity state vector or event.

    In SNAPSHOT mode each ``keys + track`` combination must be unique: duplicates
    indicate a data-modelling error, and the distinguishing dimension must be
    included in ``keys`` or ``track``. If the same entity legitimately belongs to
    multiple simultaneous states (e.g. a player on loan to two clubs at once),
    include the distinguishing column in ``track`` — both vectors will then be
    tracked independently.

    In LOG mode each ``keys + effective_date`` combination must be unique,
    tombstones flagged by ``delete_marker`` included.

    Args:
        duplicates: Human-readable representation of the conflicting key tuples.
        mode: Input mode whose uniqueness rule was violated.
    """

    def __init__(
        self, duplicates: str, *, mode: HistorifyInputMode = HistorifyInputMode.SNAPSHOT
    ) -> None:
        if mode is HistorifyInputMode.LOG:
            super().__init__(
                f"IntoHistory (LOG mode): duplicate events found — each "
                f"(keys + effective_date) combination must be unique, including "
                f"delete_marker tombstones. Conflicting rows: {duplicates}."
            )
            return
        super().__init__(
            f"IntoHistory: duplicate entity state vectors found in snapshot — "
            f"each (keys + track) combination must be unique. Conflicting rows: {duplicates}. "
            f"If the same entity legitimately occupies multiple simultaneous states "
            f"(e.g. a player on loan), include the distinguishing column in 'track'."
        )


class HistorifyDateCollisionError(ValueError):
    """Raised in LOG mode when two events share the same ``(keys + track, effective_date)``.

    At DATE granularity, two events on the same calendar day for the same entity
    state vector are ambiguous — relative ordering is lost and idempotency
    cannot be guaranteed.

    To resolve: switch to ``date_type=\"timestamp\"`` for sub-daily precision, or
    deduplicate events upstream before loading.

    Args:
        collisions: Human-readable description of the colliding key/date pairs.
    """

    def __init__(self, collisions: str) -> None:
        super().__init__(
            f"IntoHistory (LOG mode): same-date collisions detected — two events share "
            f"the same (keys + track, effective_date) value at DATE granularity. "
            f"Use date_type='timestamp' for sub-daily precision, or deduplicate upstream. "
            f"Collisions: {collisions}"
        )


class HistorifyDeleteMarkerError(ValueError):
    """Raised in LOG mode when the ``delete_marker`` column is missing or not boolean.

    Args:
        column: Declared ``delete_marker`` column name.
        problem: What is wrong with the column in the incoming frame.
    """

    def __init__(self, column: str, problem: str) -> None:
        super().__init__(
            f"IntoHistory (LOG mode): delete_marker column {column!r} {problem}. "
            f"The incoming frame must carry it as a boolean column (True = tombstone)."
        )


class HistorifyTemporalConflictError(ValueError):
    """Raised when the target contains future-open vectors and re-weave is disabled.

    A temporal conflict occurs when ``valid_from > effective_date``, meaning the
    incoming data must be inserted *before* an already-committed record. This
    requires explicit opt-in via ``allow_temporal_rerun=True``.

    Args:
        min_conflict_date: Earliest ``valid_from`` date found in the conflict.
        effective_date: The ``effective_date`` of the current run.
    """

    def __init__(self, min_conflict_date: object, effective_date: object) -> None:
        super().__init__(
            f"IntoHistory: temporal conflict detected — the target table contains records "
            f"with valid_from={min_conflict_date!r} after the current "
            f"effective_date={effective_date!r}. "
            f"Set allow_temporal_rerun=True to enable re-weave of historical data."
        )


__all__ = [
    "HistorifyKeyConflictError",
    "HistorifyDateCollisionError",
    "HistorifyDeleteMarkerError",
    "HistorifyTemporalConflictError",
]
