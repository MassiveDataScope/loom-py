"""Shared behavioral contract for SCD Type 2 historify integration tests.

All backend-agnostic scenario tests live here as a single class.
Backend-specific test classes inherit :class:`HistorifyContractTests` and
provide four pytest fixtures:

* ``writer``     — :class:`~loom.etl.runtime.contracts.TargetWriter` rooted
                   at the test's temporary directory.
* ``root``       — :class:`~pathlib.Path` of that temporary directory (used
                   to build expected table URIs in assertions).
* ``make_frame`` — callable ``(list[dict]) → backend-native frame``.
* ``read_table`` — callable ``(uri: str) → list[dict[str, Any]]`` (rows as
                   plain Python dicts with native-typed values). Timestamps
                   must come back as UTC: aware, or naive UTC wall time — naive
                   input datetimes are written as UTC.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history import (
    DeletePolicy,
    HistorifyDeleteMarkerError,
    HistorifyInputMode,
    HistorifyKeyConflictError,
    HistorifySpec,
    HistorifyTemporalConflictError,
    HistoryDateType,
)
from loom.etl.declarative.target._schema_mode import SchemaMode

# ---------------------------------------------------------------------------
# Shared helpers (module-level — imported by concrete test modules too)
# ---------------------------------------------------------------------------


@dataclass
class _Params:
    run_date: date


def _snapshot_spec(
    *,
    keys: tuple[str, ...] = ("player_id",),
    track: tuple[str, ...] | None = ("team_id",),
    overwrite: tuple[str, ...] | None = None,
    delete_policy: DeletePolicy = DeletePolicy.CLOSE,
    partition_scope: tuple[str, ...] | None = None,
    allow_temporal_rerun: bool = False,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("dim_players"),
        keys=keys,
        effective_date=p.run_date,
        mode=HistorifyInputMode.SNAPSHOT,
        track=track,
        overwrite=overwrite,
        delete_policy=delete_policy,
        partition_scope=partition_scope,
        schema_mode=SchemaMode.STRICT,
        allow_temporal_rerun=allow_temporal_rerun,
    )


def _log_spec(
    *,
    keys: tuple[str, ...] = ("subscription_id",),
    track: tuple[str, ...] | None = ("plan",),
    overwrite: tuple[str, ...] | None = None,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("dim_subs"),
        keys=keys,
        effective_date="event_date",
        mode=HistorifyInputMode.LOG,
        track=track,
        overwrite=overwrite,
        schema_mode=SchemaMode.STRICT,
    )


def _sub_event(plan: str | None, event_date: date, *, sub: int = 1, **extra: Any) -> dict[str, Any]:
    return {"subscription_id": sub, "plan": plan, "event_date": event_date, **extra}


def _replacement_spec(
    *,
    track: tuple[str, ...] | None = ("cost",),
    overwrite: tuple[str, ...] | None = None,
    date_type: HistoryDateType = HistoryDateType.TIMESTAMP,
) -> HistorifySpec:
    return HistorifySpec(
        table_ref=TableRef("dim_replacements"),
        keys=("wo", "rid"),
        effective_date="event_ts",
        mode=HistorifyInputMode.LOG,
        track=track,
        overwrite=overwrite,
        delete_marker="is_deleted",
        date_type=date_type,
        schema_mode=SchemaMode.STRICT,
    )


def _replacement(
    wo: str,
    rid: str,
    event_ts: date | datetime,
    cost: int | None,
    *,
    deleted: bool | None = False,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "wo": wo,
        "rid": rid,
        "event_ts": event_ts,
        "cost": cost,
        "is_deleted": deleted,
        **extra,
    }


def _naive(value: Any) -> Any:
    return value.replace(tzinfo=None) if isinstance(value, datetime) else value


_OCT10, _OCT11, _OCT12, _OCT15, _OCT20 = (datetime(2026, 10, d) for d in (10, 11, 12, 15, 20))
_OCT10_END, _OCT11_END, _OCT14_END = (
    datetime(2026, 10, d, 23, 59, 59, 999999) for d in (10, 11, 14)
)
_REPLACEMENTS_RUN1 = [
    _replacement("W1", "R1", _OCT10, 100),
    _replacement("W1", "R2", _OCT10, 50),
]
_REPLACEMENTS_RUN2 = [
    _replacement("W1", "R1", _OCT12, 120),
    _replacement("W1", "R2", _OCT12, None, deleted=True),
]
# (wo, rid, valid_from, valid_to, cost) after RUN1 + RUN2: R2 closed, no open row.
_REPLACEMENTS_AFTER_RUN2 = [
    ("W1", "R1", _OCT10, _OCT11_END, 100),
    ("W1", "R1", _OCT12, None, 120),
    ("W1", "R2", _OCT10, _OCT11_END, 50),
]


_D1, _D2, _D3 = date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)
_JUN, _SEP = date(2024, 6, 1), date(2024, 9, 1)

# (spec_kwargs, write batches, expected rows sorted by (subscription_id, valid_from)).
# Each expected row is a subset of columns asserted against the persisted history.
_LOG_COLLAPSE_CASES = [
    pytest.param(
        {"overwrite": ("price",)},
        [
            [
                _sub_event("basic", _D1, price=10, agent="a1"),
                _sub_event("basic", _D2, price=20, agent="a2"),
                _sub_event("basic", _D3, price=30, agent="a3"),
            ]
        ],
        [{"valid_from": _D1, "valid_to": None, "price": 30, "agent": "a1"}],
        id="same_track_run_collapses_overwrite_last_rest_first",
    ),
    pytest.param(
        {"overwrite": ("price",)},
        [
            [
                _sub_event("basic", _D1, price=10, agent="a1"),
                _sub_event("basic", _D2, price=20, agent="a2"),
                _sub_event("pro", _D3, price=30, agent="a3"),
            ]
        ],
        [
            {
                "plan": "basic",
                "valid_from": _D1,
                "valid_to": date(2024, 2, 29),
                "price": 20,
                "agent": "a1",
            },
            {"plan": "pro", "valid_from": _D3, "valid_to": None, "price": 30, "agent": "a3"},
        ],
        id="track_change_freezes_previous_run",
    ),
    pytest.param(
        {"overwrite": ("price",)},
        [
            [
                _sub_event("basic", _D1, price=10),
                _sub_event("pro", _JUN, price=20),
                _sub_event("basic", _SEP, price=30),
            ]
        ],
        [
            {"plan": "basic", "valid_from": _D1, "valid_to": date(2024, 5, 31), "price": 10},
            {"plan": "pro", "valid_from": _JUN, "valid_to": date(2024, 8, 31), "price": 20},
            {"plan": "basic", "valid_from": _SEP, "valid_to": None, "price": 30},
        ],
        id="recurrence_not_collapsed_three_versions",
    ),
    pytest.param(
        {"overwrite": None},
        [
            [
                _sub_event("basic", _D1, agent="a1"),
                _sub_event("basic", _D2, agent="a2"),
                _sub_event("basic", _D3, agent="a3"),
            ]
        ],
        [{"valid_from": _D1, "valid_to": None, "agent": "a1"}],
        id="pure_freeze_without_overwrite",
    ),
    pytest.param(
        {},
        [[_sub_event(None, _D1), _sub_event(None, _D2), _sub_event("pro", _D3)]],
        [
            {"plan": None, "valid_from": _D1, "valid_to": date(2024, 2, 29)},
            {"plan": "pro", "valid_from": _D3, "valid_to": None},
        ],
        id="null_track_values_collapse_consecutively",
    ),
    pytest.param(
        {"overwrite": ("price",)},
        [[_sub_event("basic", _D1, price=10), _sub_event("basic", _D2, price=None)]],
        [{"valid_from": _D1, "price": None}],
        id="overwrite_null_last_clobbers_open_vector",
    ),
    pytest.param(
        {},
        [
            [
                _sub_event("basic", _D1, sub=1),
                _sub_event("basic", _D1, sub=2),
                _sub_event("basic", _D2, sub=1),
                _sub_event("pro", _D2, sub=2),
            ]
        ],
        [
            {"subscription_id": 1, "plan": "basic", "valid_from": _D1, "valid_to": None},
            {
                "subscription_id": 2,
                "plan": "basic",
                "valid_from": _D1,
                "valid_to": date(2024, 1, 31),
            },
            {"subscription_id": 2, "plan": "pro", "valid_from": _D2, "valid_to": None},
        ],
        id="collapse_partitions_by_entity",
    ),
    pytest.param(
        {"overwrite": ("price",)},
        [
            [_sub_event("basic", _D1, price=10, agent="a1")],
            [_sub_event("basic", _D2, price=20, agent="a2")],
            [_sub_event("pro", _D3, price=30, agent="a3")],
        ],
        [
            {
                "plan": "basic",
                "valid_from": _D1,
                "valid_to": date(2024, 2, 29),
                "price": 20,
                "agent": "a1",
            },
            {"plan": "pro", "valid_from": _D3, "valid_to": None, "price": 30, "agent": "a3"},
        ],
        id="incremental_freeze_across_three_writes",
    ),
    pytest.param(
        {"overwrite": ("price",)},
        [
            [_sub_event("basic", _D1, price=10, agent="a1")],
            [_sub_event("basic", _JUN, price=20, agent="a2")],
        ],
        [{"valid_from": _D1, "valid_to": None, "price": 20, "agent": "a1"}],
        id="overwrite_refreshes_open_vector_across_writes",
    ),
    pytest.param(
        {"overwrite": ("price",)},
        [
            [
                _sub_event("basic", _D1, price=10, agent="a1"),
                _sub_event("basic", _D2, price=20, agent="a2"),
                _sub_event("basic", _D3, price=30, agent="a3"),
            ]
        ]
        * 2,
        [{"valid_from": _D1, "valid_to": None, "price": 30, "agent": "a1"}],
        id="same_track_collapse_replay_is_idempotent",
    ),
]


# ---------------------------------------------------------------------------
# Abstract behavioral contract
# ---------------------------------------------------------------------------


class HistorifyContractTests:
    """Behavioral contract for SCD Type 2 historify across all backends.

    Concrete subclasses must provide the four fixtures below.  Pytest
    discovers and runs every ``test_*`` method defined here as part of each
    concrete subclass.

    Required fixtures
    -----------------
    writer : TargetWriter
        Writer rooted at the test's temporary directory.
    root : Path
        The same temporary directory used by ``writer``.
    make_frame : Callable[[list[dict]], frame]
        Factory that converts a list of row-dicts to a backend frame.
    read_table : Callable[[str], list[dict[str, Any]]]
        Reads a Delta table at the given URI and returns rows as plain dicts.
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _uri(self, root: Path, name: str = "dim_players") -> str:
        return str(root / name)

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def test_creates_table(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        assert len(read_table(self._uri(root))) == 2

    def test_valid_from_equals_eff_date(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        rows = read_table(self._uri(root))
        assert rows[0]["valid_from"] == date(2024, 1, 1)

    def test_valid_to_is_null_on_first_write(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        rows = read_table(self._uri(root))
        assert all(r["valid_to"] is None for r in rows)

    # ------------------------------------------------------------------
    # No-op (no tracked changes)
    # ------------------------------------------------------------------

    def test_no_new_rows_when_unchanged(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = read_table(self._uri(root))
        assert len(rows) == 1
        assert rows[0]["valid_to"] is None

    def test_valid_from_preserved_on_no_change(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = read_table(self._uri(root))
        assert rows[0]["valid_from"] == date(2024, 1, 1)

    # ------------------------------------------------------------------
    # Null-safe track comparison (no-op when the tracked value is null)
    # ------------------------------------------------------------------

    def test_null_tracked_value_is_stable_across_runs(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A track column that is null in both existing and incoming rows must
        compare equal, not "changed" — a null must not reopen the vector on
        every run."""
        frame = make_frame([{"player_id": 1, "team_id": None}, {"player_id": 2, "team_id": "RM"}])
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 1)))
        writer.write(frame, _snapshot_spec(), _Params(run_date=date(2024, 1, 2)))
        rows = read_table(self._uri(root))
        player_1 = [r for r in rows if r["player_id"] == 1]
        assert len(player_1) == 1
        assert player_1[0]["valid_from"] == date(2024, 1, 1)
        assert player_1[0]["valid_to"] is None

    def test_overwrite_refreshes_open_row_with_null_track(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Overwrite must still land on the open row when join_key includes a
        null track value — the overwrite join must be null-safe too."""
        spec = _snapshot_spec(track=("team_id",), overwrite=("email",))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": None, "email": "old@x.com"},
                    {"player_id": 2, "team_id": "RM", "email": "rm@x.com"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": None, "email": "new@x.com"},
                    {"player_id": 2, "team_id": "RM", "email": "rm@x.com"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        player_1 = [r for r in rows if r["player_id"] == 1]
        assert len(player_1) == 1
        assert player_1[0]["email"] == "new@x.com"
        assert player_1[0]["valid_from"] == date(2024, 1, 1)

    def test_null_to_value_transition_closes_and_opens(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A track column flipping null -> value is a real change: it must
        close the old (null) vector and open a new one, same as any other
        tracked change."""
        spec = _snapshot_spec(track=("team_id",))
        writer.write(
            make_frame([{"player_id": 1, "team_id": None}, {"player_id": 2, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}, {"player_id": 2, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        player_1 = sorted((r for r in rows if r["player_id"] == 1), key=lambda r: r["valid_from"])
        assert len(player_1) == 2
        assert player_1[0]["team_id"] is None
        assert player_1[0]["valid_to"] == date(2024, 5, 31)
        assert player_1[1]["team_id"] == "BCA"
        assert player_1[1]["valid_to"] is None

    # ------------------------------------------------------------------
    # New entity
    # ------------------------------------------------------------------

    def test_new_player_inserted(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        assert len(read_table(self._uri(root))) == 2

    def test_new_player_valid_from_is_eff_date(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 5)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_from"] == date(2024, 1, 5)
        assert p2[0]["valid_to"] is None

    # ------------------------------------------------------------------
    # Tracked column change
    # ------------------------------------------------------------------

    def test_two_rows_after_team_change(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        assert len(read_table(self._uri(root))) == 2

    def test_old_row_valid_to_is_prev_day(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)

    def test_new_row_is_open_after_change(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        new = [r for r in rows if r["team_id"] == "BCA"]
        assert new[0]["valid_from"] == date(2024, 6, 1)
        assert new[0]["valid_to"] is None

    # ------------------------------------------------------------------
    # Delete policies
    # ------------------------------------------------------------------

    def test_close_sets_valid_to_on_deleted_entity(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(delete_policy=DeletePolicy.CLOSE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_to"] == date(2024, 5, 31)

    def test_ignore_leaves_deleted_vector_open(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(delete_policy=DeletePolicy.IGNORE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["valid_to"] is None

    def test_soft_delete_sets_deleted_at(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert p2[0]["deleted_at"] == date(2024, 6, 1)
        assert p2[0]["valid_to"] == date(2024, 5, 31)

    def test_ignore_closes_changed_entity_old_version(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """IGNORE closes the old version of an entity that changed in the snapshot.

        IGNORE policy means "don't close rows for entities absent from the snapshot."
        When an entity IS present but with different tracked values, the old version
        is definitively superseded — it must be closed regardless of delete policy.
        """
        spec = _snapshot_spec(delete_policy=DeletePolicy.IGNORE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 2
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)
        new = [r for r in rows if r["team_id"] == "BCA"]
        assert new[0]["valid_to"] is None

    def test_soft_delete_does_not_stamp_deleted_at_on_changed_entity(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """SOFT_DELETE does not stamp deleted_at when an entity changes tracked values.

        deleted_at marks an entity as absent from the snapshot (truly gone).
        When a new snapshot version replaces an old one, the old row is closed
        with valid_to only — no deleted_at, because the entity was not deleted.
        """
        spec = _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)
        assert old[0]["deleted_at"] is None
        new = [r for r in rows if r["team_id"] == "BCA"]
        assert new[0]["valid_to"] is None
        assert new[0]["deleted_at"] is None

    # ------------------------------------------------------------------
    # Loan case — multiple simultaneous open vectors
    # ------------------------------------------------------------------

    def test_two_vectors_open_simultaneously(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        frame = make_frame(
            [
                {"player_id": 1, "team_id": "RM", "role": "owner"},
                {"player_id": 1, "team_id": "GET", "role": "loan"},
            ]
        )
        writer.write(frame, spec, _Params(run_date=date(2024, 1, 1)))
        rows = read_table(self._uri(root))
        assert len(rows) == 2
        assert all(r["valid_to"] is None for r in rows)

    def test_loan_end_closes_only_loan_vector(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "role": "owner"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 2
        loan = [r for r in rows if r["role"] == "loan"]
        assert loan[0]["valid_to"] == date(2024, 5, 31)
        owner = [r for r in rows if r["role"] == "owner"]
        assert owner[0]["valid_to"] is None

    def test_both_vectors_change_simultaneously(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Both open vectors of an entity change on the same run date.

        Expected: 4 rows — 2 closed (old teams), 2 open (new teams).
        This verifies the engine handles multi-vector close+open atomically.
        """
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "MCF", "role": "owner"},
                    {"player_id": 1, "team_id": "BAR", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 4
        open_rows = [r for r in rows if r["valid_to"] is None]
        closed_rows = [r for r in rows if r["valid_to"] is not None]
        assert len(open_rows) == 2
        assert len(closed_rows) == 2
        assert all(r["valid_to"] == date(2024, 5, 31) for r in closed_rows)
        open_teams = {r["team_id"] for r in open_rows}
        assert open_teams == {"MCF", "BAR"}

    def test_third_vector_added_to_entity_with_two_open(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Entity gains a third simultaneous vector while two are already open.

        Expected: 3 rows open, no rows closed — existing vectors are unchanged,
        new vector is inserted with its own valid_from.
        """
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                    {"player_id": 1, "team_id": "SEV", "role": "loan2"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 3
        assert all(r["valid_to"] is None for r in rows)
        new_vec = [r for r in rows if r["role"] == "loan2"]
        assert new_vec[0]["valid_from"] == date(2024, 6, 1)

    def test_idempotency_stable_with_two_open_vectors(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Same-day rerun is stable when entity has two simultaneous open vectors.

        Expected: 4 rows after first change (2 old closed, 2 new open).
        Rerunning on the same date must not add or remove rows.
        """
        spec = _snapshot_spec(keys=("player_id",), track=("team_id", "role"))
        writer.write(
            make_frame(
                [
                    {"player_id": 1, "team_id": "RM", "role": "owner"},
                    {"player_id": 1, "team_id": "GET", "role": "loan"},
                ]
            ),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = make_frame(
            [
                {"player_id": 1, "team_id": "MCF", "role": "owner"},
                {"player_id": 1, "team_id": "BAR", "role": "loan"},
            ]
        )
        writer.write(frame2, spec, _Params(run_date=date(2024, 6, 1)))
        writer.write(frame2, spec, _Params(run_date=date(2024, 6, 1)))  # rerun
        rows = read_table(self._uri(root))
        assert len(rows) == 4

    # ------------------------------------------------------------------
    # Passive columns
    # ------------------------------------------------------------------

    def test_passive_change_does_not_trigger_new_row(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _snapshot_spec(keys=("player_id",), track=("team_id",))
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "salary": 100}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "salary": 150}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 1
        assert rows[0]["salary"] == 100

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------

    def test_same_day_rerun_stable_row_count(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = make_frame([{"player_id": 1, "team_id": "BCA"}])
        writer.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        writer.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        assert len(read_table(self._uri(root))) == 2

    def test_same_day_rerun_stable_valid_to(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        frame2 = make_frame([{"player_id": 1, "team_id": "BCA"}])
        for _ in range(3):
            writer.write(frame2, _snapshot_spec(), _Params(run_date=date(2024, 6, 1)))
        rows = read_table(self._uri(root))
        old = [r for r in rows if r["team_id"] == "RM"]
        assert old[0]["valid_to"] == date(2024, 5, 31)

    def test_soft_delete_rerun_reopens_deleted_entity(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Same-day rerun reopens a soft-deleted entity and clears deleted_at."""
        spec = _snapshot_spec(delete_policy=DeletePolicy.SOFT_DELETE)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}, {"player_id": 2, "team_id": "BCA"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),  # rerun
        )
        rows = read_table(self._uri(root))
        p2 = [r for r in rows if r["player_id"] == 2]
        assert len(p2) == 1
        assert p2[0]["valid_to"] is None
        assert p2[0]["deleted_at"] is None

    # ------------------------------------------------------------------
    # Backfill rewind (allow_temporal_rerun)
    # ------------------------------------------------------------------

    def test_backfill_rewind_rebuilds_history(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Backfill discards runs at/after the rerun date and re-weaves from there."""
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "LIV"}]),
            _snapshot_spec(allow_temporal_rerun=True),
            _Params(run_date=date(2024, 3, 1)),
        )
        rows = sorted(read_table(self._uri(root)), key=lambda r: r["valid_from"])
        assert [(r["team_id"], r["valid_from"], r["valid_to"]) for r in rows] == [
            ("RM", date(2024, 1, 1), date(2024, 2, 29)),
            ("LIV", date(2024, 3, 1), None),
        ]

    def test_backfill_rewind_is_idempotent(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Re-running the same backfill produces the exact same history."""
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "BCA"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        backfill = [{"player_id": 1, "team_id": "LIV"}]
        for _ in range(2):
            writer.write(
                make_frame(backfill),
                _snapshot_spec(allow_temporal_rerun=True),
                _Params(run_date=date(2024, 3, 1)),
            )
        rows = sorted(read_table(self._uri(root)), key=lambda r: r["valid_from"])
        assert [(r["team_id"], r["valid_from"], r["valid_to"]) for r in rows] == [
            ("RM", date(2024, 1, 1), date(2024, 2, 29)),
            ("LIV", date(2024, 3, 1), None),
        ]

    # ------------------------------------------------------------------
    # Temporal conflict guard
    # ------------------------------------------------------------------

    def test_backfill_raises_when_disabled(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        frame = make_frame([{"player_id": 1, "team_id": "RM"}])
        spec = _snapshot_spec(allow_temporal_rerun=False)
        params = _Params(run_date=date(2024, 1, 1))
        with pytest.raises(HistorifyTemporalConflictError):
            writer.write(
                frame,
                spec,
                params,
            )

    def test_backfill_allowed_when_flag_set(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
    ) -> None:
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(),
            _Params(run_date=date(2024, 6, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM"}]),
            _snapshot_spec(allow_temporal_rerun=True),
            _Params(run_date=date(2024, 1, 1)),
        )

    # ------------------------------------------------------------------
    # LOG mode
    # ------------------------------------------------------------------

    def test_log_events_get_valid_from(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = sorted(read_table(self._uri(root, "dim_subs")), key=lambda r: r["valid_from"])
        assert [r["valid_from"] for r in rows] == [date(2024, 1, 1), date(2024, 6, 1)]

    def test_log_first_event_closed_by_second(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = read_table(self._uri(root, "dim_subs"))
        basic = [r for r in rows if r["plan"] == "basic"]
        assert basic[0]["valid_to"] == date(2024, 5, 31)

    def test_log_last_event_is_open(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = read_table(self._uri(root, "dim_subs"))
        pro = [r for r in rows if r["plan"] == "pro"]
        assert pro[0]["valid_to"] is None

    def test_log_new_event_closes_previous(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)}]),
            _log_spec(),
            None,
        )
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)}]),
            _log_spec(),
            None,
        )
        rows = read_table(self._uri(root, "dim_subs"))
        assert len(rows) == 2
        basic = [r for r in rows if r["plan"] == "basic"]
        assert basic[0]["valid_to"] == date(2024, 5, 31)

    def test_log_unaffected_entities_preserved(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        writer.write(
            make_frame(
                [
                    {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                    {"subscription_id": 2, "plan": "pro", "event_date": date(2024, 1, 1)},
                ]
            ),
            _log_spec(),
            None,
        )
        writer.write(
            make_frame(
                [{"subscription_id": 1, "plan": "enterprise", "event_date": date(2024, 9, 1)}]
            ),
            _log_spec(),
            None,
        )
        rows = read_table(self._uri(root, "dim_subs"))
        sub2 = [r for r in rows if r["subscription_id"] == 2]
        assert len(sub2) == 1
        assert sub2[0]["valid_to"] is None

    def test_log_recurring_value_creates_three_versions(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 9, 1)},
            ]
        )
        writer.write(frame, _log_spec(), None)
        rows = sorted(read_table(self._uri(root, "dim_subs")), key=lambda r: r["valid_from"])
        assert len(rows) == 3
        assert [r["plan"] for r in rows] == ["basic", "pro", "basic"]
        assert [r["valid_from"] for r in rows] == [
            date(2024, 1, 1),
            date(2024, 6, 1),
            date(2024, 9, 1),
        ]
        assert [r["valid_to"] for r in rows] == [date(2024, 5, 31), date(2024, 8, 31), None]

    def test_log_recurrence_replay_is_idempotent(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        events = [
            {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
            {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
            {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 9, 1)},
        ]
        writer.write(make_frame(events), _log_spec(), None)
        writer.write(make_frame(events), _log_spec(), None)  # replay
        rows = read_table(self._uri(root, "dim_subs"))
        assert len(rows) == 3
        froms = [r["valid_from"] for r in rows]
        assert len(set(froms)) == 3
        for r in rows:
            assert r["valid_to"] is None or r["valid_to"] >= r["valid_from"]

    def test_log_same_instant_duplicate_raises(
        self,
        writer: Any,
        make_frame: Callable,
    ) -> None:
        frame = make_frame(
            [
                {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)},
            ]
        )
        spec = _log_spec()
        with pytest.raises(HistorifyKeyConflictError):
            writer.write(frame, spec, None)

    def test_log_same_date_correction_replaces_event(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A later write for the same (key, date) replaces the stored event."""
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)}]),
            _log_spec(),
            None,
        )
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "pro", "event_date": date(2024, 1, 1)}]),
            _log_spec(),
            None,
        )
        rows = read_table(self._uri(root, "dim_subs"))
        assert len(rows) == 1
        assert rows[0]["plan"] == "pro"
        assert rows[0]["valid_from"] == date(2024, 1, 1)
        assert rows[0]["valid_to"] is None

    def test_log_same_date_correction_preserves_later_boundaries(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Correcting an early event keeps boundaries against later events intact."""
        writer.write(
            make_frame(
                [
                    {"subscription_id": 1, "plan": "basic", "event_date": date(2024, 1, 1)},
                    {"subscription_id": 1, "plan": "pro", "event_date": date(2024, 6, 1)},
                ]
            ),
            _log_spec(),
            None,
        )
        writer.write(
            make_frame([{"subscription_id": 1, "plan": "premium", "event_date": date(2024, 1, 1)}]),
            _log_spec(),
            None,
        )
        rows = sorted(read_table(self._uri(root, "dim_subs")), key=lambda r: r["valid_from"])
        assert [(r["plan"], r["valid_from"], r["valid_to"]) for r in rows] == [
            ("premium", date(2024, 1, 1), date(2024, 5, 31)),
            ("pro", date(2024, 6, 1), None),
        ]

    # ------------------------------------------------------------------
    # LOG mode — same-track collapse + overwrite (table-driven)
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(("spec_kwargs", "batches", "expected"), _LOG_COLLAPSE_CASES)
    def test_log_same_track_collapse(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
        spec_kwargs: dict[str, Any],
        batches: list[list[dict[str, Any]]],
        expected: list[dict[str, Any]],
    ) -> None:
        """Consecutive same-track events collapse per entity: valid_from=first run event,
        overwrite cols=last observed, all other payload cols frozen to first."""
        spec = _log_spec(**spec_kwargs)
        for batch in batches:
            writer.write(make_frame(batch), spec, None)
        rows = sorted(
            read_table(self._uri(root, "dim_subs")),
            key=lambda r: (r["subscription_id"], r["valid_from"]),
        )
        assert len(rows) == len(expected)
        for actual, want in zip(rows, expected, strict=True):
            assert {col: actual[col] for col in want} == want

    # ------------------------------------------------------------------
    # LOG mode — delete marker (tombstones)
    # ------------------------------------------------------------------

    def _replacement_vectors(self, root: Path, read_table: Callable) -> list[tuple[Any, ...]]:
        rows = read_table(self._uri(root, "dim_replacements"))
        assert all("is_deleted" not in row for row in rows)
        vectors = [
            (r["wo"], r["rid"], _naive(r["valid_from"]), _naive(r["valid_to"]), r["cost"])
            for r in rows
        ]
        return sorted(vectors, key=lambda v: v[:3])

    def _write_batches(
        self, writer: Any, make_frame: Callable, spec: HistorifySpec, *batches: list[dict]
    ) -> None:
        for batch in batches:
            writer.write(make_frame(batch), spec, None)

    @pytest.mark.parametrize("track", [("cost",), None], ids=["explicit_track", "track_none"])
    def test_log_delete_marker_closes_open_vector(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
        track: tuple[str, ...] | None,
    ) -> None:
        """A tombstone closes the open vector one period before it and opens nothing."""
        spec = _replacement_spec(track=track)
        self._write_batches(writer, make_frame, spec, _REPLACEMENTS_RUN1, _REPLACEMENTS_RUN2)
        assert self._replacement_vectors(root, read_table) == _REPLACEMENTS_AFTER_RUN2

    def test_log_delete_marker_rerun_is_idempotent(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer, make_frame, spec, _REPLACEMENTS_RUN1, _REPLACEMENTS_RUN2, _REPLACEMENTS_RUN2
        )
        assert self._replacement_vectors(root, read_table) == _REPLACEMENTS_AFTER_RUN2

    def test_log_delete_marker_leaves_other_keys_untouched(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W2", "R1", _OCT15, 10)],
        )
        assert self._replacement_vectors(root, read_table) == [
            *_REPLACEMENTS_AFTER_RUN2,
            ("W2", "R1", _OCT15, None, 10),
        ]

    def test_log_delete_marker_event_after_tombstone_reopens(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT15, 55)],
        )
        assert self._replacement_vectors(root, read_table) == [
            *_REPLACEMENTS_AFTER_RUN2,
            ("W1", "R2", _OCT15, None, 55),
        ]

    def test_log_delete_marker_closure_survives_earlier_replay(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Replaying an event dated before a stored closure does not reopen the key."""
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT11, 50)],
        )
        assert self._replacement_vectors(root, read_table) == _REPLACEMENTS_AFTER_RUN2

    def test_log_delete_marker_closure_survives_later_events_after_reopen(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A stored closure followed by a reopen keeps its gap when the key is rebuilt."""
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT15, 55)],
            [_replacement("W1", "R2", _OCT20, 60)],
        )
        r2 = [v for v in self._replacement_vectors(root, read_table) if v[1] == "R2"]
        assert r2 == [
            ("W1", "R2", _OCT10, _OCT11_END, 50),
            ("W1", "R2", _OCT15, datetime(2026, 10, 19, 23, 59, 59, 999999), 55),
            ("W1", "R2", _OCT20, None, 60),
        ]

    def test_log_delete_marker_consecutive_tombstones_keep_first(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT15, 50, deleted=True)],
        )
        assert self._replacement_vectors(root, read_table) == _REPLACEMENTS_AFTER_RUN2

    def test_log_delete_marker_without_history_writes_nothing(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            [*_REPLACEMENTS_RUN1, _replacement("W3", "R1", _OCT10, 7, deleted=True)],
            [_replacement("W4", "R1", _OCT12, 7, deleted=True)],
        )
        assert self._replacement_vectors(root, read_table) == [
            ("W1", "R1", _OCT10, None, 100),
            ("W1", "R2", _OCT10, None, 50),
        ]

    def test_log_delete_marker_resolves_within_one_batch(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Tombstones sharing the open vector's track values still close it, and
        consecutive tombstones collapse to the first one."""
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            [
                _replacement("W1", "R1", _OCT10, 100),
                _replacement("W1", "R1", _OCT12, 100, deleted=True),
                _replacement("W1", "R1", datetime(2026, 10, 13), 100, deleted=True),
                _replacement("W1", "R1", _OCT15, 100),
            ],
        )
        assert self._replacement_vectors(root, read_table) == [
            ("W1", "R1", _OCT10, _OCT11_END, 100),
            ("W1", "R1", _OCT15, None, 100),
        ]

    def test_log_delete_marker_date_precision(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec(date_type=HistoryDateType.DATE)
        self._write_batches(
            writer,
            make_frame,
            spec,
            [_replacement("W1", "R1", date(2026, 10, 10), 100)],
            [_replacement("W1", "R1", date(2026, 10, 12), 100, deleted=True)],
            [_replacement("W1", "R1", date(2026, 10, 11), 100)],
        )
        assert self._replacement_vectors(root, read_table) == [
            ("W1", "R1", date(2026, 10, 10), date(2026, 10, 11), 100),
        ]

    def test_log_delete_marker_missing_column_raises_on_first_run(
        self,
        writer: Any,
        make_frame: Callable,
    ) -> None:
        frame = make_frame([{"wo": "W1", "rid": "R1", "event_ts": _OCT10, "cost": 100}])
        spec = _replacement_spec()
        with pytest.raises(HistorifyDeleteMarkerError, match="is_deleted"):
            writer.write(frame, spec, None)

    def test_log_delete_marker_missing_column_raises_on_later_run(
        self,
        writer: Any,
        make_frame: Callable,
    ) -> None:
        spec = _replacement_spec()
        writer.write(make_frame(_REPLACEMENTS_RUN1), spec, None)
        frame = make_frame([{"wo": "W1", "rid": "R2", "event_ts": _OCT12, "cost": 50}])
        with pytest.raises(HistorifyDeleteMarkerError, match="is_deleted"):
            writer.write(frame, spec, None)

    def test_log_delete_marker_non_boolean_column_raises(
        self,
        writer: Any,
        make_frame: Callable,
    ) -> None:
        frame = make_frame(
            [
                _replacement("W1", "R1", _OCT10, 100, deleted="N"),  # type: ignore[arg-type]
                _replacement("W1", "R1", _OCT12, 100, deleted="Y"),  # type: ignore[arg-type]
            ]
        )
        spec = _replacement_spec()
        with pytest.raises(HistorifyDeleteMarkerError, match="is_deleted.*cast it to Boolean"):
            writer.write(frame, spec, None)

    def test_log_delete_marker_tombstone_and_event_at_same_instant_raise(
        self,
        writer: Any,
        make_frame: Callable,
    ) -> None:
        frame = make_frame(
            [
                _replacement("W1", "R1", _OCT10, 100),
                _replacement("W1", "R1", _OCT10, 100, deleted=True),
            ]
        )
        spec = _replacement_spec()
        with pytest.raises(HistorifyKeyConflictError, match="effective_date"):
            writer.write(frame, spec, None)

    def test_log_delete_marker_null_marker_is_regular_event(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            [
                _replacement("W1", "R1", _OCT10, 100, deleted=None),
                _replacement("W1", "R2", _OCT10, 50),
            ],
            [
                _replacement("W1", "R1", _OCT12, 120, deleted=None),
                _replacement("W1", "R2", _OCT12, None, deleted=True),
            ],
        )
        assert self._replacement_vectors(root, read_table) == _REPLACEMENTS_AFTER_RUN2

    def test_log_delete_marker_event_at_closure_instant_reopens(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A regular event at the stored tombstone instant replaces it (incoming wins)."""
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT12, 55)],
        )
        assert self._replacement_vectors(root, read_table) == [
            *_REPLACEMENTS_AFTER_RUN2,
            ("W1", "R2", _OCT12, None, 55),
        ]

    def test_log_delete_marker_retraction_merges_back(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A same-track regular event at the tombstone instant undoes the deletion."""
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT12, 50)],
        )
        assert self._replacement_vectors(root, read_table) == [
            *_REPLACEMENTS_AFTER_RUN2[:2],
            ("W1", "R2", _OCT10, None, 50),
        ]

    def test_log_delete_marker_overwrite_keeps_last_value_before_tombstone(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec(overwrite=("note",))
        self._write_batches(
            writer,
            make_frame,
            spec,
            [
                _replacement("W1", "R1", _OCT10, 100, note="a"),
                _replacement("W1", "R1", _OCT11, 100, note="b"),
                _replacement("W1", "R1", _OCT12, 100, deleted=True, note="z"),
            ],
        )
        rows = read_table(self._uri(root, "dim_replacements"))
        assert [(_naive(r["valid_from"]), _naive(r["valid_to"]), r["note"]) for r in rows] == [
            (_OCT10, _OCT11_END, "b")
        ]

    def test_log_delete_marker_late_tombstone_opens_gap_before_later_vector(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            [_replacement("W1", "R1", _OCT10, 100), _replacement("W1", "R1", _OCT15, 120)],
            [_replacement("W1", "R1", _OCT12, 100, deleted=True)],
        )
        assert self._replacement_vectors(root, read_table) == [
            ("W1", "R1", _OCT10, _OCT11_END, 100),
            ("W1", "R1", _OCT15, None, 120),
        ]

    def test_log_delete_marker_tombstone_at_stored_valid_from_replaces_vector(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            [_replacement("W1", "R1", _OCT10, 100), _replacement("W1", "R1", _OCT15, 120)],
            [_replacement("W1", "R1", _OCT15, 120, deleted=True)],
        )
        assert self._replacement_vectors(root, read_table) == [
            ("W1", "R1", _OCT10, _OCT14_END, 100),
        ]

    def test_log_delete_marker_replay_with_new_track_value_stays_closed(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        spec = _replacement_spec()
        self._write_batches(
            writer,
            make_frame,
            spec,
            _REPLACEMENTS_RUN1,
            _REPLACEMENTS_RUN2,
            [_replacement("W1", "R2", _OCT11, 70)],
        )
        assert self._replacement_vectors(root, read_table) == [
            *_REPLACEMENTS_AFTER_RUN2[:2],
            ("W1", "R2", _OCT10, _OCT10_END, 50),
            ("W1", "R2", _OCT11, _OCT11_END, 70),
        ]

    def test_log_delete_marker_same_day_tombstone_at_date_precision_is_stable(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """An event and a tombstone on the same day leave a stable zero-width [D, D] vector."""
        spec = _replacement_spec(date_type=HistoryDateType.DATE)
        batch = [
            _replacement("W1", "R1", datetime(2026, 10, 10, 8), 100),
            _replacement("W1", "R1", datetime(2026, 10, 10, 17), 100, deleted=True),
        ]
        expected = [("W1", "R1", date(2026, 10, 10), date(2026, 10, 10), 100)]
        self._write_batches(writer, make_frame, spec, batch)
        assert self._replacement_vectors(root, read_table) == expected
        self._write_batches(writer, make_frame, spec, batch)
        assert self._replacement_vectors(root, read_table) == expected

    # ------------------------------------------------------------------
    # Overwrite columns
    # ------------------------------------------------------------------

    def test_overwrite_col_updated_on_unchanged_entity(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Overwrite columns are refreshed on the open row; no new history row created."""
        spec = _snapshot_spec(track=("team_id",), overwrite=("email",))
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "email": "old@rm.com"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "email": "new@rm.com"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 1
        assert rows[0]["email"] == "new@rm.com"
        assert rows[0]["valid_from"] == date(2024, 1, 1)

    def test_overwrite_does_not_trigger_new_history_row(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """A change in an overwrite column alone must not produce a new history row."""
        spec = _snapshot_spec(track=("team_id",), overwrite=("email",))
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "email": "v1@rm.com"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        for i in range(3):
            writer.write(
                make_frame([{"player_id": 1, "team_id": "RM", "email": f"v{i + 2}@rm.com"}]),
                spec,
                _Params(run_date=date(2024, 1, i + 2)),
            )
        rows = read_table(self._uri(root))
        assert len(rows) == 1

    def test_overwrite_none_leaves_passive_col_frozen(
        self,
        writer: Any,
        root: Path,
        make_frame: Callable,
        read_table: Callable,
    ) -> None:
        """Without overwrite, passive columns carry the value from the first write."""
        spec = _snapshot_spec(track=("team_id",), overwrite=None)
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "email": "original@rm.com"}]),
            spec,
            _Params(run_date=date(2024, 1, 1)),
        )
        writer.write(
            make_frame([{"player_id": 1, "team_id": "RM", "email": "changed@rm.com"}]),
            spec,
            _Params(run_date=date(2024, 6, 1)),
        )
        rows = read_table(self._uri(root))
        assert len(rows) == 1
        assert rows[0]["email"] == "original@rm.com"
