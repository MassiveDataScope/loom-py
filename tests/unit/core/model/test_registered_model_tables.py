from __future__ import annotations

from loom.core.model.privilege import READ_WRITE
from loom.core.model.scoped import ScopedTable, registered_model_tables


def _table(name: str, partition_by: str | None = None) -> ScopedTable:
    return ScopedTable(None, name, (), READ_WRITE, partition_by)


def test_partitions_of_a_partitioned_scoped_table_stand_for_their_parent() -> None:
    scoped = [_table("notes"), _table("events", "at")]
    registered = {"notes": None, "events": None, "events_p202601": "events"}

    assert registered_model_tables(registered, scoped) == {"notes", "events"}


def test_a_partition_of_a_table_that_is_not_a_partitioned_scoped_table_counts_as_itself() -> None:
    scoped = [_table("notes"), _table("events", "at")]
    registered = {"notes_p1": "notes", "logs_p1": "logs", "stray": None}

    assert registered_model_tables(registered, scoped) == {"notes_p1", "logs_p1", "stray"}
