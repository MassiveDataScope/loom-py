"""Table-based lineage store using backend writer adapters."""

from __future__ import annotations

from loom.core.errors import InvalidType
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.lineage._records import LineageRecord, get_lineage_table_name
from loom.etl.lineage.sinks._protocol import LineageWriter


class TableLineageStore:
    """Persist lineage records into backend tables."""

    def __init__(self, writer: LineageWriter, *, database: str = "") -> None:
        self._writer = writer
        self._database = database.strip()

    def write_record(self, record: LineageRecord) -> None:
        """Append *record* to the corresponding table."""
        try:
            table_name = get_lineage_table_name(type(record))
        except KeyError as exc:
            raise InvalidType(
                f"TableLineageStore: unrecognised record type {type(record)!r}"
            ) from exc
        self._writer.write_record(record, _table_ref(self._database, table_name))


def _table_ref(database: str, table_name: str) -> TableRef:
    if database:
        return TableRef(f"{database}.{table_name}")
    return TableRef(table_name)


__all__ = ["TableLineageStore"]
