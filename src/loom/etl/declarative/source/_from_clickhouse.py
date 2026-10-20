"""ClickHouse source builder — FromClickHouse."""

from __future__ import annotations

from typing import Any

from loom.core.routing import LogicalRef
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source._specs import ClickHouseSourceSpec
from loom.etl.schema._contract import SchemaContract, resolve_schema
from loom.etl.schema._schema import ColumnSchema


class FromClickHouse:
    """Declare a ClickHouse table or view as an ETL source.

    The builder mirrors :class:`~loom.etl.declarative.source.FromTable` so it
    fits the same authoring pattern:

    * ``where(...)`` for predicates
    * ``columns(...)`` for scan-time projection
    * ``with_schema(...)`` for Loom schema contracts
    * ``distinct()`` and ``unbounded()`` for query shaping / explicit scans
    """

    __slots__ = (
        "_ref",
        "_predicates",
        "_schema",
        "_columns",
        "_distinct",
        "_allow_full_scan",
    )

    def __init__(self, ref: str | LogicalRef | TableRef) -> None:
        self._ref: TableRef = ref if isinstance(ref, TableRef) else TableRef(ref)
        self._predicates: tuple[Any, ...] = ()
        self._schema: tuple[ColumnSchema, ...] = ()
        self._columns: tuple[str, ...] = ()
        self._distinct: bool = False
        self._allow_full_scan: bool = False

    @property
    def table_ref(self) -> TableRef:
        """The ClickHouse table/view reference."""
        return self._ref

    @property
    def predicates(self) -> tuple[Any, ...]:
        """Declared filter predicates."""
        return self._predicates

    def with_schema(self, schema: SchemaContract) -> FromClickHouse:
        """Return a new source with a Loom schema contract attached."""
        return self._clone(_schema=resolve_schema(schema))

    def where(self, *predicates: Any) -> FromClickHouse:
        """Return a new source with the given predicates appended."""
        return self._clone(_predicates=self._predicates + predicates)

    def columns(self, *cols: str) -> FromClickHouse:
        """Return a new source projecting only the listed columns."""
        if not cols:
            raise ValueError("FromClickHouse.columns() requires at least one column name.")
        return self._clone(_columns=cols)

    def distinct(self) -> FromClickHouse:
        """Return a new source marked as DISTINCT."""
        return self._clone(_distinct=True)

    def unbounded(self) -> FromClickHouse:
        """Opt into a full-table scan explicitly."""
        return self._clone(_allow_full_scan=True)

    def _to_spec(self, alias: str) -> ClickHouseSourceSpec:
        """Compile the declarative builder into a frozen source spec."""
        if not self._predicates and not self._allow_full_scan:
            raise ValueError(
                f"FromClickHouse({self._ref.ref!r}) has no predicates. "
                "Use .where() to filter rows or .unbounded() for an explicit full scan."
            )
        return ClickHouseSourceSpec(
            alias=alias,
            table_ref=self._ref,
            predicates=self._predicates,
            columns=self._columns,
            schema=self._schema,
            distinct=self._distinct,
            allow_full_scan=self._allow_full_scan,
        )

    def _clone(self, **overrides: Any) -> FromClickHouse:
        new = object.__new__(FromClickHouse)
        for slot in self.__slots__:
            object.__setattr__(new, slot, overrides.get(slot, getattr(self, slot)))
        return new

    def __repr__(self) -> str:
        return f"FromClickHouse({self._ref.ref!r})"


__all__ = ["FromClickHouse"]
