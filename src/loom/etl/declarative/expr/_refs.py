"""Table and column references for the ETL declarative DSL."""

from __future__ import annotations

from loom.core.routing import LogicalRef, as_logical_ref
from loom.etl.declarative.expr._predicate import _ColOps


class TableRef:
    """Logical table identifier used by the ETL declarative DSL.

    Args:
        ref: Dotted logical table reference (for example ``"raw.orders"``).
    """

    __slots__ = ("_ref",)

    def __init__(self, ref: str | LogicalRef) -> None:
        self._ref = as_logical_ref(ref)

    @property
    def ref(self) -> str:
        """Raw dotted table reference."""
        return self._ref.ref

    def qualify(self, default_catalog: str) -> TableRef:
        """Return a catalog-qualified reference when the ref is 2-part.

        If the ref already contains 3 parts (catalog.schema.table) or
        *default_catalog* is empty, the original reference is returned unchanged.

        Args:
            default_catalog: Catalog name to prepend when the ref is 2-part.

        Returns:
            New ``TableRef`` with catalog prefix, or self if already qualified.
        """
        parts = self.ref.split(".")
        if len(parts) == 2 and default_catalog:
            return TableRef(f"{default_catalog}.{self.ref}")
        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableRef):
            return NotImplemented
        return self.ref == other.ref

    def __hash__(self) -> int:
        return hash(self.ref)

    def __repr__(self) -> str:
        return f"TableRef({self.ref!r})"


class UnboundColumnRef(_ColOps):
    """Column reference not yet bound to a specific table."""

    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        """Column name."""
        return self._name

    def __hash__(self) -> int:
        return hash(self._name)

    def __repr__(self) -> str:
        return f"col({self._name!r})"


def col(name: str) -> UnboundColumnRef:
    """Return an unbound column reference for DSL predicates."""
    return UnboundColumnRef(name)


__all__ = ["TableRef", "UnboundColumnRef", "col"]
