"""Marker and descriptors for tables whose rows are scoped by a column value."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Literal

from loom.core.model.privilege import READ_WRITE, Privilege

Reach = Literal["read", "write", "both"]


class RowScoped:
    """Mark a model as row-scoped; adds no column.

    The product names the scope on one of the model's columns with
    ``ColumnField(scope=...)``. A read-only scoped table narrows
    ``__scope_privileges__`` to ``{Privilege.SELECT}``.
    """

    __row_scoped__: ClassVar[bool] = True
    __scope_privileges__: ClassVar[frozenset[Privilege]] = READ_WRITE


@dataclass(frozen=True, slots=True)
class ScopeColumn:
    """One scoped column: which scope, which column, and how far it reaches."""

    scope: str
    column: str
    on: Reach
    elevable: bool

    @property
    def is_boundary(self) -> bool:
        return self.on == "both" and not self.elevable


@dataclass(frozen=True, slots=True)
class ScopedTable:
    """A compiled scoped table: its scopes and the privileges it grants."""

    schema: str | None
    name: str
    scopes: tuple[ScopeColumn, ...]
    privileges: frozenset[Privilege]

    @property
    def boundary(self) -> ScopeColumn:
        return next(scope for scope in self.scopes if scope.is_boundary)
