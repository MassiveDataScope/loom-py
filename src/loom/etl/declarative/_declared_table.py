"""The one place a declaration naming a table, by reference or by model, is normalised."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from loom.core.model import BaseModel
from loom.core.routing import LogicalRef
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.schema import ModelTable

TableDeclaration: TypeAlias = str | LogicalRef | TableRef | type[BaseModel]
"""What a builder accepts to name a table: a reference, or a model that names its own."""


@dataclass(frozen=True, slots=True)
class DeclaredTable:
    """A table as a step declares it: its reference and, for a model, its lake table.

    Args:
        ref: Logical reference the table is routed by.
        model_table: The lake table of the model the declaration named, ``None`` for a reference.
    """

    ref: TableRef
    model_table: ModelTable | None = None

    @classmethod
    def from_declaration(cls, declaration: TableDeclaration) -> DeclaredTable:
        """Normalise *declaration*; a model resolves to its ``__tablename__`` and lake table.

        Raises:
            InvalidType: A model declares no ``__tablename__``.
            InvalidValue: A model column has a type the lake refuses.
        """
        if isinstance(declaration, type):
            table = ModelTable.from_model(declaration)
            return cls(TableRef(table.name), table)
        if isinstance(declaration, TableRef):
            return cls(declaration)
        return cls(TableRef(declaration))

    def merge_keys(self, declared: tuple[str, ...] | None) -> tuple[str, ...]:
        """The keys to merge on: those declared, else the model's when the frame can supply them.

        A reference has no default, so it gives ``()`` and the compiler asks for ``keys``.
        """
        if declared:
            return declared
        return self.model_table.frame_keys() if self.model_table is not None else ()
