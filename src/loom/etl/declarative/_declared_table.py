"""The one place a declaration naming a table, by reference or by model, is normalised."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TypeAlias

from loom.core.model import BaseModel
from loom.core.model.introspection import ModelKeys, get_column_fields, get_table_name
from loom.core.routing import LogicalRef
from loom.etl.declarative._history_date_type import HistoryDateType
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.schema import ModelTable

TableDeclaration: TypeAlias = str | LogicalRef | TableRef | type[BaseModel]
"""What a builder accepts to name a table: a reference, or a model that names its own."""


@dataclass(frozen=True, slots=True)
class DeclaredTable:
    """A table as a step declares it: its reference and, for a model, the model.

    The model is translated to lake types by the compiler, which refuses what the
    lake cannot store; a declaration only names it.

    Args:
        ref: Logical reference the table is routed by.
        model: The model the declaration named, ``None`` for a reference.
    """

    ref: TableRef
    model: type[BaseModel] | None = None

    @classmethod
    def from_declaration(cls, declaration: TableDeclaration) -> DeclaredTable:
        """Normalise *declaration*; a model resolves to its ``__tablename__``.

        Raises:
            InvalidType: A model declares no ``__tablename__``.
        """
        if isinstance(declaration, type):
            return cls(TableRef(get_table_name(declaration)), declaration)
        if isinstance(declaration, TableRef):
            return cls(declaration)
        return cls(TableRef(declaration))

    def merge_keys(self, declared: tuple[str, ...] | None) -> tuple[str, ...]:
        """The keys to merge on: those declared, else the model's when the frame can supply them.

        A reference has no default, so it gives ``()`` and the compiler asks for ``keys``.
        """
        if declared:
            return declared
        return ModelKeys.of(self.model).frame_supplied if self.model is not None else ()

    def boundary_type(self, column: str) -> HistoryDateType | None:
        """The history date type of model *column*, ``None`` when it is not a temporal column."""
        if self.model is None:
            return None
        info = get_column_fields(self.model).get(column)
        return HistoryDateType.of_kind(None if info is None else info.kind)


@dataclass(frozen=True)
class ModelBoundSpec:
    """What every table spec shares: the model its target was declared with, if any.

    Args:
        model: The model the target named, ``None`` for a plain reference. The compiler
            translates it to lake types and refuses what the lake cannot store.
    """

    model: type[BaseModel] | None = field(default=None, kw_only=True)

    @property
    def model_table(self) -> ModelTable | None:
        """The lake table of the model: translated once per class, at compile time."""
        return None if self.model is None else ModelTable.from_model(self.model)

    @property
    def declared_columns(self) -> Mapping[str, tuple[str, ...]]:
        """The model columns this spec names, by the argument that names them."""
        return {}
