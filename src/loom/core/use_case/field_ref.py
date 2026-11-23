from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from loom.core.command.base import Command
from loom.core.errors import InvalidValue
from loom.core.model.base import BaseModel

FieldRoot = type[Command] | type[BaseModel] | str


class PredicateOp(StrEnum):
    OR = "or"
    AND = "and"


@dataclass(frozen=True, slots=True)
class FieldRef:
    """Declarative reference to a field path on a command (or loaded alias)."""

    root: FieldRoot
    path: tuple[str, ...]

    def __getattr__(self, item: str) -> FieldRef:
        if item.startswith("_"):
            raise AttributeError(item)
        return FieldRef(root=self.root, path=(*self.path, item))

    def __or__(self, other: FieldRef | FieldExpr) -> FieldExpr:
        return FieldExpr(PredicateOp.OR, self, other)

    def __and__(self, other: FieldRef | FieldExpr) -> FieldExpr:
        return FieldExpr(PredicateOp.AND, self, other)

    @property
    def leaf(self) -> str:
        leaf = next(reversed(self.path), None)
        if leaf is None:
            raise InvalidValue("FieldRef.path cannot be empty.")
        return leaf


@dataclass(frozen=True, slots=True)
class FieldExpr:
    """Boolean expression over field references used by DSL predicates."""

    op: PredicateOp
    left: FieldRef | FieldExpr
    right: FieldRef | FieldExpr

    def __or__(self, other: FieldRef | FieldExpr) -> FieldExpr:
        return FieldExpr(PredicateOp.OR, self, other)

    def __and__(self, other: FieldRef | FieldExpr) -> FieldExpr:
        return FieldExpr(PredicateOp.AND, self, other)


class _FieldRefFactory:
    __slots__ = ("_root",)

    def __init__(self, root: FieldRoot) -> None:
        self._root = root

    def __getattr__(self, item: str) -> FieldRef:
        if item.startswith("_"):
            raise AttributeError(item)
        return FieldRef(root=self._root, path=(item,))


def field_ref(
    root: FieldRoot,
) -> Any:
    """Build a typed field-reference factory.

    Example:
        ``F(UpdateUserCommand).birthdate``
    """
    return _FieldRefFactory(root)


F = field_ref
