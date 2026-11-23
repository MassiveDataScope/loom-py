"""Path reference builders for expression DSLs."""

from __future__ import annotations

from typing import Any

from loom.core.errors import InvalidValue
from loom.core.expr.nodes import (
    AndExpr,
    BetweenExpr,
    EqExpr,
    GeExpr,
    GtExpr,
    InExpr,
    LeExpr,
    LtExpr,
    NeExpr,
    NotExpr,
    OrExpr,
    PathSegment,
)
from loom.core.model import LoomFrozenStruct


class PathRef(LoomFrozenStruct, frozen=True):
    """Reference to a path under a named expression root.

    Args:
        root: Root identifier interpreted by module-specific evaluators.
        path: Path segments under the root.
    """

    root: str
    path: tuple[PathSegment, ...] = ()

    def __getattr__(self, name: str) -> PathRef:
        if name.startswith("_"):
            raise AttributeError(name)
        return PathRef(root=self.root, path=(*self.path, name))

    def __getitem__(self, key: str | int) -> PathRef:
        return PathRef(root=self.root, path=(*self.path, key))

    def __eq__(self, other: object) -> EqExpr:  # type: ignore[override]
        return EqExpr(left=self, right=other)

    def __ne__(self, other: object) -> NeExpr:  # type: ignore[override]
        return NeExpr(left=self, right=other)

    def __gt__(self, other: object) -> GtExpr:
        return GtExpr(left=self, right=other)

    def __ge__(self, other: object) -> GeExpr:
        return GeExpr(left=self, right=other)

    def __lt__(self, other: object) -> LtExpr:
        return LtExpr(left=self, right=other)

    def __le__(self, other: object) -> LeExpr:
        return LeExpr(left=self, right=other)

    def isin(self, values: Any) -> InExpr:
        """Return membership expression."""
        return InExpr(ref=self, values=values)

    def between(self, low: Any, high: Any) -> BetweenExpr:
        """Return inclusive range expression."""
        return BetweenExpr(ref=self, low=low, high=high)

    def __and__(self, other: Any) -> AndExpr:
        return AndExpr(left=self, right=other)

    def __or__(self, other: Any) -> OrExpr:
        return OrExpr(left=self, right=other)

    def __invert__(self) -> NotExpr:
        return NotExpr(operand=self)

    def __hash__(self) -> int:
        return hash((self.root, self.path))


class RootRef:
    """Factory for root-scoped :class:`PathRef` objects."""

    __slots__ = ("_root",)

    def __init__(self, root: str) -> None:
        if not root.strip():
            raise InvalidValue("RootRef.root must be a non-empty string.")
        self._root = root

    @property
    def root(self) -> str:
        """Root identifier."""
        return self._root

    def __getattr__(self, name: str) -> PathRef:
        if name.startswith("_"):
            raise AttributeError(name)
        return PathRef(root=self._root, path=(name,))

    def __getitem__(self, key: str | int) -> PathRef:
        return PathRef(root=self._root, path=(key,))


__all__ = ["PathRef", "RootRef"]
