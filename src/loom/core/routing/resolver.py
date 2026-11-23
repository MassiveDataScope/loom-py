"""Common route resolver helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Generic, Protocol, TypeVar

from loom.core.errors import UnknownKey
from loom.core.routing.ref import LogicalRef, as_logical_ref

TargetT = TypeVar("TargetT")
TargetT_co = TypeVar("TargetT_co", covariant=True)


class RouteResolver(Protocol[TargetT_co]):
    """Resolve one logical reference into a module-specific target."""

    def resolve(self, ref: str | LogicalRef) -> TargetT_co:
        """Resolve a logical reference."""
        ...


class DefaultingRouteResolver(Generic[TargetT]):
    """Resolve named overrides before falling back to a default target.

    Args:
        default: Optional default target.
        overrides: Specific targets keyed by logical reference string.
        kind: Human-readable target kind used in error messages.
    """

    def __init__(
        self,
        *,
        default: TargetT | None,
        overrides: Mapping[str, TargetT],
        kind: str,
    ) -> None:
        self._default = default
        self._overrides = dict(overrides)
        self._kind = kind

    def resolve(self, ref: str | LogicalRef) -> TargetT:
        """Resolve *ref* from overrides or default target.

        Args:
            ref: Logical reference string or object.

        Returns:
            Specific target when present, otherwise the default target.

        Raises:
            UnknownKey: If no specific or default target is configured.
        """

        logical_ref = as_logical_ref(ref)
        if logical_ref.ref in self._overrides:
            return self._overrides[logical_ref.ref]
        if self._default is not None:
            return self._default
        raise UnknownKey(f"{self._kind} route not found for the logical reference.")


__all__ = ["DefaultingRouteResolver", "RouteResolver"]
