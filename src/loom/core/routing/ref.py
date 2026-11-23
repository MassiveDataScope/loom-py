"""Logical references shared by infrastructure-facing DSLs."""

from __future__ import annotations

from loom.core.errors import InvalidValue
from loom.core.model import LoomFrozenStruct


class LogicalRef(LoomFrozenStruct, frozen=True):
    """Stable logical reference resolved by module-specific routing config.

    Args:
        value: Logical reference value used in user DSLs.
    """

    value: str

    def __post_init__(self) -> None:
        """Validate reference value."""
        if not self.value.strip():
            raise InvalidValue("LogicalRef.value must be a non-empty string.")

    @property
    def ref(self) -> str:
        """Raw logical reference string."""
        return self.value

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"LogicalRef({self.value!r})"


def as_logical_ref(value: str | LogicalRef) -> LogicalRef:
    """Normalize a string or logical reference.

    Args:
        value: Logical reference string or existing :class:`LogicalRef`.

    Returns:
        Normalized logical reference.
    """

    if isinstance(value, LogicalRef):
        return value
    return LogicalRef(value)


__all__ = ["LogicalRef", "as_logical_ref"]
