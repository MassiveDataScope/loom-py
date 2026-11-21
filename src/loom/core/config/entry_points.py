"""The ``app.entry_points`` item: one entry-point group and whether it may be empty."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

_GROUP_KEY: Final = "group"
_OPTIONAL_KEY: Final = "optional"
_KEYS: Final = frozenset({_GROUP_KEY, _OPTIONAL_KEY})


class EntryPointGroup:
    """An entry-point group the application scans, required unless declared optional.

    Declared as a plain string (a required group) or as a mapping
    ``{group: <name>, optional: true}``. The scanner, the
    ``loom.discovery.W001`` check and ``loom check`` all read this object, so the
    shape is decoded once.

    Args:
        name: The entry-point group.
        is_optional: Whether the group may legitimately have no installed entry point.
    """

    __slots__ = ("is_optional", "name")

    def __init__(self, name: str, *, is_optional: bool = False) -> None:
        self.name = name
        self.is_optional = is_optional

    @classmethod
    def from_declared(cls, declared: object) -> EntryPointGroup:
        """Decode one item of ``app.entry_points``.

        Args:
            declared: A group name, or a mapping with ``group`` and optionally ``optional``.

        Raises:
            ValueError: When the item has another shape, a key other than ``group`` and
                ``optional``, no group name, or an ``optional`` that is not a boolean.
        """
        if isinstance(declared, str):
            return cls(declared)
        if not isinstance(declared, Mapping):
            raise ValueError("expected a group name or {group: <name>, optional: <bool>}")
        unknown = sorted(str(key) for key in declared if key not in _KEYS)
        if unknown:
            raise ValueError(
                f"unknown key {unknown[0]!r} in an entry point group: "
                f"use {_GROUP_KEY!r} and {_OPTIONAL_KEY!r}"
            )
        name = declared.get(_GROUP_KEY)
        if not isinstance(name, str):
            raise ValueError(f"an entry point group needs a {_GROUP_KEY!r} name")
        is_optional = declared.get(_OPTIONAL_KEY, False)
        if not isinstance(is_optional, bool):
            raise ValueError(f"{_OPTIONAL_KEY!r} must be true or false")
        return cls(name, is_optional=is_optional)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EntryPointGroup):
            return NotImplemented
        return (self.name, self.is_optional) == (other.name, other.is_optional)

    def __hash__(self) -> int:
        return hash((self.name, self.is_optional))

    def __repr__(self) -> str:
        return f"EntryPointGroup({self.name!r}, is_optional={self.is_optional})"
