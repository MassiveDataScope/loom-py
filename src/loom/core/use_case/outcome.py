from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

_URI_REFERENCE = re.compile(r"(?:[\w\-.~:/?#\[\]@!$&'()*+,;=]|%[\dA-Fa-f]{2})+", re.ASCII)


@dataclass(frozen=True, slots=True)
class Created(Generic[T]):
    """The use case created ``value``.

    A use case that may create declares ``T | Created[T]``; a caller that
    wants the value reads ``value``.

    Args:
        value: The created resource.
        location: URI of the created resource, absolute or relative, or
            ``None``.

    Raises:
        ValueError: If ``location`` is empty or holds a character not allowed
            in an RFC 3986 URI reference (unreserved, reserved and
            percent-encoded characters only).
        TypeError: If ``location`` is not a ``str``.

    Example::

        async def execute(self, command: CreateItem = Input()) -> ItemView | Created[ItemView]:
            view, changed = await self._items.ensure(command)
            return Created(view, location=f"/items/{view.id}") if changed else view
    """

    value: T
    location: str | None = None

    def __post_init__(self) -> None:
        location = self.location
        if location is not None and not _URI_REFERENCE.fullmatch(location):
            raise ValueError(
                "Created.location must be non-empty and hold only characters allowed "
                "in an RFC 3986 URI reference"
            )
