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
        location: RFC 3986 URI reference of the created resource, absolute
            or relative, or ``None``.

    Raises:
        ValueError: If ``location`` is not a ``str`` or not a non-empty URI
            reference: only unreserved, reserved and percent-encoded
            characters.

    Example::

        async def execute(self, command: CreateItem = Input()) -> ItemView | Created[ItemView]:
            view, changed = await self._items.ensure(command)
            return Created(view, location=f"/items/{view.id}") if changed else view
    """

    value: T
    location: str | None = None

    def __post_init__(self) -> None:
        location = self.location
        if location is not None and not (
            isinstance(location, str) and _URI_REFERENCE.fullmatch(location)
        ):
            raise ValueError("Created.location must be a non-empty RFC 3986 URI reference")
