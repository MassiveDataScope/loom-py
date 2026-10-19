from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Created(Generic[T]):
    """The use case created ``value``.

    A use case that may create declares ``T | Created[T]``; a caller that
    wants the value reads ``value``.

    Args:
        value: The created resource.
        location: URI reference of the created resource, or ``None``.

    Raises:
        ValueError: If ``location`` is empty or holds a character outside
            printable ASCII, or a space.

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
            location and location.isascii() and location.isprintable() and " " not in location
        ):
            raise ValueError(
                "Created.location must be a non-empty URI reference of printable ASCII "
                "without spaces"
            )
