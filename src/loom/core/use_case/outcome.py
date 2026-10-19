"""Results a use case returns to say what its operation did, free of any transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Created(Generic[T]):
    """The use case created ``value``.

    Any other result answers the route's own status. A use case that may
    create declares ``T | Created[T]``; a caller that wants the value reads
    ``value``.

    Args:
        value: The created resource.

    Example::

        async def execute(self, command: CreateItem = Input()) -> ItemView | Created[ItemView]:
            view, changed = await self._items.ensure(command)
            return Created(view) if changed else view
    """

    value: T
