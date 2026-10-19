"""Results a use case returns to say what its operation did, free of any transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Created(Generic[T]):
    """The use case created ``value``; a plain result means it was already there.

    A use case that may create declares ``T | Created[T]``. The REST adapter
    answers ``Created`` with ``201`` and ``value`` as the body; every other
    caller receives the wrapper as it is and reads ``value``.

    Args:
        value: The created resource.

    Example::

        async def execute(self, command: CreateGrant = Input()) -> GrantView | Created[GrantView]:
            view, changed = await self._grants.grant(command)
            return Created(view) if changed else view
    """

    value: T
