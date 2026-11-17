"""Job dispatch and retry policy value objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class Countdown:
    """A delay in whole seconds, from 0 to 900: the longest any job transport can wait.

    Raises:
        ValueError: When the seconds fall outside the bound.
    """

    MAX_SECONDS: ClassVar[int] = 900

    seconds: int

    def __post_init__(self) -> None:
        if not 0 <= self.seconds <= self.MAX_SECONDS:
            raise ValueError(f"a countdown is 0 to {self.MAX_SECONDS} seconds, not {self.seconds}")
