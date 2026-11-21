"""Retry vocabulary of jobs: the bounded delay, the backoff and the attempt limit."""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, ClassVar

import msgspec

MIN_SECONDS = 0
"""The shortest delay, and the lowest start of a backoff."""
MAX_COUNTDOWN_SECONDS = 900
"""The longest delay any job transport can wait."""
MIN_BACKOFF_FACTOR = 1
"""The lowest growth of a backoff: a delay never shrinks."""
MIN_ATTEMPTS = 1
"""The fewest attempts a policy allows, and the number of the first attempt."""

Seconds = Annotated[int, msgspec.Meta(ge=MIN_SECONDS, le=MAX_COUNTDOWN_SECONDS)]
"""Whole seconds within the bound of a countdown."""


class Redelivery(StrEnum):
    """What the transport can do with a job that must run again."""

    DELAYED = "delayed"
    SAME = "same"
    NONE = "none"

    @property
    def can_postpone(self) -> bool:
        """Whether the transport brings the job back after the delay the job asks for."""
        return self is Redelivery.DELAYED

    @property
    def can_retry(self) -> bool:
        """Whether the transport brings a failed job back for another attempt."""
        return self is not Redelivery.NONE


@dataclass(frozen=True, slots=True)
class Countdown:
    """A delay in whole seconds, from 0 to 900: the longest any job transport can wait.

    Raises:
        ValueError: When the seconds fall outside the bound.
    """

    MAX_SECONDS: ClassVar[int] = MAX_COUNTDOWN_SECONDS

    seconds: Seconds

    def __post_init__(self) -> None:
        if not MIN_SECONDS <= self.seconds <= self.MAX_SECONDS:
            raise ValueError(f"a countdown is 0 to {self.MAX_SECONDS} seconds, not {self.seconds}")


@dataclass(frozen=True, slots=True)
class Backoff:
    """Exponential delay between attempts, capped, with full jitter by default.

    Attributes:
        first_s: Ceiling of the delay after the first failure.
        factor: Growth of the ceiling for each further attempt.
        max_s: Ceiling of every delay; at most 900.
        jitter: Whether a delay is drawn uniformly from zero to its ceiling.

    Raises:
        ValueError: When *max_s* is outside 0 to 900, *first_s* is negative
            or *factor* is below 1.
    """

    first_s: Annotated[int, msgspec.Meta(ge=MIN_SECONDS)]
    factor: Annotated[float, msgspec.Meta(ge=MIN_BACKOFF_FACTOR)]
    max_s: Seconds
    jitter: bool = True

    def __post_init__(self) -> None:
        if not MIN_SECONDS <= self.max_s <= Countdown.MAX_SECONDS:
            raise ValueError(f"max_s must be 0 to {Countdown.MAX_SECONDS}, not {self.max_s}")
        if self.first_s < MIN_SECONDS:
            raise ValueError(f"first_s cannot be negative, not {self.first_s}")
        if self.factor < MIN_BACKOFF_FACTOR:
            raise ValueError(f"factor must be at least 1, not {self.factor}")

    def delay(self, attempt: int, rng: random.Random) -> Countdown:
        """The wait after failed attempt number *attempt*, counting from 1.

        Args:
            attempt: The attempt that failed, or the postponement being scheduled.
            rng: The source of the jitter draw; unused without jitter.

        Returns:
            The ceiling for that attempt, or a draw from zero to it with jitter.
        """
        try:
            ceiling = self.first_s * self.factor ** max(attempt - 1, 0)
        except OverflowError:
            ceiling = self.max_s
        capped = int(min(self.max_s, ceiling))
        return Countdown(rng.randint(0, capped) if self.jitter else capped)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How a job is repeated: the delay between attempts and how many it gets.

    Attributes:
        backoff: The delay after a failure or a postponement.
        max_attempts: Attempts before a failure is final, at least 1; ``None``
            leaves the limit to the transport.

    Raises:
        ValueError: When *max_attempts* is below 1.
    """

    backoff: Backoff
    max_attempts: Annotated[int, msgspec.Meta(ge=MIN_ATTEMPTS)] | None

    def __post_init__(self) -> None:
        if self.max_attempts is not None and self.max_attempts < MIN_ATTEMPTS:
            raise ValueError(f"max_attempts must be at least 1, not {self.max_attempts}")

    def can_retry_after(self, attempt: int) -> bool:
        """Whether a failed attempt number *attempt* may be followed by another."""
        return self.max_attempts is None or attempt < self.max_attempts


DEFAULT_RETRY = RetryPolicy(Backoff(first_s=5, factor=2, max_s=300), max_attempts=3)
"""The policy of a job that no layer configures."""
