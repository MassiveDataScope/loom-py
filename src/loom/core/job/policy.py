"""Retry vocabulary of jobs: the bounded delay, the backoff and the attempt limit."""

from __future__ import annotations

import random
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

    first_s: int
    factor: float
    max_s: int
    jitter: bool = True

    def __post_init__(self) -> None:
        if not 0 <= self.max_s <= Countdown.MAX_SECONDS:
            raise ValueError(f"max_s must be 0 to {Countdown.MAX_SECONDS}, not {self.max_s}")
        if self.first_s < 0:
            raise ValueError(f"first_s cannot be negative, not {self.first_s}")
        if self.factor < 1:
            raise ValueError(f"factor must be at least 1, not {self.factor}")

    def delay(self, attempt: int) -> Countdown:
        """The wait after failed attempt number *attempt*, counting from 1.

        Args:
            attempt: The attempt that failed, or the deferral being scheduled.

        Returns:
            The ceiling for that attempt, or a draw from zero to it with jitter.
        """
        try:
            ceiling = self.first_s * self.factor ** max(attempt - 1, 0)
        except OverflowError:
            ceiling = self.max_s
        capped = int(min(self.max_s, ceiling))
        return Countdown(random.randint(0, capped) if self.jitter else capped)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How a job is repeated: the delay between attempts and how many it gets.

    Attributes:
        backoff: The delay after a failure or a deferral.
        max_attempts: Attempts before a failure is final, at least 1; ``None``
            leaves the limit to the transport.

    Raises:
        ValueError: When *max_attempts* is below 1.
    """

    backoff: Backoff
    max_attempts: int | None

    def __post_init__(self) -> None:
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError(f"max_attempts must be at least 1, not {self.max_attempts}")

    def allows_attempt_after(self, attempt: int) -> bool:
        """Whether a failed attempt number *attempt* may be followed by another."""
        return self.max_attempts is None or attempt < self.max_attempts


DEFAULT_RETRY = RetryPolicy(Backoff(first_s=5, factor=2, max_s=300), max_attempts=3)
"""The policy of a job that no layer configures."""
