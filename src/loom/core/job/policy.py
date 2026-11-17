"""Job dispatch and retry policy value objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Countdown:
    """Delay in seconds before a job runs, bounded to 0..900 inclusive.

    This is the only owner of that bound: dispatch countdown, backoff steps
    and JobRetry delays all construct a ``Countdown``.

    Args:
        seconds: Delay in seconds.

    Raises:
        ValueError: When *seconds* is outside 0..900 inclusive.
    """

    seconds: int

    def __post_init__(self) -> None:
        if self.seconds < 0 or self.seconds > 900:
            raise ValueError(
                f"Countdown must be between 0 and 900 seconds inclusive, got {self.seconds}"
            )
