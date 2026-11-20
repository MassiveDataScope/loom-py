"""What a job says about its own run, and what a run came to."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.core.errors import PostCommitError
from loom.core.job.policy import Countdown


class JobSignal(Exception):
    """Raised by a job to steer its own delivery; not a failure."""


class JobPostponement(JobSignal):
    """Raised by a job to run again later, without counting as a failed attempt.

    Attributes:
        reason: Why the job waits.
        delay: The wait; ``None`` leaves it to the job's backoff.

    Raises:
        ValueError: When *delay_s*, the seconds to wait from 0 to 900, is outside
            the countdown bound; ``None`` takes the backoff step.
    """

    def __init__(self, reason: str, delay_s: int | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.delay = None if delay_s is None else Countdown(delay_s)


class JobAbandonment(JobSignal):
    """Raised by a job to give up for good: the delivery is settled as abandoned.

    Attributes:
        reason: Why the job gives up.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class Succeeded:
    """The job ran to completion."""

    result: Any


@dataclass(frozen=True, slots=True)
class Postponed:
    """The job asked to run again after *delay*; no attempt was spent."""

    delay: Countdown
    reason: str


@dataclass(frozen=True, slots=True)
class Abandoned:
    """The job will not run again: it gave up, its error cannot succeed, or attempts ran out.

    Attributes:
        reason: The job's reason, or the class of its error.
        error: The error or signal that ended the job.
    """

    reason: str
    error: Exception | None = None

    def as_exception(self) -> Exception:
        """The error that ended the job, or a :class:`JobAbandonment` with its reason."""
        return self.error or JobAbandonment(self.reason)


@dataclass(frozen=True, slots=True)
class Failed:
    """The attempt failed and another follows after *delay*."""

    error: Exception
    delay: Countdown

    def as_exception(self) -> Exception:
        """The error of the failed attempt."""
        return self.error


JobOutcome = Succeeded | Postponed | Abandoned | Failed


def failure_log_fields(exc: BaseException) -> dict[str, object]:
    """Return transport-neutral details for a failed job."""
    if isinstance(exc, PostCommitError):
        return {"committed": exc.committed, "failures": len(exc.failures)}
    return {}
