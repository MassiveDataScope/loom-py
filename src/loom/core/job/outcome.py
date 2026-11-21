"""What a job says about its own run, and what a run came to."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.core.engine.events import AbandonReason
from loom.core.job.errors import JobAbandonment, JobPostponement
from loom.core.job.policy import Countdown


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
    """The job will not run again: it gave up, its error cannot succeed, or no run can follow.

    Attributes:
        reason: Why the message will not run again.
        error: The error or signal that ended the job; its class and message say
            what happened.
    """

    reason: AbandonReason
    error: Exception | None = None

    def as_exception(self) -> Exception:
        """The error that ended the job, or a :class:`JobAbandonment` naming the reason."""
        return self.error or JobAbandonment(reason=self.reason)

    @property
    def statement(self) -> str:
        """The sentence a dead-letter message carries: the job's words, or its error's class."""
        error = self.as_exception()
        match error:
            case JobAbandonment():
                return error.reason
            case JobPostponement():
                return f"cannot wait: {error.reason}"
            case _:
                return type(error).__qualname__


@dataclass(frozen=True, slots=True)
class Failed:
    """The attempt failed and another follows after *delay*."""

    error: Exception
    delay: Countdown

    def as_exception(self) -> Exception:
        """The error of the failed attempt."""
        return self.error


JobOutcome = Succeeded | Postponed | Abandoned | Failed
