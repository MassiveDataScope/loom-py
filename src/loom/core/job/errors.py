"""What a job raises to steer its own delivery: postpone it, or give it up."""

from __future__ import annotations

from types import MappingProxyType
from typing import ClassVar

from loom.core.engine.events import Status
from loom.core.errors.base import ABSENT, DomainError
from loom.core.errors.codes import ErrorCode
from loom.core.job.log_events import JobLogField
from loom.core.job.policy import Countdown
from loom.core.vocabulary import LogField


class JobSignal(DomainError):
    """Raised by a job to steer its own delivery; not a failure, so its code is expected."""

    status: ClassVar[Status]

    @property
    def reason(self) -> str:
        """Why the job steers its delivery, as rendered when it was built."""
        value = self.log_fields().get(LogField.REASON)
        return ABSENT if value is None else str(value)


class JobPostponement(JobSignal):
    """Raised by a job to run again later, without counting as a failed attempt.

    Built as ``JobPostponement(reason=..., delay_s=...)``.

    Args:
        reason: Why the job waits.
        delay_s: Seconds to wait, from 0 to 900; ``None`` takes the job's backoff step.

    Raises:
        ValueError: When *delay_s* is outside the countdown bound.

    Attributes:
        delay: The countdown it asks for, or ``None`` for the job's backoff step.
    """

    code = ErrorCode.JOB_POSTPONED
    status: ClassVar[Status] = Status.POSTPONED
    template = "postponed for {delay_s} seconds: {reason}"
    attributes = MappingProxyType(
        {"reason": LogField.REASON, "delay_s": JobLogField.POSTPONE_DELAY_S}
    )

    def __init__(
        self, *rebuilt: object, reason: str | None = None, delay_s: int | None = None
    ) -> None:
        self.delay = None if delay_s is None else Countdown(delay_s)
        super().__init__(*rebuilt, reason=reason, delay_s=delay_s)


class JobAbandonment(JobSignal):
    """Raised by a job to give up for good: the delivery is settled as abandoned.

    Built as ``JobAbandonment(reason=...)``.
    """

    code = ErrorCode.JOB_ABANDONED
    status: ClassVar[Status] = Status.ABANDONED
    template = "abandoned: {reason}"
    attributes = MappingProxyType({"reason": LogField.REASON})
