"""Job, post-commit and SQS dispatch errors."""

from __future__ import annotations

from loom.core.errors.base import LoomError, SystemError
from loom.core.errors.codes import ErrorCode


class MalformedJobMessage(SystemError):
    """A job message violates the envelope contract.

    Retryable on purpose: the queue's DLQ keeps the message as evidence; configure a DLQ.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.MALFORMED_JOB_MESSAGE)


class UnknownJob(SystemError):
    """A message names a job absent from this application's registry.

    Retryable on purpose: the queue's DLQ keeps the message as evidence; configure a DLQ.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Unknown job {name!r}", code=ErrorCode.UNKNOWN_JOB)


class JobTimeoutError(LoomError):
    """Raised when waiting for a job result exceeds the requested timeout.

    Args:
        job_id: UUID of the timed-out job.
        timeout: The timeout value that was exceeded, in seconds.

    Example::

        raise JobTimeoutError("abc-123", timeout=30.0)
    """

    def __init__(self, job_id: str, timeout: float | None = None) -> None:
        msg = f"Job {job_id!r} timed out"
        if timeout is not None:
            msg += f" after {timeout}s"
        super().__init__(msg, code="job_timeout")
        self.job_id = job_id
        self.timeout = timeout


class JobFailedError(LoomError):
    """Raised when a job fails during execution.

    Wraps the underlying cause so callers can inspect it without catching
    arbitrary exceptions directly.

    Args:
        job_id: UUID of the failed job.
        cause: The exception that caused the failure, if available.

    Example::

        raise JobFailedError("abc-123", cause=exc)
    """

    def __init__(self, job_id: str, cause: BaseException | None = None) -> None:
        msg = f"Job {job_id!r} failed"
        if cause is not None:
            msg += f": {cause}"
        super().__init__(msg, code="job_failed")
        self.job_id = job_id
        self.cause = cause


class MixedJobBackendsError(LoomError):
    """``dispatch_parallel`` with callbacks spans jobs of more than one backend."""

    retryable = False

    def __init__(self) -> None:
        super().__init__(
            "dispatch_parallel callbacks need every job on one job backend; "
            "these jobs route to several",
            code="job_backends_mixed",
        )


class PostCommitError(LoomError):
    """Raised when one or more post-commit actions failed after a commit.

    Every action runs even when an earlier one fails; the error carries all
    failures in priority, publish, then plain order.

    Args:
        committed: Whether the transaction had committed when the actions ran.
        failures: Exceptions in priority, publish, then plain order.
    """

    def __init__(self, *, committed: bool, failures: tuple[Exception, ...]) -> None:
        self.committed = committed
        # A retry once the transaction committed would apply it twice.
        self.retryable = not committed
        self.failures = failures
        super().__init__(
            f"{len(failures)} post-commit action(s) failed (committed={committed})",
            code=ErrorCode.POST_COMMIT_FAILURE,
        )
