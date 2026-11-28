"""Errors a Celery worker process raises about itself, not about a job."""

from __future__ import annotations

from loom.core.errors import SystemError
from loom.core.errors.summary import ErrorSummary


class WorkerNotOpenError(SystemError):
    """A task reached a worker process whose application failed to open.

    Celery swallows a failure in ``worker_process_init`` and keeps the child
    accepting tasks, and killing the child would only make the pool respawn it
    into the same failure. Instead every task of that process fails fast with
    this error, chained (``from``) to the failure to open. A system error: a
    retry may land on a healthy process.

    The message cites the failure as loom does any cause: it travels to the
    result backend and the caller. The failure itself stays on ``__cause__``,
    which is not pickled, and in the worker's log.

    Args:
        cause: The failure to open the application in this process.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(
            f"this worker process could not open its application "
            f"({ErrorSummary.message_for(cause)}); "
            "its tasks fail until the process is replaced"
        )


__all__ = ["WorkerNotOpenError"]
