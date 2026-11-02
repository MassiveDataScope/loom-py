"""Errors a Celery worker process raises about itself, not about a job."""

from __future__ import annotations

import re

from loom.core.errors.errors import SystemError
from loom.core.model.redaction import mask_dsn

_ADDRESS = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://\S+")
"""A URL inside free text; each one is masked on its own."""


class WorkerNotOpenError(SystemError):
    """A task reached a worker process whose application failed to open.

    Celery swallows a failure in ``worker_process_init`` and keeps the child
    accepting tasks, and killing the child would only make the pool respawn it
    into the same failure. Instead every task of that process fails fast with
    this error, chained (``from``) to the failure to open. A system error: a
    retry may land on a healthy process.

    The message names the type of the failure and its text with every address
    masked: it travels to the result backend and the caller. The failure
    itself stays on ``__cause__``, which is not pickled, and in the worker's
    log.

    Args:
        cause: The failure to open the application in this process.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(
            f"this worker process could not open its application "
            f"({type(cause).__qualname__}: {_masked(str(cause))}); "
            "its tasks fail until the process is replaced"
        )


def _masked(text: str) -> str:
    """Return *text* with the password and secret parameters of each URL masked."""
    return _ADDRESS.sub(lambda match: mask_dsn(match.group()), text)


__all__ = ["WorkerNotOpenError"]
