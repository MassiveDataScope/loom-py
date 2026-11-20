"""The port that sends a validated job dispatch."""

from __future__ import annotations

from typing import Any, Protocol

from loom.core.job.callback import JobCallback
from loom.core.job.dispatch import JobDispatch
from loom.core.job.handle import JobHandle


class JobTransport(Protocol):
    """Send a validated dispatch on the immediate or post-commit channel."""

    def send_after_commit(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Schedule the send for after the current unit of work commits."""
        ...

    async def send(self, dispatch: JobDispatch) -> JobHandle[Any]:
        """Send now, even when the caller's unit of work remains open."""
        ...

    def check_callbacks(
        self, on_success: type[JobCallback] | None, on_failure: type[JobCallback] | None
    ) -> None:
        """Refuse callbacks that the transport cannot carry before discovery checks."""
        ...
