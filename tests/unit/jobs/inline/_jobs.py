"""The jobs the inline enqueue tests send."""

from __future__ import annotations

from loom.core.job.job import Job
from loom.core.job.service import JobService
from tests.helpers.jobs import Trail


class RecordedJob(Job[str]):
    """Records each run in the trail."""

    def __init__(self, trail: Trail) -> None:
        self._trail = trail

    async def execute(self) -> str:
        self._trail.record("ran")
        return "done"


class ChainedJob(Job[None]):
    """Dispatches the next link of a chain of three, recording when each link enters and exits."""

    def __init__(self, trail: Trail, jobs: JobService) -> None:
        self._trail = trail
        self._jobs = jobs

    async def execute(self, link: int) -> None:
        self._trail.record(f"enter {link}")
        if link < 2:
            await self._jobs.dispatch_now(ChainedJob, params={"link": link + 1})
        self._trail.record(f"exit {link}")


class AcmeError(Exception):
    pass
