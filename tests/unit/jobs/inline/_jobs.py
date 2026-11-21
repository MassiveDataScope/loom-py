"""The jobs the inline enqueue tests send."""

from __future__ import annotations

from loom.core.job.job import Job
from loom.core.job.service import JobService
from tests.helpers.jobs import JobActivity


class RecordedJob(Job[str]):
    """Records each run in the activity."""

    def __init__(self, activity: JobActivity) -> None:
        self._activity = activity

    async def execute(self) -> str:
        self._activity.record("ran")
        return "done"


class ChainedJob(Job[None]):
    """Dispatches the next link of a chain of three, recording when each link enters and exits."""

    def __init__(self, activity: JobActivity, jobs: JobService) -> None:
        self._activity = activity
        self._jobs = jobs

    async def execute(self, link: int) -> None:
        self._activity.record(f"enter {link}")
        if link < 2:
            await self._jobs.dispatch_now(ChainedJob, params={"link": link + 1})
        self._activity.record(f"exit {link}")


class AcmeError(Exception):
    pass
