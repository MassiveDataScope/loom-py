"""The use case and job the worker-parity test composes."""

from __future__ import annotations

from sqlalchemy import text

from loom.core.job.job import Job
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.use_case import UseCase
from loom.stores.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import notes


class ListNoteBodies(UseCase[notes.Note, list[str]]):
    """Read every note the caller's scope lets through."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> list[str]:
        session = self._session_manager.active_session
        if session is None:
            raise RuntimeError("no unit-of-work session")
        rows = await session.execute(text("SELECT body FROM notes ORDER BY body"))
        return [str(body) for body in rows.scalars()]


class ListNoteBodiesJob(Job[list[str]]):
    """Run :class:`ListNoteBodies` as the caller the task carries."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self) -> list[str]:
        return list(await self._app.invoke(ListNoteBodies))
