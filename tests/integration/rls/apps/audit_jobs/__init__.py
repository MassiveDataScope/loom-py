"""The use case and job the audit-actor test composes."""

from __future__ import annotations

from sqlalchemy import text

from loom.core.job.job import Job
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.use_case import UseCase
from loom.stores.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import bookkeeping


class AddMemo(UseCase[bookkeeping.Memo, None]):
    """Write one audited memo as the caller."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> None:
        session = self._session_manager.active_session
        if session is None:
            raise RuntimeError("no unit-of-work session")
        await session.execute(text("INSERT INTO memos (body) VALUES ('from a job')"))


class AddMemoJob(Job[None]):
    """Run :class:`AddMemo` as the caller the task carries."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self) -> None:
        await self._app.invoke(AddMemo)
