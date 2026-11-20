"""The use cases the scoped-elevation tests compose."""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.authz import Decision, Grant, Scope
from loom.core.authz.elevation import elevated_scopes
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.use_case import UseCase
from loom.stores.sqlalchemy.rls import elevate
from loom.stores.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import FLAG, U1

DECISION = Decision(
    allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.from_segments(U1))
)
_INSERT = f"INSERT INTO notes (owner_id, editor, body) VALUES ('{U1}', 'ana', 'outer')"
FLAGS: list[str] = []
"""The flag each elevating execution saw, kept even when its invocation fails."""


def _session(session_manager: SessionManager) -> AsyncSession:
    session = session_manager.active_session
    assert session is not None
    return session


class EditBobElevated(UseCase[notes.Note, None]):
    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> None:
        await elevate("editor", DECISION, at=Scope.from_segments(U1))
        session = _session(self._session_manager)
        FLAGS.append((await session.execute(text(FLAG))).scalar_one())
        await session.execute(text("UPDATE notes SET body = 'edited' WHERE editor = 'bob'"))


class WriteAndNest(UseCase[notes.Note, None]):
    """Write, run :class:`EditBobElevated` nested, maybe catching its error, write again."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self, catch: bool, timeout_s: float | None) -> None:
        await _session(self._session_manager).execute(text(_INSERT))
        try:
            async with asyncio.timeout(timeout_s):
                await self._app.invoke(EditBobElevated)
        except Exception:
            if not catch:
                raise
        await _session(self._session_manager).execute(text(_INSERT))


class ElevateFromGatherChild(UseCase[notes.Note, tuple[object, frozenset[str], str]]):
    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> tuple[object, frozenset[str], str]:
        elevation = elevate("editor", DECISION, at=Scope.from_segments(U1))
        (refused,) = await asyncio.gather(elevation, return_exceptions=True)
        flag = (await _session(self._session_manager).execute(text(FLAG))).scalar_one()
        return refused, elevated_scopes(), flag
