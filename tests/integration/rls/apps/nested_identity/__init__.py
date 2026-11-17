"""The use cases and jobs the nested-identity tests compose."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from sqlalchemy import CursorResult, text

from loom.core.authz import Decision, Grant, Scope
from loom.core.errors import NestedIdentityMismatch
from loom.core.identity import Identity
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.use_case.invoker import AppInvoker, ApplicationInvoker
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from loom.stores.sqlalchemy.rls import elevate
from loom.stores.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import FLAG, U1, U2

ANA = Identity(subject=U1)
ZOE = Identity(subject=U2)
DECISION = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))

_OWNER = "SELECT coalesce(current_setting('loom.scope.owner', true), '')"
_BODIES = "SELECT body FROM notes ORDER BY body"
_TOUCH_BOB = "UPDATE notes SET body = body WHERE editor = 'bob'"


@dataclass(frozen=True)
class Seen:
    """What one execution saw on its transaction."""

    caller: str | None
    owner_setting: str
    editor_any: str
    bodies: tuple[str, ...]
    bob_rows_writable: int


async def _observe(session_manager: SessionManager, caller: Identity | None) -> Seen:
    session = session_manager.active_session
    if session is None:
        raise RuntimeError("no unit-of-work session")
    touched = cast(CursorResult[Any], await session.execute(text(_TOUCH_BOB))).rowcount
    return Seen(
        caller=None if caller is None else caller.subject,
        owner_setting=(await session.execute(text(_OWNER))).scalar_one(),
        editor_any=(await session.execute(text(FLAG))).scalar_one(),
        bodies=tuple((await session.execute(text(_BODIES))).scalars()),
        bob_rows_writable=touched,
    )


class ProbeRls(UseCase[notes.Note, Seen]):
    """Report the RLS settings and rows visible to this execution; declares no caller."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> Seen:
        return await _observe(self._session_manager, None)


class ProbeRlsAsCaller(UseCase[notes.Note, Seen]):
    """Report what ``Caller()`` received next to what RLS applied."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self, caller: Identity = Caller()) -> Seen:
        return await _observe(self._session_manager, caller)


class ElevateThenNest(UseCase[notes.Note, Seen]):
    """Elevate ``editor``, then invoke :class:`ProbeRls` as a nested execution."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def execute(self) -> Seen:
        await elevate("editor", DECISION, at=Scope.of(U1))
        return cast(Seen, await self._app.invoke(ProbeRls))


class NestAsZoe(UseCase[notes.Note, tuple[NestedIdentityMismatch | None, str]]):
    """Invoke :class:`ProbeRlsAsCaller` as ``ZOE``; report the refusal and the owner afterwards."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = cast(AppInvoker, app)

    async def execute(self) -> tuple[NestedIdentityMismatch | None, str]:
        as_zoe = self._app.for_identity(ZOE)
        refused: NestedIdentityMismatch | None = None
        try:
            await as_zoe.invoke(ProbeRlsAsCaller)
        except NestedIdentityMismatch as exc:
            refused = exc
        return refused, (await _observe(self._session_manager, None)).owner_setting


class NestAsSelf(UseCase[notes.Note, Seen]):
    """Invoke :class:`ProbeRlsAsCaller` as the caller this execution received."""

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = cast(AppInvoker, app)

    async def execute(self, caller: Identity = Caller()) -> Seen:
        return cast(Seen, await self._app.for_identity(caller).invoke(ProbeRlsAsCaller))


class ReadThenWriteAsZoe(UseCase[notes.Note, Seen]):
    """A read-only parent running a writing child as ``ZOE``."""

    read_only = True

    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = cast(AppInvoker, app)

    async def execute(self) -> Seen:
        return cast(Seen, await self._app.for_identity(ZOE).invoke(ProbeRlsAsCaller))


class ElevatingChild(UseCase[notes.Note, Seen]):
    """Elevate ``editor`` inside the nested execution itself."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> Seen:
        await elevate("editor", DECISION, at=Scope.of(U1))
        return await _observe(self._session_manager, None)


class NestElevatingChild(UseCase[notes.Note, tuple[Seen, str]]):
    """Invoke :class:`ElevatingChild`, then report the flag on the parent's transaction."""

    def __init__(self, session_manager: SessionManager, app: ApplicationInvoker) -> None:
        self._session_manager = session_manager
        self._app = app

    async def execute(self) -> tuple[Seen, str]:
        child = cast(Seen, await self._app.invoke(ElevatingChild))
        return child, (await _observe(self._session_manager, None)).editor_any


class WhoAmIJob(Job[str]):
    """Return the subject the job's ``Caller()`` received."""

    async def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


class ProbeJob(Job[Seen]):
    """Report the RLS settings the job's own unit of work runs with."""

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    async def execute(self) -> Seen:
        return await _observe(self._session_manager, None)


class ElevateThenDispatch(UseCase[notes.Note, Any]):
    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self) -> Any:
        await elevate("editor", DECISION, at=Scope.of(U1))
        return self._jobs.dispatch_after_commit(ProbeJob)


class RunOrDispatchJob(UseCase[notes.Note, Any]):
    def __init__(self, jobs: JobService) -> None:
        self._jobs = jobs

    async def execute(self, dispatch: bool) -> Any:
        if dispatch:
            return self._jobs.dispatch_after_commit(WhoAmIJob)
        return await self._jobs.run(WhoAmIJob)
