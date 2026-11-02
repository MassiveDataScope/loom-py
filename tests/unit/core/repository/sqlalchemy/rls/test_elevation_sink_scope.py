"""The real elevation sink, driven by elevation frames, through the invocation scope checkpoint.

A phased unit of work of the default store holds a recording session, so
every flag the sink sets or clears goes through ``participant_for`` and its
owner-task check exactly as it does against Postgres, without one.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

import pytest

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevate_scope, elevation_scope
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository.sqlalchemy.rls import SQLAlchemyElevationSink
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import OrderedCommits, PhysicalStore
from loom.core.uow.scope import Frame, invocation

WRITE = Permission("rows.write")
CATALOG = RoleCatalog((WRITE,), (Role("editor", (WRITE,)),))
BOUNDARY = Scope.of("b1")
ON = {"loom.scope.editor.any": "on"}
CLEARED = {"loom.scope.editor.any": ""}


@dataclass
class _RecordingSession:
    """Stands in for the participant's ``AsyncSession``: records the settings it is sent."""

    settings: list[Mapping[str, str]] = field(default_factory=list)
    invalidated: int = 0
    clearing: asyncio.Event = field(default_factory=asyncio.Event)
    """Set once a clearing statement has started."""
    release: asyncio.Event | None = None
    """When given, a clearing statement waits for it before it is recorded."""
    fail_clearing: bool = False
    """A clearing statement raises once released, as a dropped connection would."""

    def in_transaction(self) -> bool:
        return True

    async def execute(self, statement: object, parameters: Mapping[str, str]) -> None:
        del statement
        values: dict[str, str] = json.loads(parameters["settings"])
        if "" in values.values():
            self.clearing.set()
            if self.release is not None:
                await self.release.wait()
            if self.fail_clearing:
                raise ConnectionError("connection lost")
        self.settings.append(values)

    async def invalidate(self) -> None:
        self.invalidated += 1


class _PhasedUnitOfWork:
    """A phased unit of work of ``owner`` whose session is ``session``."""

    transactional: ClassVar[bool] = True
    phased: ClassVar[bool] = True

    def __init__(self, owner: SessionManager, session: _RecordingSession) -> None:
        self.owner = owner
        self.session = session
        self.physical = PhysicalStore(backend="sqlalchemy", target="recording")

    async def begin(self) -> None:
        return None

    async def flush(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> _PhasedUnitOfWork:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


@dataclass(frozen=True)
class _Given:
    unit_of_work: UnitOfWork

    def create(self) -> UnitOfWork:
        return self.unit_of_work


@pytest.fixture
async def manager() -> AsyncIterator[SessionManager]:
    built = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
    )
    try:
        yield built
    finally:
        await built.dispose()


def _decision() -> Decision:
    return Decision(allowed=True, grant=Grant(subject="ana", role="editor", scope=BOUNDARY))


async def _nested_elevation(
    manager: SessionManager, session: _RecordingSession, *, hold: bool = False
) -> None:
    """An outer execution owning the transaction, and a nested one that elevates ``editor``.

    With ``hold`` the nested execution waits, to be cancelled, before it returns.
    """
    sink = SQLAlchemyElevationSink(manager)
    unit_of_work: Any = _PhasedUnitOfWork(manager, session)
    coordinator = OrderedCommits({DEFAULT_STORE: _Given(unit_of_work)})
    async with (
        elevation_scope(owns_transaction=True, sink=sink),
        invocation(coordinator) as scope,
        scope.frame(Frame(DEFAULT_STORE)),
        elevation_scope(owns_transaction=False, sink=sink),
    ):
        await elevate_scope("editor", _decision(), at=BOUNDARY, permission=WRITE, catalog=CATALOG)
        if hold:
            await asyncio.sleep(10)


class TestClosingANestedFrame:
    async def test_it_clears_the_flag_on_the_owner_s_session(self, manager: SessionManager) -> None:
        session = _RecordingSession()

        await _nested_elevation(manager, session)

        assert session.settings == [ON, CLEARED]

    async def test_it_never_invalidates_the_owner_s_session(self, manager: SessionManager) -> None:
        session = _RecordingSession()

        await _nested_elevation(manager, session)

        assert session.invalidated == 0


async def _cancelled_twice_while_clearing(
    manager: SessionManager, session: _RecordingSession
) -> tuple[bool, list[Mapping[str, str]]]:
    """Cancel the execution, then again while its flag is being cleared; release the clearing.

    Returns:
        Whether the execution was still unwinding before the release, and the
        settings recorded by then.
    """
    task = asyncio.create_task(_nested_elevation(manager, session, hold=True))
    await asyncio.sleep(0.01)
    task.cancel()
    async with asyncio.timeout(1):
        await session.clearing.wait()
    task.cancel()
    for _ in range(5):
        await asyncio.sleep(0)
    unwinding = not task.done()
    seen = list(session.settings)
    assert session.release is not None
    session.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    return unwinding, seen


class TestASecondCancellationWhileClearing:
    async def test_the_execution_waits_for_the_clearing(self, manager: SessionManager) -> None:
        session = _RecordingSession(release=asyncio.Event())

        unwinding, seen = await _cancelled_twice_while_clearing(manager, session)

        assert (unwinding, seen) == (True, [ON])

    async def test_the_flag_is_cleared_once_it_ends(self, manager: SessionManager) -> None:
        session = _RecordingSession(release=asyncio.Event())

        await _cancelled_twice_while_clearing(manager, session)

        assert session.settings == [ON, CLEARED]


class TestAClearingThatFailsWhileCancelled:
    async def test_the_session_is_invalidated(self, manager: SessionManager) -> None:
        session = _RecordingSession(release=asyncio.Event(), fail_clearing=True)

        await _cancelled_twice_while_clearing(manager, session)

        assert session.invalidated == 1
